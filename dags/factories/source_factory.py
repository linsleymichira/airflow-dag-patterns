"""Config-driven DAG factory for NYC Socrata sources.

Reads `include/sources.yaml`, loops over the entries, and registers one extract+load
DAG per source under a `<name>_pipeline` id. Every generated DAG inherits the same
five patterns as the hand-written `nyc_311_pipeline`:

  1. Idempotent bounded interval (data_interval_start/_end, no `now()`)
  2. Freshness sensor in reschedule mode
  3. INSERT OR REPLACE load on the configured primary key
  4. Asset outlet for data-aware downstream scheduling
  5. on_failure_callback for structured alerting

The hand-written DAG stays in dags/ as the readable reference. The factory is the
production scale-out story: adding a source = a YAML diff, not a Python diff.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import duckdb
import pendulum
import requests
import yaml
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import Asset, Variable, dag, task

from include.callbacks import alert_callback

logger = logging.getLogger(__name__)

SOCRATA_BASE = "https://data.cityofnewyork.us/resource"
AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
DUCKDB_PATH = AIRFLOW_HOME / "include" / "nyc_311.duckdb"
SOURCES_YAML = AIRFLOW_HOME / "include" / "sources.yaml"
PAGE_SIZE = 1000
MAX_PAGES = 50


def _socrata_headers() -> dict[str, str]:
    token = Variable.get("socrata_app_token", default_var="")
    return {"X-App-Token": token} if token else {}


def _socrata_url(socrata_id: str) -> str:
    return f"{SOCRATA_BASE}/{socrata_id}.json"


def _compose_where(freshness_field: str, start_iso: str, end_iso: str, extra: str | None) -> str:
    clause = f"{freshness_field} >= '{start_iso}' AND {freshness_field} < '{end_iso}'"
    return f"({clause}) AND ({extra})" if extra else clause


def load_sources() -> list[dict[str, Any]]:
    with open(SOURCES_YAML, "r") as f:
        config = yaml.safe_load(f) or {}
    return config.get("sources", [])


def build_source_dag(cfg: dict[str, Any]):
    name = cfg["name"]
    socrata_id = cfg["socrata_id"]
    primary_key = cfg["primary_key"]
    freshness_field = cfg["freshness_field"]
    asset_uri = cfg["asset_uri"]
    schedule = cfg.get("schedule", "@daily")
    where_filter = cfg.get("where_filter")
    table = f"raw_{asset_uri.removeprefix('raw_')}"

    asset = Asset(asset_uri)
    socrata_url = _socrata_url(socrata_id)

    def _check_freshness(data_interval_start: pendulum.DateTime, **_: Any) -> bool:
        # Factory peers ship with a safe default freshness rule so they're runnable
        # out of the box: proceed iff the source's max freshness timestamp is newer
        # than the interval start. The hand-written nyc_311_pipeline keeps the
        # TODO(human) hook because that's where the production-grade rule (which
        # adds a row-count threshold) is meant to be authored.
        response = requests.get(
            socrata_url,
            params={"$select": f"max({freshness_field}) AS max_freshness"},
            headers=_socrata_headers(),
            timeout=30,
        )
        response.raise_for_status()
        raw_value = response.json()[0]["max_freshness"]
        max_freshness = pendulum.parse(raw_value)
        logger.info(
            "[%s] freshness poll: max_%s=%s data_interval_start=%s",
            name,
            freshness_field,
            max_freshness,
            data_interval_start,
        )
        return max_freshness > data_interval_start

    @dag(
        dag_id=f"{name}_pipeline",
        start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "data-eng",
            "retries": 2,
            "retry_delay": pendulum.duration(minutes=5),
            "on_failure_callback": alert_callback,
        },
        tags=["factory", "nyc-open-data", asset_uri],
        doc_md=f"Factory-generated extract+load for {socrata_id}. Outlets Asset({asset_uri!r}).",
    )
    def _generated():
        freshness_sensor = PythonSensor(
            task_id="freshness_sensor",
            python_callable=_check_freshness,
            mode="reschedule",
            poke_interval=300,
            timeout=60 * 60 * 6,
            on_failure_callback=alert_callback,
        )

        @task(on_failure_callback=alert_callback)
        def extract(data_interval_start=None, data_interval_end=None) -> list[dict[str, Any]]:
            assert data_interval_start is not None and data_interval_end is not None
            where_clause = _compose_where(
                freshness_field,
                data_interval_start.to_iso8601_string(),
                data_interval_end.to_iso8601_string(),
                where_filter,
            )
            rows: list[dict[str, Any]] = []
            for page in range(MAX_PAGES):
                resp = requests.get(
                    socrata_url,
                    params={
                        "$where": where_clause,
                        "$limit": PAGE_SIZE,
                        "$offset": page * PAGE_SIZE,
                        "$order": freshness_field,
                    },
                    headers=_socrata_headers(),
                    timeout=60,
                )
                resp.raise_for_status()
                batch = resp.json()
                rows.extend(batch)
                if len(batch) < PAGE_SIZE:
                    break
            else:
                logger.warning("[%s] hit MAX_PAGES=%d; consider raising or chunking", name, MAX_PAGES)
            logger.info("[%s] extracted %d rows", name, len(rows))
            return rows

        @task(outlets=[asset], on_failure_callback=alert_callback)
        def load(rows: list[dict[str, Any]]) -> int:
            import json as _json

            if not rows:
                logger.info("[%s] no rows to load; emitting empty asset event", name)
                return 0
            DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
            records = [
                (
                    str(r.get(primary_key)),
                    r.get(freshness_field.lstrip(":")) or r.get(freshness_field),
                    _json.dumps(r),
                )
                for r in rows
                if r.get(primary_key) is not None
            ]
            if not records:
                # Surface the misconfig instead of silently emitting an empty Asset event.
                # Usually means primary_key in sources.yaml doesn't match a real column in the
                # Socrata response — check the source's API surface and update the YAML.
                raise ValueError(
                    f"[{name}] received {len(rows)} rows but none had a {primary_key!r} value; "
                    f"check that '{primary_key}' is a real column on Socrata dataset {socrata_id}"
                )
            with duckdb.connect(str(DUCKDB_PATH)) as conn:
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {table}")
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table}.records (
                        {primary_key} VARCHAR PRIMARY KEY,
                        freshness_at TIMESTAMP,
                        raw JSON,
                        _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                conn.executemany(
                    f"""
                    INSERT OR REPLACE INTO {table}.records ({primary_key}, freshness_at, raw, _loaded_at)
                    VALUES (?, TRY_CAST(? AS TIMESTAMP), ?, CURRENT_TIMESTAMP)
                    """,
                    records,
                )
            logger.info("[%s] merged %d rows into %s.records", name, len(records), table)
            return len(records)

        extracted = extract()
        freshness_sensor >> extracted
        load(extracted)

    return _generated()


def register_dags() -> None:
    """Walk include/sources.yaml and register one DAG per source under the module globals."""
    for cfg in load_sources():
        dag_id = f"{cfg['name']}_pipeline"
        globals()[dag_id] = build_source_dag(cfg)


register_dags()
