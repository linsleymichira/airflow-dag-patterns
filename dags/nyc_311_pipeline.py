"""NYC 311 ingest pipeline: freshness sensor → extract → idempotent load → Asset emit.

The hand-written reference DAG. Demonstrates five of the six repo patterns:

  1. Idempotent task design (bounded by data_interval_start/_end, INSERT OR REPLACE on unique_key)
  2. Freshness-aware scheduling (PythonSensor in reschedule mode)
  3. Data-aware downstream via Asset outlet
  4. Structured on_failure_callback alerting
  5. TaskFlow API for the whole graph

The sixth pattern (config-driven DAG factory) lives in dags/factories/source_factory.py;
this DAG stays hand-written so a reviewer has one readable reference to anchor the patterns.

Source: NYC 311 Service Requests via the Socrata Open Data API
        https://data.cityofnewyork.us/resource/erm2-nwe9.json
Target: DuckDB file at include/nyc_311.duckdb (raw_311.complaints)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import duckdb
import pendulum
import requests
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import Asset, Variable, dag, task

from include.callbacks import alert_callback

logger = logging.getLogger(__name__)

SOCRATA_BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
DUCKDB_PATH = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")) / "include" / "nyc_311.duckdb"
RAW_311_ASSET = Asset("raw_311")
PAGE_SIZE = 1000
MAX_PAGES = 50  # hard cap to avoid runaway pulls; tune for production


def _socrata_headers() -> dict[str, str]:
    token = Variable.get("socrata_app_token", default="")
    return {"X-App-Token": token} if token else {}


def _check_freshness(data_interval_start: pendulum.DateTime, **_: Any) -> bool:
    """Poll Socrata's :updated_at and decide whether upstream is fresh enough to proceed.

    Implements the rule from README §"Design decisions to defend" #2: proceed only when the
    dataset has moved past this interval's start AND the interval actually has rows.

    Returns True to let the extract run, False to reschedule and poll again later.
    """
    response = requests.get(
        SOCRATA_BASE,
        params={"$select": "max(:updated_at) AS max_updated_at"},
        headers=_socrata_headers(),
        timeout=30,
    )
    response.raise_for_status()
    max_updated_at_raw = response.json()[0]["max_updated_at"]
    max_updated_at = pendulum.parse(max_updated_at_raw)

    count_response = requests.get(
        SOCRATA_BASE,
        params={
            "$select": "count(*) AS row_count",
            "$where": f":updated_at >= '{data_interval_start.to_iso8601_string()}'",
        },
        headers=_socrata_headers(),
        timeout=30,
    )
    count_response.raise_for_status()
    row_count = int(count_response.json()[0]["row_count"])

    logger.info(
        "freshness poll: max_updated_at=%s data_interval_start=%s row_count=%d",
        max_updated_at,
        data_interval_start,
        row_count,
    )

    # Both conditions, because each alone fails a real case. max_updated_at alone fires on any
    # dataset touch, waking the dbt DAG for an extract that lands nothing. row_count alone would
    # pass a stale interval whose rows were all counted on an earlier poll. Together they mean
    # "upstream moved, and it moved into this interval".
    #
    # The threshold is a Variable rather than a literal 0 because NYC 311's weekend cadence makes
    # the right floor environment-specific: 1 is honest for a demo (any new row is real activity),
    # while a production deployment may raise it to damp near-empty runs. Hardcoding it is the
    # "too strict" failure in the docstring, and it misses legitimate slow days.
    min_new_rows = int(Variable.get("nyc_311_min_new_rows", default=1))
    return max_updated_at > data_interval_start and row_count >= min_new_rows


@dag(
    dag_id="nyc_311_pipeline",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": alert_callback,
    },
    tags=["nyc-311", "ingest", "patterns"],
    doc_md=__doc__,
)
def nyc_311_pipeline():
    freshness_sensor = PythonSensor(
        task_id="freshness_sensor",
        python_callable=_check_freshness,
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 6,
        on_failure_callback=alert_callback,
    )

    @task(on_failure_callback=alert_callback)
    def extract_311(data_interval_start=None, data_interval_end=None) -> list[dict[str, Any]]:
        """Pull rows where :updated_at falls in [data_interval_start, data_interval_end).

        Idempotent: same interval inputs → same row set on every rerun. Paginates internally
        to clear the 1000-row default cap.
        """
        assert data_interval_start is not None and data_interval_end is not None
        start_iso = data_interval_start.to_iso8601_string()
        end_iso = data_interval_end.to_iso8601_string()
        where_clause = f":updated_at >= '{start_iso}' AND :updated_at < '{end_iso}'"

        rows: list[dict[str, Any]] = []
        for page in range(MAX_PAGES):
            response = requests.get(
                SOCRATA_BASE,
                params={
                    "$where": where_clause,
                    "$limit": PAGE_SIZE,
                    "$offset": page * PAGE_SIZE,
                    "$order": ":updated_at",
                },
                headers=_socrata_headers(),
                timeout=60,
            )
            response.raise_for_status()
            batch = response.json()
            rows.extend(batch)
            if len(batch) < PAGE_SIZE:
                break
        else:
            logger.warning("hit MAX_PAGES=%d; consider raising or chunking", MAX_PAGES)

        logger.info("extracted %d rows for [%s, %s)", len(rows), start_iso, end_iso)
        return rows

    @task(outlets=[RAW_311_ASSET], on_failure_callback=alert_callback)
    def load_to_duckdb(rows: list[dict[str, Any]]) -> int:
        """Upsert rows into raw_311.complaints on unique_key.

        Partition-overwrite-safe: rerunning the same interval produces the same rows because
        the natural key (`unique_key`) is unique per 311 service request and `INSERT OR REPLACE`
        idempotently overwrites prior versions of the same record. The outlet emits
        Asset("raw_311") on success, triggering nyc_311_dbt downstream.
        """
        import json as _json

        if not rows:
            logger.info("no rows to load; emitting empty asset event")
            return 0

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        records = [
            (
                r.get("unique_key"),
                r.get("created_date"),
                r.get("closed_date"),
                r.get("complaint_type"),
                r.get("descriptor"),
                r.get("borough"),
                r.get("status"),
                r.get("due_date"),
                r.get("resolution_description"),
                r.get(":updated_at") or r.get("updated_at"),
                _json.dumps(r),
            )
            for r in rows
            if r.get("unique_key")
        ]

        with duckdb.connect(str(DUCKDB_PATH)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw_311")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_311.complaints (
                    unique_key VARCHAR PRIMARY KEY,
                    created_date TIMESTAMP,
                    closed_date TIMESTAMP,
                    complaint_type VARCHAR,
                    descriptor VARCHAR,
                    borough VARCHAR,
                    status VARCHAR,
                    due_date TIMESTAMP,
                    resolution_description VARCHAR,
                    updated_at TIMESTAMP,
                    raw JSON,
                    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO raw_311.complaints (
                    unique_key, created_date, closed_date, complaint_type, descriptor,
                    borough, status, due_date, resolution_description, updated_at, raw, _loaded_at
                ) VALUES (
                    ?, TRY_CAST(? AS TIMESTAMP), TRY_CAST(? AS TIMESTAMP), ?, ?, ?, ?,
                    TRY_CAST(? AS TIMESTAMP), ?, TRY_CAST(? AS TIMESTAMP), ?, CURRENT_TIMESTAMP
                )
                """,
                records,
            )

        logger.info("merged %d rows into raw_311.complaints", len(records))
        return len(records)

    extracted = extract_311()
    freshness_sensor >> extracted
    load_to_duckdb(extracted)


nyc_311_pipeline()
