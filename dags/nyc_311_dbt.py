"""Multi-Asset-triggered dbt transform DAG.

Demonstrates pattern #3 (data-aware downstream) and Design Decision #10 from the README:
one dbt DAG subscribed to every raw_* Asset emitted by the hand-written pipeline AND the
factory-generated peers. `dbt build --select state:modified+` then lets dbt's lineage
graph decide what rebuilds — Airflow handles "when," dbt handles "what."
"""

from __future__ import annotations

import os
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag

from include.callbacks import alert_callback

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))
DBT_PROJECT_DIR = AIRFLOW_HOME / "dbt_project"

RAW_311_ASSET = Asset("raw_311")
RAW_TAXI_ASSET = Asset("raw_taxi")
RAW_NOISE_ASSET = Asset("raw_noise")
MART_ASSET = Asset("mart_complaints_daily")


@dag(
    dag_id="nyc_311_dbt",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    # `[a, b, c]` is AND in Airflow 3 (DAG waits for *all* to update since last run).
    # We want OR semantics — fire whenever any source lands — so use the | operator.
    schedule=(RAW_311_ASSET | RAW_TAXI_ASSET | RAW_NOISE_ASSET),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": alert_callback,
    },
    tags=["nyc-311", "transform", "patterns"],
    doc_md=__doc__,
)
def nyc_311_dbt():
    BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt build --select state:modified+ "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            "|| dbt build "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
        outlets=[MART_ASSET],
        on_failure_callback=alert_callback,
    )


nyc_311_dbt()
