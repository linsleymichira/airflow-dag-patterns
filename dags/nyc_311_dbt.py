"""Asset-triggered dbt transform DAG.

Demonstrates pattern #3 from the README: data-aware downstream scheduling. This DAG
has no cron; it fires the moment `nyc_311_pipeline` emits Asset("raw_311"). It then
runs `dbt build --select state:modified+` over the embedded dbt project and emits
Asset("mart_complaints_daily") on success.
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
MART_ASSET = Asset("mart_complaints_daily")


@dag(
    dag_id="nyc_311_dbt",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    schedule=[RAW_311_ASSET],
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
