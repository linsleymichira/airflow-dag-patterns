"""Parse-time DAG import test.

Loops over every DAG file in dags/ and asserts the file imports cleanly under Airflow's
DagBag loader. Catches typos, missing deps, and bad import paths before they reach a
worker. Fast enough for every PR; this is the cheaper gate before `astro dev parse`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="session")
def dag_bag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dag_bag: DagBag) -> None:
    if dag_bag.import_errors:
        formatted = "\n".join(f"{path}: {err}" for path, err in dag_bag.import_errors.items())
        pytest.fail(f"DAG import errors:\n{formatted}")


def test_expected_dags_loaded(dag_bag: DagBag) -> None:
    expected = {"nyc_311_pipeline", "nyc_311_dbt"}
    missing = expected - set(dag_bag.dag_ids)
    assert not missing, f"Missing DAGs: {missing}. Loaded: {set(dag_bag.dag_ids)}"
