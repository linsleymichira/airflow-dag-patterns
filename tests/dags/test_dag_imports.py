"""Parse-time DAG import test.

Loops over every DAG file in dags/ and asserts the file imports cleanly under Airflow's
DagBag loader. Also checks the factory's count contract — a silent factory failure (e.g.,
a malformed sources.yaml entry that gets skipped) would otherwise pass `astro dev parse`
without anyone noticing.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from airflow.models import DagBag

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_DIR = REPO_ROOT / "dags"
SOURCES_YAML = REPO_ROOT / "include" / "sources.yaml"


@pytest.fixture(scope="session")
def dag_bag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def _expected_factory_dag_ids() -> set[str]:
    with open(SOURCES_YAML, "r") as f:
        config = yaml.safe_load(f) or {}
    return {f"{src['name']}_pipeline" for src in config.get("sources", [])}


def test_no_import_errors(dag_bag: DagBag) -> None:
    if dag_bag.import_errors:
        formatted = "\n".join(f"{path}: {err}" for path, err in dag_bag.import_errors.items())
        pytest.fail(f"DAG import errors:\n{formatted}")


def test_hand_written_dags_loaded(dag_bag: DagBag) -> None:
    expected = {"nyc_311_pipeline", "nyc_311_dbt"}
    missing = expected - set(dag_bag.dag_ids)
    assert not missing, f"Missing hand-written DAGs: {missing}. Loaded: {set(dag_bag.dag_ids)}"


def test_factory_registers_all_sources(dag_bag: DagBag) -> None:
    expected = _expected_factory_dag_ids()
    missing = expected - set(dag_bag.dag_ids)
    assert not missing, (
        f"Factory failed to register sources from include/sources.yaml: missing {missing}. "
        f"Loaded: {set(dag_bag.dag_ids)}"
    )


def test_total_dag_count(dag_bag: DagBag) -> None:
    expected_total = 2 + len(_expected_factory_dag_ids())  # hand-written pipeline + dbt + factory peers
    assert len(dag_bag.dags) == expected_total, (
        f"Expected {expected_total} DAGs, got {len(dag_bag.dags)}: {set(dag_bag.dag_ids)}"
    )
