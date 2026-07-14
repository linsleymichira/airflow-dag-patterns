# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An Airflow 3 reference repo demonstrating six orchestration patterns end-to-end on a laptop: one hand-written extract+load DAG (`nyc_311_pipeline`), a config-driven DAG factory that generates peers from YAML, and one multi-Asset-triggered dbt transform DAG. Stack is Astro Runtime 3.2-4 + dbt-core + DuckDB, so the whole thing runs with no cloud accounts or credentials. Source data is the public NYC 311 Socrata API. `README.md` is the deep-dive (design-decision rationale, hiring-manager Q&A). CLAUDE.md is the operational map.

## Commands

Full run (needs Astro CLI + Docker):
```bash
astro dev start          # Airflow + Postgres in Docker, http://localhost:8080 (admin/admin)
astro dev parse          # static import check on every DAG file, no Docker services
astro dev restart        # after code changes
```

Tests (no Docker, bare Python):
```bash
pip install "apache-airflow==3.0.*" "apache-airflow-providers-standard" pytest
pip install -r requirements.txt
pytest tests/dags/ -v                                        # all DAG import + count tests
pytest tests/dags/test_dag_imports.py::test_total_dag_count  # a single test
```
CI (`.github/workflows/ci.yml`) runs exactly `pytest tests/dags/ -v` on Python 3.12 with `AIRFLOW_HOME=$GITHUB_WORKSPACE`. There is no separate lint step.

dbt (run from inside the container, or locally with deps installed):
```bash
cd dbt_project && dbt build --profiles-dir . --project-dir .
```

Inspect the DuckDB output:
```bash
docker exec -it $(docker ps -qf name=scheduler) duckdb /usr/local/airflow/include/nyc_311.duckdb
```

## Airflow 3 import conventions (do not regress to 2.x paths)

DAG code imports from the Airflow 3 Task SDK and the split-out `standard` provider. Match these exactly. Airflow 2 tutorial imports (`airflow.decorators`, `airflow.operators.bash`, `airflow.sensors.python`) are wrong here.

- `from airflow.sdk import Asset, Variable, dag, task`
- `from airflow.providers.standard.operators.bash import BashOperator`
- `from airflow.providers.standard.sensors.python import PythonSensor`

## Deliberate landmine: the freshness TODO is not a bug

`dags/nyc_311_pipeline.py::_check_freshness` intentionally ends in `raise NotImplementedError` behind a `TODO(human)` marker. It is a teaching hook (the repo's central design decision: how stale is too stale to run), not a defect. Do not "fix" it by inventing a return value unless the user explicitly asks. The factory peers in `dags/factories/source_factory.py` ship a working default rule (`max_freshness > data_interval_start`), so those DAGs run out of the box while the reference DAG's sensor stays open by design.

## Asset wiring is string-coupled across files

Data-aware scheduling is keyed on Asset URI strings, defined independently in multiple files. Renaming a URI in one place silently breaks the trigger.

- Producers: `load_to_duckdb` outlets `Asset("raw_311")`, and the factory outlets `Asset(asset_uri)` per source (`raw_taxi`, `raw_noise`).
- Consumer: `dags/nyc_311_dbt.py` subscribes with `schedule=(RAW_311_ASSET | RAW_TAXI_ASSET | RAW_NOISE_ASSET)`.
- Semantics matter: `|` is OR (fire when ANY source lands). A list `[a, b, c]` would be AND (wait for ALL). The dbt DAG wants OR, hence the `|` operator, not a list.

## The DAG factory and its test contract

`source_factory.py` calls `register_dags()` at module import, which loops `include/sources.yaml` and assigns each generated DAG into `globals()[f"{name}_pipeline"]` so the scheduler discovers it. Adding a source is a ~6-line YAML edit plus a dbt staging model, no new Python file.

`tests/dags/test_dag_imports.py` enforces this and will catch a silently-skipped factory entry:
- `test_total_dag_count` asserts total DAGs `== 2 + len(sources)`. The `2` is hardcoded (the two hand-written DAGs).
- `test_hand_written_dags_loaded` asserts a hardcoded `{"nyc_311_pipeline", "nyc_311_dbt"}`.

Consequence: adding a **source** needs no test change. Adding a **new hand-written DAG** breaks `test_total_dag_count` (the hardcoded `2 +`) until you bump it, and should also be added to the `test_hand_written_dags_loaded` set.

## Two load schemas by design

- Hand-written `nyc_311_pipeline` writes typed columns into `raw_311.complaints` (created_date, closed_date, borough, ...) plus a `raw` JSON column.
- Factory peers write a generic 3-column shape into `raw_<asset>.records`: `(<primary_key>, freshness_at, raw JSON)`. The typed projection is deferred to dbt staging, which keeps the factory source-agnostic.

Both use `INSERT OR REPLACE` on the natural key for idempotency, so a rerun of the same `data_interval` reproduces the same rows.

## dbt layer

One dbt DAG for all sources. `dbt build --select state:modified+ ... || dbt build` (the `|| dbt build` is a fallback because `state:modified+` needs a `--state` manifest that is not wired up here, so it falls through to a full build). Lineage lives in dbt, not Airflow, on purpose. `dbt_project/profiles.yml` targets DuckDB via `{{ env_var('AIRFLOW_HOME', ...) }}/include/nyc_311.duckdb` and is committed (no secrets, local file path). `mart_complaints_daily` is incremental with `delete+insert` on `[complaint_date, borough, complaint_type]`.

## AIRFLOW_HOME is the anchor path

Everything resolves relative to `AIRFLOW_HOME` (default `/usr/local/airflow` in-container): the DuckDB file, `include/sources.yaml`, and the dbt profile all read it. CI sets it to the workspace root. The DuckDB file (`include/nyc_311.duckdb`) is gitignored.

## Secrets

The Socrata app token flows `.env` -> `airflow_settings.yaml` (`${SOCRATA_APP_TOKEN}`) -> Airflow Variable `socrata_app_token`, read in code via `Variable.get("socrata_app_token", default_var="")`. Never hardcode it. The DAG runs without a token (lower rate limit).

## Test path shim

`conftest.py` prepends repo root and `include/` to `sys.path` so `from include.callbacks import ...` resolves under bare pytest. Astro Runtime does this automatically in-container. CI and local pytest do not, so the shim is required for the import tests to pass outside Docker.

<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
<!-- SPECKIT END -->
