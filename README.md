## airflow-dag-patterns

A minimal, runnable Airflow 2.10 reference DAG demonstrating **five production orchestration patterns** on a live public API. Built with **Astro Runtime + dbt-core + DuckDB** so the whole pipeline runs end-to-end on a single `astro dev start`, no cloud accounts, no credentials, no waiting.

> **Why this repo exists:** Airflow tutorials show you how to write a DAG. Production Airflow shows you how to write one that survives an on-call rotation. This repo includes idempotent tasks, freshness-aware sensors, data-aware downstream triggering, structured alerting, and a real dbt handoff, all in one DAG you can run on a laptop in five minutes.

---

### What it demonstrates

1. **Idempotent task design** Every task is bound to `data_interval_start` / `data_interval_end`, so reruns and backfills produce the same output rows: partition-overwrite-safe loads, no duplicates, no surprises on a clear-and-rerun.
2. **Freshness-aware scheduling** A `PythonSensor` in `reschedule` mode polls the Socrata API's `:updated_at` field before any extract runs, so the DAG never burns a worker slot waiting on stale upstream data. Reschedule mode releases the slot between polls, the way you'd actually run this in production.
3. **Data-aware downstream (Airflow Datasets, 2.4+)**  the load task emits a `Dataset("raw_311")` event. The DBT DAG is scheduled on the dataset, not on a cron. The dbt run fires the moment new data lands, not three minutes later, because the cron happened to align.
4. **Structured `on_failure_callback` alerting**  every task wires a single callback that emits a JSON-shaped log line with run context (`dag_id`, `task_id`, `data_interval`, `try_number`, exception type, exception message). The exact shape an alerting backend (Slack, PagerDuty, OpsGenie) consumes without further parsing drops into a webhook, and you're paging.
5. **TaskFlow API** The DAG is written in the modern decorator style (`@dag`, `@task`), with explicit XCom passing via return values rather than `xcom_push` / `xcom_pull` string lookups. Type-friendly, lint-friendly, and the style every Airflow doc has used since 2.0.

---

### Architecture

```
NYC 311 Socrata API
  (JSON, ~daily refresh, :updated_at header)
        │
        ▼
  freshness_sensor               ← PythonSensor, reschedule mode
        │                          (polls:updated_at, releases slot between polls)
        ▼
  extract_311 (TaskFlow)         ← bounded by data_interval_start / _end
        │                          (idempotent: same interval → same rows)
        ▼
  load_to_duckdb                 ← MERGE on (unique_key, created_date)
        │                          (partition-overwrite-safe upsert)
        ▼
  emits Dataset("raw_311") ─────┐
                                │  (Airflow 2.4+ data-aware scheduling)
                                ▼
                          dbt_run DAG     ← schedule=[Dataset("raw_311")]
                                │           runs `dbt build --select state:modified+`
                                ▼
                          emits Dataset("mart_complaints_daily")
```

Full diagram with task names, retry/SLA settings, and Dataset URIs: [`docs/architecture.png`](docs/architecture.png).

Every task wires `on_failure_callback=alert_callback` from `include/callbacks.py`.

---

### Stack

- **Orchestrator:** Apache Airflow 2.10+ on [Astro Runtime](https://docs.astronomer.io/astro/runtime-release-notes) (free, open-source, recruiters recognize the `Dockerfile` + `astro dev` shape)
- **Local execution:** [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)  `astro dev start` spins up Airflow + Postgres metadata DB in Docker, no manual `airflow db init`
- **Source data:** [NYC 311 Service Requests](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) via the Socrata Open Data API. Free, ~38M+ rows, daily-fresh, optional app token (raises rate limit but not required)
- **Local warehouse target:** [DuckDB](https://duckdb.org/)  in-process, file-backed under `include/nyc_311.duckdb`. Same engine MotherDuck runs in production
- **Transformation:** [dbt-core](https://github.com/dbt-labs/dbt-core) 1.7+ via [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb) adapter  embedded in `dbt_project/`, runs end-to-end with no external warehouse
- **Tests:** dbt built-ins (`unique`, `not_null`, `relationships`) + DAG import test in `tests/dags/test_dag_imports.py`
- **CI:** GitHub Actions: `astro dev parse` (Astro's static-check) + `pytest tests/dags/` on every PR

---

### DAG layout

#### `dags/nyc_311_pipeline.py`  the extract+load DAG

| Task | Type | Purpose |
|---|---|---|
| `freshness_sensor` | `PythonSensor` (reschedule mode) | Polls `https://data.cityofnewyork.us/resource/erm2-nwe9.json?$select=max(:updated_at)` and succeeds when the max `:updated_at` is newer than `data_interval_start`. Releases the worker slot between polls. |
| `extract_311` | `@task` (TaskFlow) | Pulls records where `:updated_at` falls in `[data_interval_start, data_interval_end)` via `$where=` Socrata clause. Returns a list of dicts via XCom (small payload  paginates internally to avoid the 1000-row default cap). |
| `load_to_duckdb` | `@task` | `MERGE` into `raw_311.complaints` on `(unique_key, created_date)`. Partition-overwrite-safe: same interval reruns produce the same rows. Outlets `Dataset("raw_311")`. |

#### `dags/nyc_311_dbt.py`  the transform DAG

| Task | Type | Purpose |
|---|---|---|
| `dbt_build` | `BashOperator` | Runs `dbt build --select state:modified+ --profiles-dir /usr/local/airflow/dbt_project` from the embedded dbt project. Schedule: `[Dataset("raw_311")]`  no cron, fires on data event. Outlets `Dataset("mart_complaints_daily")`. |

#### `include/callbacks.py`  shared alert callback

A single `alert_callback(context)` function that every task wires via `on_failure_callback`. Emits a JSON log line with `dag_id`, `task_id`, `data_interval_start`, `data_interval_end`, `try_number`, `exception_type`, `exception_message`, `log_url`. Drop in a Slack/PagerDuty webhook by changing one line.

> The exact freshness threshold (how stale is "stale enough" to skip the run vs. proceed?) is the design decision in this repo most worth thinking about. See [`§ Design decisions to defend`](#design-decisions-to-defend) #2.

---

### dbt models

#### Staging (`dbt_project/models/staging/`)

| Model | Materialization | Purpose |
|---|---|---|
| `stg_311_complaints` | View | 1:1 mirror of `raw_311.complaints`. Light renaming to snake_case, type casting, drops Socrata's `:@computed_region_*` columns. No business logic. |

#### Marts (`dbt_project/models/marts/`)

| Model | Materialization | Grain | Purpose |
|---|---|---|---|
| `mart_complaints_daily` | Incremental (insert_overwrite on `complaint_date`) | One row per `complaint_date` × `borough` × `complaint_type` | Daily complaint counts with median resolution time, p95 resolution time, and pct-overdue (closed past `due_date`). The mart that a Power BI / Looker layer would actually read. |

---

### How to run it

#### Quickstart (recommended)

```bash
## 1. Install Astro CLI (one-line install, no Docker required for the install itself)
curl -sSL install.astronomer.io | sudo bash -s

## 2. Clone
git clone https://github.com/linsleymichira/airflow-dag-patterns.git
cd airflow-dag-patterns

## 3. Optional: get a Socrata app token to raise the rate limit (free, 30 seconds)
##    https://data.cityofnewyork.us/profile/edit/developer_settings
##    Then: cp .env.example .env  and paste SOCRATA_APP_TOKEN=...
##    Without a token, the API allows ~1000 req/hour  plenty for this DAG.

## 4. Start Airflow + Postgres in Docker
astro dev start

## 5. Open the Airflow UI
##    http://localhost:8080  (admin / admin)
##    Unpause `nyc_311_pipeline` and `nyc_311_dbt`.
##    The pipeline DAG runs on a daily schedule. Trigger manually for an immediate run.

## 6. Inspect the DuckDB output
docker exec -it $(docker ps -qf name=scheduler) duckdb /usr/local/airflow/include/nyc_311.duckdb
##    SELECT * FROM mart_complaints_daily ORDER BY complaint_date DESC LIMIT 10;
```

Total cold-start: under 5 minutes (Astro image pull dominates). Subsequent `astro dev restart` is under 30 seconds.

#### Static-check only (no Docker)

If you don't want to run Docker, you can still validate the DAGs parse cleanly:

```bash
pip install -r requirements.txt
astro dev parse              ## runs Airflow's import check on every DAG file
pytest tests/dags/           ## runs the import-time DAG tests
```

---

### Free-tier playbook (no spend, no employer overlap)

The whole repo is reproducible at zero cost on personal accounts:

| Component | Free path | Watch out for |
|---|---|---|
| Astro CLI + Astro Runtime | Free, open-source | Docker Desktop on Mac/Windows is free for personal use. Linux users run Docker Engine directly. |
| NYC 311 Socrata API | Free, no account required | Optional app token raises the rate limit from ~1000/hr to effectively unlimited for this workload. Get one in 30 seconds. |
| DuckDB | Free, in-process, no infrastructure | The `.duckdb` file lives in `include/` and is gitignored. |
| dbt-core + dbt-duckdb | Free, open-source | None. |
| GitHub | Public repo, free. Actions free tier covers CI on this repo's footprint. | None. |

---

### What's interesting (the stuff hiring managers ask about)

1. **Why reschedule-mode sensors over poke-mode?**  Poke-mode sensors hold a worker slot for the entire timeout, even if the slot is waiting on an external check. Reschedule mode releases the slot between checks, so a sensor that polls every 5 minutes for 6 hours occupies a worker for ~1 second instead of 6 hours straight. On a fixed pool, the difference between "DAGs queue forever" and "DAGs run on time."
2. **What does "idempotent" mean for an Airflow task, and how doThe subsequentntee it?**  The same task instance, given the same inputs (`data_interval_start`, `data_interval_end`), produces the same outputs no matter how many times it runs. This DAG guarantees it by (a) bounding the API query by `:updated_at` between the interval bounds, not "since now," and (b) using `MERGE` on the natural key + date, not `INSERT`.
3. **Why Datasets over `TriggerDagRunOperator` / `ExternalTaskSensor`?**  `TriggerDagRunOperator` is a one-way push: the upstream DAG decides when downstream runs, regardless of whether the downstream is healthy. `ExternalTaskSensor` is a polling pattern: the downstream waits for the upstream's task instance and breaks subtly when intervals don't align. `Dataset` is event-driven: upstream emits, downstream subscribes, scheduling is decoupled. Multiple downstream DAGs can subscribe to the same Dataset; older patterns enforce one-to-one wiring.
4. **How do you parameterize backfills without breaking idempotency?**  `airflow dags backfill` walks `data_interval_start` / `_end` across the requested range. Because every task in this DAG reads only those two parameters and writes via `MERGE`, a backfill produces the same output as the original runs. No "since when" timestamps anywhere in the code. `[Confirm tested via airflow dags backfill in setup checklist]`
5. **Why TaskFlow API instead of `PythonOperator` + `op_kwargs`?**  TaskFlow makes XCom passing implicit (return value → next task argument), which catches type errors at parse time instead of at runtime. `PythonOperator` passes context as a dict, which the type checker can't help you with. Same DAG in TaskFlow style is ~30% fewer lines and reads like Python rather than a config file.
6. **How do you keep secrets out of the DAG code?**  Socrata app token comes from `.env` → Airflow Variable / Connection at startup, never hardcoded. In production, swap the env-var loader for a [Secrets Backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html) (AWS SSM, GCP Secret Manager, Vault). The DAG code reads `Variable.get("socrata_app_token")`; either way,  the backend is configuration, not code.
7. **When would you use dynamic task mapping vs. a TaskGroup?**  TaskGroup is for *visual* grouping of tasks you know at parse time (e.g., one staging task per source system, listed statically). Dynamic task mapping (`@task.expand()`) is for *data-driven* fan-out where the count is only known at runtime (e.g., one task per partition discovered by a listing operator). This DAG uses neither a single-source pipeline nor the [Roadmap](#roadmap) lists a dynamic-mapping fan-out across complaint types as a v2 demo.
8. **Why DuckDB and not Postgres / SQLite as the local target?**  Postgres works, but adds a service to Docker Compose. SQLite struggles with the columnar/analytic queries, as well as the dbt mart issues. DuckDB is in-process (no service), columnar (fast on the analytic queries), and dbt-duckdb is a first-class adapter. Same engine MotherDuck runs in production;  this isn't a toy choice.
9. **What's the trade-off of running dbt inside Airflow vs. dbt Cloud / Dagster?**  Airflow + `BashOperator dbt build` is the simplest and most portable. dbt Cloud manages state, environments, and CI for you, but couples you to their pricing. Dagster is asset-first by design and arguably cleaner for dbt-heavy pipelines, but it introduces a second orchestrator. For a team already running Airflow, `BashOperator dbt build` is the right starting point. graduate to a dedicated orchestrator only when the dbt project is the whole pipeline, not one step in it.
10. **How do you alert on failure without inviting alert fatigue?**  Per-task `on_failure_callback` is too noisy if every retry triggers an alert. Standard pattern: alert only on the *final* try (`context['ti'].try_number == context['ti'].max_tries`), and bucket by `dag_id` so a flapping DAG doesn't fire 50 alerts in 10 minutes. `include/callbacks.py` does the first; the second is a roadmap item (would require a small dedup state store).

---

### Roadmap (not in v1)

- [ ] **Dynamic task mapping demo**  fan out one extract task per complaint-type partition discovered at runtime
- [ ] **Sensor pool + concurrency control**  demonstrate how `pool` slots prevent sensor storms on a many-DAG instance
- [ ] **Multi-source merge**  add a second source (e.g., NYC Open Data weather) and join in a downstream dbt mart, to put the multi-source orchestration story on display
- [ ] **Alert dedup**  backend for the `on_failure_callback` to suppress duplicate alerts within a configurable window
- [ ] **Airflow 3.x patterns**  when stable, demonstrate the new task SDK + dag-level retries
- [ ] **Great Expectations / dbt-expectations validation step**  separate task between load and dbt that fails the DAG (and the Dataset emit) on a contract violation
- [ ] **CI: `dbt build` on PR against an ephemeral DuckDB**  The test target is already DuckDB, so that CI can run the entire pipeline on every PR

---

### License

MIT  see [`LICENSE`](LICENSE).

### Author

Associate People BI Analyst @ JD North America (JDNA). I run production GCP pipelines (Airflow + dbt + BigQuery + Data Vault 2.0) for People + Finance teams across a 7-brand retail portfolio. This repo is the public reference for the Airflow patterns I use at work.

📧 linsleymichira@outlook.com
💼 [LinkedIn](https://linkedin.com/in/linsley-michira)
🔗 [linsleymichira.com](https://linsleymichira.com)
