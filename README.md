## airflow-dag-patterns

Built entirely on public NYC Open Data (Socrata). No employer or proprietary data is used anywhere in this repo.

A minimal, runnable Airflow 3 reference pipeline demonstrating **six production orchestration patterns** on a live public API . one hand-written reference DAG, plus a **config-driven DAG factory** that generates peers from `include/sources.yaml`. Built with **Astro Runtime + dbt-core + DuckDB** so the whole pipeline runs end-to-end on a single `astro dev start`, no cloud accounts, no credentials, no waiting.

> **Why this repo exists:** Airflow tutorials show you how to write *a* DAG. Production Airflow shows you how to write one that survives an on-call rotation . and how to write the *next* fifteen without copy-paste. This repo includes idempotent tasks, freshness-aware sensors, data-aware downstream triggering, structured alerting, a real dbt handoff, and a config-driven factory that turns a YAML file into N parallel pipelines. All on a laptop in five minutes.

---

### What it demonstrates

1. **Idempotent task design** Every task is bound to `data_interval_start` / `data_interval_end`, so reruns and backfills produce the same output rows: partition-overwrite-safe loads, no duplicates, no surprises on a clear-and-rerun.
2. **Freshness-aware scheduling** A `PythonSensor` in `reschedule` mode polls the Socrata API's `:updated_at` field before any extract runs, so the DAG never burns a worker slot waiting on stale upstream data. Reschedule mode releases the slot between polls, the way you'd actually run this in production.
3. **Data-aware downstream (Airflow 3 Assets)**  the load task emits an `Asset("raw_311")` event. The dbt DAG is scheduled on the asset, not on a cron. The dbt run fires the moment new data lands, not three minutes later, because the cron happened to align.
4. **Structured `on_failure_callback` alerting**  every task wires a single callback that emits a JSON-shaped log line with run context (`dag_id`, `task_id`, `data_interval`, `try_number`, exception type, exception message). The exact shape an alerting backend (Slack, PagerDuty, OpsGenie) consumes without further parsing drops into a webhook, and you're paging.
5. **TaskFlow API** The DAG is written in the modern decorator style (`@dag`, `@task`), with explicit XCom passing via return values rather than `xcom_push` / `xcom_pull` string lookups. Type-friendly, lint-friendly, and the style every Airflow doc has used since 2.0.
6. **Config-driven DAG factory**  `dags/factories/source_factory.py` reads `include/sources.yaml` and registers one extract+load DAG per source listed. The five patterns above are encoded *once* inside `build_source_dag(cfg)` . adding a new source means ~6 lines of YAML, not a new DAG file. The hand-written `nyc_311_pipeline` DAG stays in `dags/` as the readable reference; the factory generates its peers.

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
  load_to_duckdb                 ← INSERT OR REPLACE on unique_key
        │                          (partition-overwrite-safe upsert)
        ▼
  emits Asset("raw_311") ───────┐
                                │  (Airflow 3 data-aware scheduling)
                                ▼
                          dbt_run DAG     ← schedule=[Asset("raw_311"), Asset("raw_taxi"), Asset("raw_noise")]
                                │           runs `dbt build --select state:modified+`
                                ▼
                          emits Asset("mart_complaints_daily")
```

Full diagram with task names, retry/SLA settings, and Asset URIs: [`docs/architecture.png`](docs/architecture.png).

Every task wires `on_failure_callback=alert_callback` from `include/callbacks.py`.

#### DAG factory layer

The diagram above shows *one* DAG (`nyc_311_pipeline`, hand-written as the readable reference). At scheduler-parse-time, the factory generates additional DAGs from config:

```
include/sources.yaml                ← YAML config, one block per source
        │
        ▼
dags/factories/source_factory.py    ← reads YAML, loops, registers DAGs
        │
        ├─►  nyc_311_pipeline               (hand-written, in dags/, the reference)
        ├─►  nyc_taxi_pipeline              (factory-generated)
        └─►  nyc_noise_complaints_pipeline  (factory-generated)
                │
                │  each emits its own Asset on successful load
                ▼
        Assets: raw_311, raw_taxi, raw_noise
                │
                ▼
        nyc_311_dbt DAG  (schedule = [Asset("raw_311"), Asset("raw_taxi"), Asset("raw_noise")])
                │       single dbt DAG, multi-Asset trigger . Airflow runs it on any source landing
                ▼
        dbt build --select state:modified+
```

Every factory-generated DAG inherits the same five patterns as the hand-written reference (idempotency, freshness sensor, Asset outlet, TaskFlow, `on_failure_callback`). Adding a fourth source = ~6 YAML lines + one staging model. No new DAG file.

---

### Stack

- **Orchestrator:** Apache Airflow 3 on [Astro Runtime 3.2+](https://docs.astronomer.io/astro/runtime-release-notes) (free, open-source, recruiters recognize the `Dockerfile` + `astro dev` shape)
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
| `load_to_duckdb` | `@task` | `INSERT OR REPLACE` into `raw_311.complaints` keyed on `unique_key`. Partition-overwrite-safe: same interval reruns produce the same rows. Outlets `Asset("raw_311")`. |

#### `dags/nyc_311_dbt.py`  the transform DAG

| Task | Type | Purpose |
|---|---|---|
| `dbt_build` | `BashOperator` | Runs `dbt build --select state:modified+ --profiles-dir /usr/local/airflow/dbt_project` from the embedded dbt project. Schedule: `[Asset("raw_311"), Asset("raw_taxi"), Asset("raw_noise")]`  multi-Asset trigger, fires whenever *any* source lands. `state:modified+` selector means only the affected models rebuild. Outlets `Asset("mart_complaints_daily")`. |

> **Why one dbt DAG with a multi-Asset trigger instead of one dbt DAG per source?** The dbt project's lineage graph is already the source of truth . `state:modified+` reads the lineage and rebuilds the right slice automatically. Pairing one dbt DAG per source pushes responsibility for "what depends on what" into Airflow, where it doesn't belong.

#### `dags/factories/source_factory.py`  the DAG factory

| Function | Purpose |
|---|---|
| `build_source_dag(cfg)` | Takes one entry from `sources.yaml` (a dict with `name`, `socrata_id`, `primary_key`, `freshness_field`, `asset_uri`, `schedule`, optional `where_filter`) and returns a fully-wired `@dag`-decorated Airflow DAG. Every generated DAG carries the same five patterns as the hand-written reference: idempotent bounded interval, reschedule-mode freshness sensor, `INSERT OR REPLACE`-based load, Asset outlet, and `on_failure_callback`. |
| `register_dags()` | Module-level loop: reads `include/sources.yaml`, iterates, calls `build_source_dag()` for each entry, and assigns the returned DAG to a unique global name (`globals()[f"{name}_pipeline"] = dag`) so the scheduler picks it up. |

`include/sources.yaml` shape:

```yaml
sources:
  - name: nyc_taxi
    socrata_id: gkne-dk5s
    primary_key: trip_id
    freshness_field: tpep_pickup_datetime
    asset_uri: raw_taxi
    schedule: "@hourly"
  - name: nyc_noise_complaints
    socrata_id: erm2-nwe9
    primary_key: unique_key
    freshness_field: ":updated_at"
    asset_uri: raw_noise
    schedule: "@daily"
    where_filter: "complaint_type='Noise'"
```

> The hand-written `nyc_311_pipeline` DAG is **intentionally not generated by the factory** . it's the readable reference a hiring manager opens first. The factory generates its peers. Trade-off: a reviewer can't `grep dags/nyc_taxi_pipeline.py` because that file doesn't exist; the reference DAG is the anchor for understanding the generated ones.

#### `include/callbacks.py`  shared alert callback

A single `alert_callback(context)` function that every task wires via `on_failure_callback`. Emits a JSON log line with `dag_id`, `task_id`, `data_interval_start`, `data_interval_end`, `try_number`, `exception_type`, `exception_message`, `log_url`. Drop in a Slack/PagerDuty webhook by changing one line.

> The exact freshness threshold (how stale is "stale enough" to skip the run vs. proceed?) is the design decision in this repo most worth thinking about . too lax wastes credits on empty Asset events; too strict misses legitimate slow days. NYC 311 has known weekend cadences, so the production-grade rule keys off `:updated_at > data_interval_start` AND `row_count > 0` with the row-count threshold configurable per environment.

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
2. **What does "idempotent" mean for an Airflow task, and how doThe subsequentntee it?**  The same task instance, given the same inputs (`data_interval_start`, `data_interval_end`), produces the same outputs no matter how many times it runs. This DAG guarantees it by (a) bounding the API query by `:updated_at` between the interval bounds, not "since now," and (b) upserting on the natural key (`unique_key`) via `INSERT OR REPLACE`, not a plain `INSERT`.
3. **Why Assets over `TriggerDagRunOperator` / `ExternalTaskSensor`?**  `TriggerDagRunOperator` is a one-way push: the upstream DAG decides when downstream runs, regardless of whether the downstream is healthy. `ExternalTaskSensor` is a polling pattern: the downstream waits for the upstream's task instance and breaks subtly when intervals don't align. `Asset` is event-driven: upstream emits, downstream subscribes, scheduling is decoupled. Multiple downstream DAGs can subscribe to the same Asset; older patterns enforce one-to-one wiring.
4. **How do you parameterize backfills without breaking idempotency?**  `airflow dags backfill` walks `data_interval_start` / `_end` across the requested range. Because every task in this DAG reads only those two parameters and writes via `INSERT OR REPLACE` on the natural key, a backfill produces the same output as the original runs. No "since when" timestamps anywhere in the code. `[Confirm tested via airflow dags backfill in setup checklist]`
5. **Why TaskFlow API instead of `PythonOperator` + `op_kwargs`?**  TaskFlow makes XCom passing implicit (return value → next task argument), which catches type errors at parse time instead of at runtime. `PythonOperator` passes context as a dict, which the type checker can't help you with. Same DAG in TaskFlow style is ~30% fewer lines and reads like Python rather than a config file.
6. **How do you keep secrets out of the DAG code?**  Socrata app token comes from `.env` → Airflow Variable / Connection at startup, never hardcoded. In production, swap the env-var loader for a [Secrets Backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html) (AWS SSM, GCP Secret Manager, Vault). The DAG code reads `Variable.get("socrata_app_token")`; either way,  the backend is configuration, not code.
7. **When would you use dynamic task mapping vs. a TaskGroup?**  TaskGroup is for *visual* grouping of tasks you know at parse time (e.g., one staging task per source system, listed statically). Dynamic task mapping (`@task.expand()`) is for *data-driven* fan-out where the count is only known at runtime (e.g., one task per partition discovered by a listing operator). This DAG uses neither a single-source pipeline nor the [Roadmap](#roadmap) lists a dynamic-mapping fan-out across complaint types as a v2 demo.
8. **Why DuckDB and not Postgres / SQLite as the local target?**  Postgres works, but adds a service to Docker Compose. SQLite struggles with the columnar/analytic queries, as well as the dbt mart issues. DuckDB is in-process (no service), columnar (fast on the analytic queries), and dbt-duckdb is a first-class adapter. Same engine MotherDuck runs in production;  this isn't a toy choice.
9. **What's the trade-off of running dbt inside Airflow vs. dbt Cloud / Dagster?**  Airflow + `BashOperator dbt build` is the simplest and most portable. dbt Cloud manages state, environments, and CI for you, but couples you to their pricing. Dagster is asset-first by design and arguably cleaner for dbt-heavy pipelines, but it introduces a second orchestrator. For a team already running Airflow, `BashOperator dbt build` is the right starting point. graduate to a dedicated orchestrator only when the dbt project is the whole pipeline, not one step in it.
10. **How do you alert on failure without inviting alert fatigue?**  Per-task `on_failure_callback` is too noisy if every retry triggers an alert. Standard pattern: alert only on the *final* try (`context['ti'].try_number == context['ti'].max_tries`), and bucket by `dag_id` so a flapping DAG doesn't fire 50 alerts in 10 minutes. `include/callbacks.py` does the first; the second is a roadmap item (would require a small dedup state store).
11. **When do you reach for a DAG factory vs. a hand-written DAG?**  Factory when N similar pipelines share ≥80% of their shape (same source-system family, same load pattern, only inputs/schedules vary). Hand-written when the DAG is one-of-a-kind, has unusual branching, or carries enough custom logic that the factory's parameter surface would balloon to ~the same size as the DAG itself. The line: if the YAML for a new source is shorter than the diff against an existing DAG, factory wins. If they're comparable in size, hand-written readability beats clever abstraction. This repo demonstrates both because that's how production Airflow actually looks: a handful of bespoke reference DAGs alongside a generated long tail.
12. **What's the cost of a DAG factory in production?**  Two things. **(a) Scheduler parse-time:** the factory module runs on every scheduler heartbeat (default 30s). A factory that reads a 200-source YAML and constructs DAG objects every time burns CPU and stalls the scheduler. Mitigations: cache the parsed YAML at module load with `functools.lru_cache`, or move to a file-watched config with a separate generation step. **(b) Debugging:** stack traces and the Airflow UI point you at "the factory file" instead of "the DAG file"; a reviewer can't grep `dags/nyc_taxi_pipeline.py` because that file doesn't exist. Mitigation: keep the factory short (<200 lines), make the YAML a readable spec, and pin one hand-written reference DAG so anyone debugging a generated peer can compare structures.

---

### Roadmap (not in v1)

- [ ] **Dynamic task mapping demo**  fan out one extract task per complaint-type partition discovered at runtime (different shape from the DAG factory: factory is DAG-level fan-out at parse time, dynamic mapping is task-level fan-out at runtime)
- [ ] **Cross-source joined mart**  the factory already lands `raw_311`, `raw_taxi`, `raw_noise` as parallel sources. Roadmap is a downstream dbt mart that joins across them (e.g., complaints-per-taxi-trip-density), putting the multi-source *modeling* story on top of the multi-source *orchestration* story already shipped
- [ ] **Sensor pool + concurrency control**  demonstrate how `pool` slots prevent sensor storms on a many-DAG instance (especially relevant once the factory generates ≥10 DAGs)
- [ ] **Factory-level parse-time caching**  `lru_cache` the YAML read so the factory doesn't re-parse on every scheduler heartbeat. Worth showing once the factory crosses ~5 sources
- [ ] **Alert dedup**  backend for the `on_failure_callback` to suppress duplicate alerts within a configurable window
- [ ] **Great Expectations / dbt-expectations validation step**  separate task between load and dbt that fails the DAG (and the Asset emit) on a contract violation
- [ ] **CI: `dbt build` on PR against an ephemeral DuckDB**  the test target is already DuckDB, so CI can run the entire pipeline on every PR

---

### License

MIT  see [`LICENSE`](LICENSE).

### Author

Associate People BI Analyst @ JD North America (JDNA). I run production GCP pipelines (Airflow + dbt + BigQuery + Data Vault 2.0) for People + Finance teams across a 7-brand retail portfolio. This repo is the public reference for the Airflow patterns I use at work.

📧 linsleymichira@outlook.com
💼 [LinkedIn](https://linkedin.com/in/linsley-michira)
🔗 [linsleymichira.com](https://linsleymichira.com)
