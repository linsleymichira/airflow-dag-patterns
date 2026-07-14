<!--
SYNC IMPACT REPORT
Version change: (uninitialized template) -> 1.0.0
Bump rationale: Initial ratification. All placeholder tokens replaced with concrete,
  project-specific principles derived from README.md and CLAUDE.md.
Modified principles: all five template placeholders replaced. Final set (6 principles):
  I. Idempotent, Interval-Bounded Tasks (NON-NEGOTIABLE)
  II. Freshness-Aware, Slot-Respecting Scheduling
  III. Data-Aware Scheduling via Assets
  IV. Structured Failure Alerting on Every Task
  V. TaskFlow API + Airflow 3 Conventions
  VI. Config-Driven Scale-Out Behind a Hand-Written Reference
Added sections: "Technology & Runtime Constraints", "Development Workflow & Quality Gates".
Removed sections: none (template SECTION_2 / SECTION_3 placeholders filled).
Templates status:
  .specify/templates/plan-template.md ......... OK (Constitution Check gate reads principles generically, no edit needed)
  .specify/templates/spec-template.md ......... OK (no constitution references)
  .specify/templates/tasks-template.md ........ OK (no constitution references)
  .specify/templates/checklist-template.md .... OK (no constitution references)
Runtime guidance: CLAUDE.md and README.md already encode these rules, no edits required.
Deferred / follow-up TODOs:
  RATIFICATION_DATE set to 2026-07-13 (date of adoption). Change if an earlier original
  adoption date is preferred.
-->

# airflow-dag-patterns Constitution

airflow-dag-patterns is a runnable reference for production Airflow 3 orchestration patterns.
Its value is pedagogical: every DAG, model, and config is an exemplar a reviewer can trust and
copy. The principles below are the non-negotiables that keep it exemplary. They bind every
new DAG, dbt model, and factory source added to the repo.

## Core Principles

### I. Idempotent, Interval-Bounded Tasks (NON-NEGOTIABLE)

Every task MUST be a pure function of its data interval, and reruns MUST produce identical
output.

- Tasks MUST derive their working window from `data_interval_start` / `data_interval_end`.
  Wall-clock reads (`now()`, "since last run", `datetime.now()`) as query bounds are forbidden.
- Loads MUST be partition-overwrite-safe: upsert on a natural key (`INSERT OR REPLACE` on
  the configured primary key), never a bare `INSERT`.
- A clear-and-rerun or `airflow dags backfill` across the same interval MUST yield the same
  rows with no duplicates.

Rationale: idempotency is what makes backfills, retries, and on-call reruns safe. It is the
foundation every other pattern in this repo assumes.

### II. Freshness-Aware, Slot-Respecting Scheduling

A DAG MUST NOT burn a worker slot waiting on stale upstream data.

- Extraction MUST be gated by a freshness check against the source before any pull runs.
- Sensors that wait on external state MUST run in `mode="reschedule"`, never `poke`, so the
  worker slot is released between polls.
- The freshness decision (how stale is too stale to proceed) is a deliberate, documented
  choice. The hand-written reference DAG keeps that decision as an explicit authored hook
  and MUST NOT be silently auto-filled.

Rationale: poke-mode sensors hold a slot for their whole timeout and starve a fixed pool.
Reschedule mode is the difference between DAGs that run on time and DAGs that queue forever.

### III. Data-Aware Scheduling via Assets

Cross-DAG dependencies MUST be event-driven through Airflow 3 Assets, not push or poll.

- A producer task MUST emit its `Asset(...)` outlet on successful load. A consumer DAG MUST
  subscribe via `schedule=<asset expression>`.
- `TriggerDagRunOperator` and `ExternalTaskSensor` are forbidden for this wiring.
- Asset trigger semantics MUST be explicit: `|` for OR (fire on any source), a list for AND
  (wait for all). The intended semantics MUST be stated in a comment where the schedule is
  declared.
- Asset URIs are the coupling contract. Renaming a URI MUST be done in every producer and
  consumer in the same change.

Rationale: Assets decouple scheduling from wiring and let many consumers subscribe to one
producer. Push and poll patterns hard-code one-to-one dependencies that break when intervals
drift.

### IV. Structured Failure Alerting on Every Task

Every task MUST wire `on_failure_callback=alert_callback` (directly or via `default_args`).

- The callback MUST emit a single machine-parseable JSON line carrying run context (`dag_id`,
  `task_id`, `run_id`, data interval, try number, exception type and message, log URL).
- Alerts MUST fire only on the final try, so retries do not page.
- The alert payload shape is the contract a webhook backend consumes. Changing its keys is a
  breaking change to that contract.

Rationale: a consistent, final-try-only, JSON-shaped alert is what a Slack / PagerDuty /
OpsGenie backend ingests without custom parsing, and it is what prevents alert fatigue.

### V. TaskFlow API + Airflow 3 Conventions

DAGs MUST be written in the modern decorator style against the Airflow 3 Task SDK.

- Use `@dag` / `@task` with implicit XCom via return values. `PythonOperator` + `op_kwargs`
  string plumbing is avoided for new task graphs.
- Imports MUST use Airflow 3 paths: `from airflow.sdk import ...` and
  `from airflow.providers.standard...`. Airflow 2 paths (`airflow.decorators`,
  `airflow.operators.*`, `airflow.sensors.*`) MUST NOT be introduced.
- Secrets MUST be read via `Variable.get(...)` (production: a Secrets Backend), never
  hardcoded in DAG code.

Rationale: TaskFlow catches type errors at parse time and reads like Python. Pinning the
Airflow 3 import surface keeps the repo from silently regressing to 2.x idioms.

### VI. Config-Driven Scale-Out Behind a Hand-Written Reference

Families of similar pipelines MUST scale out through the config factory, not copy-paste, and
MUST stay anchored by one readable reference DAG.

- Adding a like-shaped source MUST be a `include/sources.yaml` edit (plus its dbt staging
  model), not a new hand-written DAG file.
- Every factory-generated DAG MUST carry the same five patterns (I-V) as the hand-written
  reference.
- Exactly one hand-written reference DAG MUST remain in `dags/` as the anchor a reviewer
  reads first. It is intentionally not generated by the factory.
- A new source MUST NOT require test edits. Adding a new hand-written DAG MUST bump the
  DAG-count assertion in the same change, and be added to the hand-written-DAG assertion set.

Rationale: the factory encodes the patterns once so the sixteenth pipeline costs six lines of
YAML. The reference DAG keeps the abstraction debuggable when a generated peer misbehaves.

## Technology & Runtime Constraints

- **Laptop-runnable, zero-cloud.** The whole pipeline MUST run end-to-end from a single
  `astro dev start` with no cloud accounts, credentials, or paid services. New dependencies
  MUST preserve this.
- **Pinned local stack.** Astro Runtime 3.2-4 (Airflow 3.x), dbt-core + dbt-duckdb, DuckDB as
  the local warehouse target. Do not pin providers already shipped by the Runtime image.
- **dbt owns lineage.** Transformation dependency order lives in the dbt project, selected via
  `state:modified+`. Airflow decides *when* to run dbt (Asset trigger), dbt decides *what*
  rebuilds. Do not push model-to-model dependencies into Airflow.
- **`AIRFLOW_HOME` is the anchor path.** The DuckDB file, `include/sources.yaml`, and the dbt
  profile all resolve relative to it. New paths MUST follow the same convention.
- **Committed config carries no secrets.** `dbt_project/profiles.yml` and
  `airflow_settings.yaml` are committed because they hold only local paths and env-var
  references. Any file that would carry a secret MUST stay gitignored.

## Development Workflow & Quality Gates

- **Parse gate.** Every DAG MUST import cleanly under `astro dev parse` and under
  `pytest tests/dags/` before merge.
- **Factory contract test.** `tests/dags/test_dag_imports.py` MUST continue to assert that
  every `include/sources.yaml` source registers and that the total DAG count matches
  `2 + len(sources)`. A silently skipped factory entry is a failing build, not a warning.
- **CI on every PR.** GitHub Actions runs the DAG import tests on every pull request and on
  pushes to `main`. A red CI run blocks merge.
- **Design rationale lives in the README.** Any new pattern or non-obvious trade-off MUST be
  explained in `README.md`, and operational conventions in `CLAUDE.md`, so a reviewer can
  reconstruct the *why*, not just the *what*.

## Governance

- The constitution supersedes ad-hoc convention. Where a proposed change conflicts
  with a principle, the principle wins unless the constitution is amended first.
- **Amendments** require: a written change to this file, a version bump per the
  policy below, and a refreshed Sync Impact Report at the top of the file.
- **Versioning policy** (semantic):
  - MAJOR: a principle is removed or redefined in a backward-incompatible way.
  - MINOR: a new principle or section is added, or guidance is materially expanded.
  - PATCH: wording, clarifications, or typo fixes with no change in meaning.
- **Compliance review.** Every PR MUST verify it upholds Principles I-VI. Any deliberate
  deviation MUST be recorded in the plan's Complexity Tracking table with the justification
  and the simpler alternative that was rejected.
- **Runtime guidance.** Day-to-day operational guidance for agents and contributors
  lives in `CLAUDE.md`. It elaborates these principles but MUST NOT contradict them.

**Version**: 1.0.0 | **Ratified**: 2026-07-13 | **Last Amended**: 2026-07-13
