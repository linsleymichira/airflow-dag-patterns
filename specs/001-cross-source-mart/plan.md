# Implementation Plan: Cross-Source Daily Mart

**Branch**: `main` (this repo commits direct to main, no feature branch) | **Date**: 2026-07-13 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/001-cross-source-mart/spec.md`

## Summary

Build a daily analytical mart (`mart_cross_source_daily`) that joins 311 complaints, traffic
crashes, and noise complaints on a date-and-borough grain, exposing per-source daily metrics
(including crash severity: persons injured and killed) plus cross-source derived measures (311
complaints per traffic crash, and 311 complaints per person injured). The feature also lands
the traffic-crash source through the existing config-driven factory, replacing the dead 2014
taxi source. The demo path is a backfill over the source overlap window, because the crash
dataset lags 311 by roughly one month.

That lag drives the two design decisions that most shape the model (both added 2026-07-14 after
the `/speckit-checklist` pass). The mart reprocesses keys touched by newly-loaded source rows
rather than by an event-date recency window, because a recency window anchored on the freshest
source would never reach month-old crash dates. Every per-source metric carries a coverage flag,
so an unpublished date reads as null-and-uncovered rather than as a factual zero.

See [research.md](./research.md) for the resolved technical decisions, [data-model.md](./data-model.md)
for the staging and mart shapes, [contracts/](./contracts/) for the mart output contract and
the source-landing contract, and [quickstart.md](./quickstart.md) for the end-to-end
validation path.

## Technical Context

**Language/Version**: Python 3.12 (DAG factory), SQL (dbt models on DuckDB)

**Primary Dependencies**: Apache Airflow 3 on Astro Runtime 3.2-4, dbt-core + dbt-duckdb, DuckDB, requests, PyYAML (all already in the repo)

**Storage**: DuckDB file at `$AIRFLOW_HOME/include/nyc_311.duckdb` (existing local warehouse)

**Testing**: `pytest tests/dags/` (DAG import + factory-count contract) and dbt built-in tests (`unique`, `not_null`, `dbt_utils.unique_combination_of_columns`)

**Target Platform**: Local Docker via `astro dev start` (laptop, zero cloud)

**Project Type**: Data pipeline (Airflow extract/load factory + dbt transform), not a web or mobile app

**Performance Goals**: Not applicable. Local demo over a bounded backfill window (~6 weeks of NYC open data)

**Constraints**: Laptop-runnable and zero-cloud (Constitution). No new services, no credentials. Crash data lags 311 by ~1 month, so co-occurring rows exist only for dates the crash dataset has published (through ~2026-06-11)

**Scale/Scope**: Demo scale. One new factory source, one new dbt mart, one new plus two updated staging views, one shared macro, one DAG-schedule edit, one README section (FR-012)

**Unknowns**: None. All open questions from the spec were resolved during `/speckit-clarify` and by live-API verification recorded in [research.md](./research.md)

## Constitution Check

*GATE: evaluated against `.specify/memory/constitution.md` v1.0.0. All principles pass.*

|Principle|How this feature complies|
|---|---|
|I. Idempotent, interval-bounded tasks|Collisions extract is bounded by `crash_date` in `[data_interval_start, data_interval_end)` and loads via `INSERT OR REPLACE` on `collision_id`. `crash_date` is a stable event date, so a backfill re-pull upserts revised records idempotently. Because extraction is bounded by `crash_date`, a record NYC publishes long after its event date is landed by running or re-running that record's interval, not by a forward run: the backfill run strategy (Decision 8) is what lands the lagging crash history, and FR-007's guarantee starts once a record is landed. The mart is incremental `delete+insert` keyed on `[activity_date, borough]`, reprocessing keys touched by source rows new since each source's own watermark plus keys whose coverage advanced (Decision 7), so a rebuild against an unchanged snapshot is value-identical.|
|II. Freshness-aware, slot-respecting scheduling|The collisions pipeline inherits the factory's reschedule-mode `PythonSensor`. On bleeding-edge dates the sensor correctly waits (then times out) until NYC publishes the next monthly crash batch. The demo runs via backfill over the published overlap window.|
|III. Data-aware scheduling via Assets|The collisions pipeline emits `Asset("raw_collisions")`. `nyc_311_dbt` subscribes with `schedule=(RAW_311 \| RAW_COLLISIONS \| RAW_NOISE)` (OR semantics) and adds `Asset("mart_cross_source_daily")` as an outlet.|
|IV. Structured failure alerting on every task|The collisions pipeline inherits `on_failure_callback=alert_callback` from the factory `default_args`. No new task bypasses it.|
|V. TaskFlow API + Airflow 3 conventions|No new hand-written DAG. The source is added via the factory (TaskFlow already). No imports outside the Airflow 3 surface.|
|VI. Config-driven scale-out behind a hand-written reference|The crash source is a `include/sources.yaml` edit, not a new DAG file. The hand-written `nyc_311_pipeline` remains the anchor reference.|

**Spec-to-code reconciliation (resolved 2026-07-14)**: the spec clarification originally listed
a "source-count test update" as part of the scope. In this repo `test_total_dag_count` derives
the expected count dynamically as `2 + len(sources)`, so replacing the taxi source with
collisions keeps the count at 4 and needs **no** test edit. The conflict was surfaced by
`checklists/mart.md` CHK020 and the spec text has since been corrected in place, so the plan and
the spec now agree. FR-011 carries the real obligation: the count test must stay green.

**Result**: PASS. No complexity deviations to justify.

## Project Structure

### Documentation (this feature)

```text
specs/001-cross-source-mart/
├── spec.md              # feature spec (committed)
├── plan.md              # this file
├── research.md          # Phase 0 decisions
├── data-model.md        # Phase 1 staging + mart design
├── contracts/           # Phase 1 mart-output + source-landing contracts
│   ├── mart_cross_source_daily.md
│   └── raw_collisions_source.md
├── quickstart.md        # Phase 1 end-to-end validation guide
└── checklists/
    └── requirements.md  # spec quality checklist (committed)
```

### Source code (repository root): files this feature will touch

```text
README.md                                # FR-012: describe the cross-source model

include/
└── sources.yaml                         # REPLACE nyc_taxi entry with nyc_collisions

dags/
└── nyc_311_dbt.py                        # RAW_TAXI_ASSET -> RAW_COLLISIONS_ASSET; add mart outlet

dbt_project/
├── macros/
│   └── normalize_borough.sql            # NEW: canonical borough normalization
└── models/
    ├── staging/
    │   ├── _sources.yml                  # drop raw_taxi, add raw_collisions
    │   ├── _schema.yml                   # drop stg_taxi_trips, add stg_collisions
    │   ├── stg_taxi_trips.sql            # DELETE (taxi retired)
    │   ├── stg_collisions.sql            # NEW: typed view over raw_collisions.records
    │   ├── stg_311_complaints.sql        # normalize_borough; project _loaded_at
    │   └── stg_noise_complaints.sql      # normalize_borough; project _loaded_at
    └── marts/
        ├── _schema.yml                   # add mart_cross_source_daily tests
        └── mart_cross_source_daily.sql   # NEW: the cross-source daily mart
```

**Structure Decision**: This is the existing Airflow-factory + dbt layout. The feature adds one
factory source and one dbt mart, mirroring the established staging-then-mart pattern. Replacing
the taxi source (rather than adding a fourth) matches the spec's "crashes replace taxi" framing
and retires the pre-existing broken taxi config as a side effect.

## Complexity Tracking

No constitution violations. However, the 2026-07-14 amendments materially increased this
feature's complexity, and the honest accounting is below rather than a bare "no entries
required". Every item traces to a requirement, but the aggregate is worth ratifying before
implementation.

|Added complexity|Why it exists|Simpler alternative rejected|
|---|---|---|
|Per-source coverage flags (3 columns) and null-vs-zero counts|FR-004. Crashes lag 311 by a month, so a plain zero would report "no crashes" on every recent date|Accept the zeros and document the lag. Rejected: the mart lies on its freshest rows|
|Two-armed incremental filter (per-source watermarks plus coverage transitions)|FR-007. A calendar window never reaches month-old crash dates, a shared watermark skips independently-landing batches, and a coverage flip touches no source row|Full refresh (table). Rejected as sidestepping rather than modeling the problem|
|Three internal watermark columns|Required by the per-source arm above|One shared watermark. Rejected: silently skips crash batches|
|Contiguous-publication assumption|Coverage is derived from each source's furthest-published event date|A per-date publication manifest. Deferred as heavier than a demo needs|

**Constitution tension worth naming (Principle VI, Simplicity First)**: `mart_cross_source_daily`
is now the most elaborate model in the repo, well past the hand-written `mart_complaints_daily`
reference it was meant to mirror. That is defensible, since the complexity is a faithful response
to a genuinely lagging real-world source and is exactly the judgment a portfolio repo should
demonstrate. It is not free. If the goal is the simplest artifact that teaches multi-source
orchestration, the full-refresh option (research.md Decision 7, rejected) removes the incremental
filter and the watermark columns outright, keeping only the coverage flags.

**Known edge in the coverage model** (implementation watch-item): coverage is derived from
`max(event_date)` over each source's landed rows, which equates "the source's last day with any
activity" to "the source's publication frontier". Those differ when the frontier day itself had
no qualifying events. It bites hardest for noise, whose frontier is really 311's (noise is
filtered 311), so a citywide zero-noise day would read as uncovered rather than as a true zero:
the exact conflation the coverage layer exists to prevent. Real NYC data never hits this (noise
and crashes both occur daily citywide), so it is documented rather than designed around. A
publication manifest keyed on which intervals actually ran would remove the edge.

The one non-obvious operational constraint (not a violation) is the crash-data lag: the demo
depends on a backfill over the source overlap region rather than a forward `@daily` tick. It is
documented in [quickstart.md](./quickstart.md) and is the freshness sensor (Principle II)
behaving correctly, not a workaround.
