---

description: "Task list for Cross-Source Daily Mart"
---

# Tasks: Cross-Source Daily Mart

**Input**: Design documents from `/specs/001-cross-source-mart/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/](./contracts/), [quickstart.md](./quickstart.md)

**Tests**: Included. The design mandates them: [contracts/mart_cross_source_daily.md](./contracts/mart_cross_source_daily.md)
fixes an enforcing-test list, and FR-009 plus SC-003 require reconciliation. Test tasks here are
dbt data tests plus the existing `pytest tests/dags/` contract, not new unit-test scaffolding.

**Organization**: Grouped by user story so each is independently implementable and testable.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story the task belongs to (US1, US2, US3)
- Exact file paths are in each description

## Path Conventions

Repository root is `/Users/linsleymichira/GitHub/airflow-dag-patterns`. The repo is an Airflow
factory plus dbt project, not a `src/`-style app: DAG code lives in `dags/`, config in
`include/`, models in `dbt_project/models/`, and DAG contract tests in `tests/dags/`.

## Verified preconditions (do not re-derive)

- All three landing tables already carry `_loaded_at`: `raw_311.complaints`
  (`dags/nyc_311_pipeline.py:217`) and the factory's `raw_<x>.records`
  (`dags/factories/source_factory.py:190`). Staging only needs to project it.
- `dbt_utils` is already declared in `dbt_project/packages.yml`, but neither `dbt_project/macros/`
  nor `dbt_project/tests/` exists yet, even though `dbt_project.yml` sets `macro-paths:
  ["macros"]` and `test-paths: ["tests"]`.
- `tests/dags/test_dag_imports.py::test_total_dag_count` derives its expectation as
  `2 + len(sources)`. Replacing taxi with collisions keeps the count at 4, so **no test edit is
  needed** (FR-011 requires only that it stays green).
- The factory sensor is `PythonSensor(mode="reschedule", poke_interval=300, timeout=6h)` with no
  `soft_fail`, so a timeout currently fails the task and fires `alert_callback`. FR-008 requires
  a stall be distinguishable from a genuine failure.
- 311 and noise read the same dataset (`erm2-nwe9`), noise filtered to `complaint_type='Noise'`.
  Both key on `unique_key`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Make the dbt project able to hold a macro and resolve `dbt_utils`.

- [ ] T001 Create the `dbt_project/macros/` and `dbt_project/tests/` directories. `dbt_project/dbt_project.yml` already points at both (`macro-paths: ["macros"]`, `test-paths: ["tests"]`) but neither exists: the macro dir holds T005's `normalize_borough`, and the tests dir holds T039's singular severity test
- [ ] T002 Install dbt packages so `dbt_utils` test macros resolve: run `dbt deps --profiles-dir . --project-dir .` from `dbt_project/`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Land the traffic-crash source, retire taxi, and align the borough domain. Every
user story depends on this phase, because no story can build without the crash source present
and the three sources sharing a join key.

**⚠️ CRITICAL**: No user story phase can start until this phase is complete.

- [ ] T003 Replace the `nyc_taxi` entry with `nyc_collisions` in `include/sources.yaml` (`socrata_id: h9gi-nx95`, `primary_key: collision_id`, `freshness_field: crash_date`, `asset_uri: raw_collisions`, `schedule: "@daily"`), per [contracts/raw_collisions_source.md](./contracts/raw_collisions_source.md)
- [ ] T004 Make `soft_fail` an **opt-in per-source** knob rather than a global sensor change, so only the lagging source skips on timeout (FR-008). In `dags/factories/source_factory.py` read `soft_fail = cfg.get("soft_fail", False)` and pass it to the `PythonSensor`, mirroring how `where_filter` is already read at line 70. In `include/sources.yaml` set `soft_fail: true` on `nyc_collisions` only (paired with T003), with a comment that crashes publish ~monthly so a timeout is the lag, not a fault. The default of `false` is what keeps the blast radius at zero: `nyc_311_complaints` and `nyc_noise_complaints` publish continuously, so for them a 6-hour timeout is a genuine publication stall that must still fail and page. A global `soft_fail=True` would silence exactly that alert on all three sources. Do NOT touch the sensor in `dags/nyc_311_pipeline.py`: its `NotImplementedError` is a deliberate teaching landmine
- [ ] T005 [P] Create the `normalize_borough(column)` macro in `dbt_project/macros/normalize_borough.sql`, trimming and upper-casing to the canonical five and mapping everything else to the literal `Unknown` (FR-005, [data-model.md](./data-model.md))
- [ ] T006 Update `dags/nyc_311_dbt.py`: replace `RAW_TAXI_ASSET` with `RAW_COLLISIONS_ASSET = Asset("raw_collisions")`, set `schedule=(RAW_311_ASSET | RAW_COLLISIONS_ASSET | RAW_NOISE_ASSET)` (OR semantics, FR-008), and add `Asset("mart_cross_source_daily")` to the task outlets
- [ ] T007 [P] Update `dbt_project/models/staging/_sources.yml`: drop the `raw_taxi` source, add `raw_collisions` (schema `raw_collisions`, table `records`, `collision_id` tested not_null + unique)
- [ ] T008 [P] Delete `dbt_project/models/staging/stg_taxi_trips.sql` (FR-011, taxi retired)
- [ ] T009 [P] Create `dbt_project/models/staging/stg_collisions.sql` as a view over `source('raw_collisions', 'records')`, typing `collision_id`, `crash_at`, `crash_date`, `persons_injured`, `persons_killed` from the JSON payload, applying `normalize_borough(raw ->> 'borough')`, and projecting `_loaded_at` (column list in [data-model.md](./data-model.md))
- [ ] T010 [P] Update `dbt_project/models/staging/stg_311_complaints.sql`: replace `nullif(trim(borough), '')` with `{{ normalize_borough('borough') }}` and project `_loaded_at` through
- [ ] T011 [P] Update `dbt_project/models/staging/stg_noise_complaints.sql`: replace `nullif(trim(raw ->> 'borough'), '')` with `{{ normalize_borough("raw ->> 'borough'") }}` and project `_loaded_at` through
- [ ] T012 Update `dbt_project/models/staging/_schema.yml`: drop the `stg_taxi_trips` entry, add `stg_collisions` (`collision_id` not_null + unique, `crash_date` not_null, `_loaded_at` not_null) and add `_loaded_at` not_null to `stg_311_complaints` and `stg_noise_complaints`
- [ ] T013 Run `astro dev parse` and `pytest tests/dags/ -v` to confirm the collisions DAG registers, the taxi DAG is gone, and `test_total_dag_count` stays green at 4 with no test edit (FR-011)
- [ ] T014 Land the fixture data per [quickstart.md](./quickstart.md) step 1: backfill `nyc_311_pipeline` over `2026-05-01..2026-07-10` and `nyc_collisions_pipeline` over `2026-05-01..2026-06-10`. The asymmetry is required, and produces the ≥30-day covered overlap and ≥7-day uncovered region the spec's Assumptions mandate

**Checkpoint**: Three sources land, boroughs share a domain, and the DAG contract is green.

---

## Phase 3: User Story 1 - Compare NYC domains in one daily table (Priority: P1) 🎯 MVP

**Goal**: One daily table placing 311, crash, and noise metrics side by side per borough per
day, so domains are compared without hand-joining.

**Independent Test**: Build the model and confirm a single query returns one row per borough per
day carrying each source's daily metric, matching values computed by hand from each source's own
daily aggregate.

- [ ] T015 [US1] Create `dbt_project/models/marts/mart_cross_source_daily.sql` with the per-source daily aggregate CTEs (`c311`, `crashes`, `noise`), each grouping to `(activity_date, borough)` and carrying `max(_loaded_at) as loaded_at`. Aggregate severity **all-or-null**, `case when count(persons_injured) = count(*) then sum(persons_injured) end`, never a plain `sum()` and never coalesced to 0. Verified against the live `h9gi-nx95` API: the source writes an explicit `0` when it means zero (895 of 903 reporting rows in the demo window), so a missing field is a pending determination, and 90.7% of recent rows omit `number_of_persons_killed`. Expect null `persons_killed` on most recent borough-days: that is the correct answer, not a defect (see [data-model.md](./data-model.md))
- [ ] T016 [US1] Add the `spine` CTE to `dbt_project/models/marts/mart_cross_source_daily.sql`, unioning the three sources' `(activity_date, borough)` keys, then left-join each aggregate onto it so a key survives whenever any source reported (FR-004, SC-002)
- [ ] T017 [US1] Materialize `mart_cross_source_daily` as a plain table for now via `{{ config(materialized='table') }}`, deferring the incremental filter to Phase 6 so US1 is testable on its own
- [ ] T018 [US1] Add the `mart_cross_source_daily` entry to `dbt_project/models/marts/_schema.yml` with `not_null` on `activity_date` and `borough`, `dbt_utils.unique_combination_of_columns(['activity_date', 'borough'])`, and `accepted_values` on `borough` = the six canonical values (FR-005, contract)
- [ ] T019 [US1] Run `dbt build --profiles-dir . --project-dir .` from `dbt_project/` and confirm the model builds and its tests pass
- [ ] T020 [US1] Reconcile the Phase 3 table against its inputs (FR-009, and the strata of SC-003 that exist at this phase): for each source, verify its count equals that source's standalone daily aggregate across a sample of at least 10 keys **that source actually populated**, spanning two or more boroughs and including at least one `Unknown`-borough key, all drawn from the covered overlap region (`2026-05-01..2026-06-10`). Restricting to populated keys is load-bearing, not laziness: the model is still a plain table with no coverage gating until T026, so any key a source did not populate reads NULL here while its standalone aggregate reads 0, and an equality check over that key would fail on correct data. SC-003's empty-but-covered and uncovered strata are therefore verified at T042, once coverage semantics exist

**Checkpoint**: US1 is independently deliverable. The table answers "how did 311 and crashes move
together in this borough over this range?" with no manual join (SC-001).

---

## Phase 4: User Story 2 - A measure that only exists across sources (Priority: P2)

**Goal**: Expose measures computable only by combining sources, proving modeling value beyond
co-locating counts.

**Independent Test**: Compute the derived measure by hand from the per-source daily aggregates
for a sample borough and day, then confirm the model's value matches within rounding.

- [ ] T021 [US2] Add both derived measures to `dbt_project/models/marts/mart_cross_source_daily.sql`: `complaints_per_crash` as `complaint_count::double / nullif(crash_count, 0)` and `complaints_per_person_injured` as `complaint_count::double / nullif(persons_injured, 0)`. Both are mandatory, not "at least one" (FR-003)
- [ ] T022 [US2] Add `dbt_utils.expression_is_true` tests to `dbt_project/models/marts/_schema.yml` asserting each measure is null exactly when its numerator is unavailable or its denominator is unusable: `complaints_per_crash is null = (complaint_count is null or crash_count is null or crash_count = 0)` and `complaints_per_person_injured is null = (complaint_count is null or persons_injured is null or persons_injured = 0)`. The numerator arm is load-bearing: a null `complaint_count` nulls the ratio even where the denominator is fine, so omitting it fails the test on correct data (FR-006, contract)
- [ ] T023 [US2] Run `dbt build` and verify per [quickstart.md](./quickstart.md) that a borough-day with zero crashes yields a null measure rather than an error, with its row retained (SC-005)

**Checkpoint**: US1 and US2 both work. The mart carries the flagship and severity-normalized
measures.

---

## Phase 5: User Story 3 - Partial-source days stay complete (Priority: P3)

**Goal**: Keep a row when only one source reported, labelling each absent source as
published-and-empty or not-yet-published rather than dropping the row or faking a zero.

**Independent Test**: Provide data for only one source on a day and confirm the row survives with
each other source labelled zero-and-covered or null-and-uncovered per whether it published.

- [ ] T024 [US3] Add the `coverage` CTE to `dbt_project/models/marts/mart_cross_source_daily.sql` computing each source's publication frontier from `max(event_date)`. **Noise must reuse 311's frontier** (`max(complaint_date)` from `stg_311_complaints`), because noise is 311 filtered to noise types, so deriving it from `max(noise complaint_date)` would read a citywide zero-noise day as unpublished. The frontier is a proxy valid only for contiguous, daily-dense sources: FR-004 is scoped to those, and adding a source that is neither needs a publication manifest instead
- [ ] T025 [US3] Add the three `*_covered` flags (`c311_covered`, `crashes_covered`, `noise_covered`) to `dbt_project/models/marts/mart_cross_source_daily.sql` as `coalesce(activity_date <= <source>_max_date, false)`. The `coalesce(..., false)` matters: an empty source leaves the frontier null, which must read as uncovered rather than as an unknown three-valued result
- [ ] T026 [US3] Gate every per-source count on its coverage flag in `dbt_project/models/marts/mart_cross_source_daily.sql`: a count is `coalesce(<agg>, 0)` only when covered, and null otherwise (FR-004). For severity, emit 0 only when the crash source is covered **and** the day genuinely had no crashes (`crashes.crash_count is null`), pass the all-or-null aggregate through unchanged when covered with crashes, and emit null whenever the source is uncovered. Nesting the crash-count branch inside the coverage branch is what keeps an uncovered date from reading as a fabricated zero
- [ ] T027 [US3] Add `dbt_utils.expression_is_true` tests to `dbt_project/models/marts/_schema.yml` asserting a count is null if and only if its source is uncovered, for example `(crash_count is null) = (not crashes_covered)`, plus `not_null` on the three coverage flags. Counts and severity stay nullable by design (contract)
- [ ] T028 [US3] Add the covered-measure non-negativity test to `dbt_project/models/marts/_schema.yml`, null-tolerant on severity: `not crashes_covered or (crash_count >= 0 and coalesce(persons_injured, 0) >= 0 and coalesce(persons_killed, 0) >= 0)`
- [ ] T029 [US3] Run the coverage check from [quickstart.md](./quickstart.md) against the 311-only region (`2026-06-11..2026-07-10`), taking the cutoff from `stg_collisions` rather than the mart's own flags. Expect populated `complaint_count` with `c311_covered = true`, and null `crash_count` with `crashes_covered = false`. A `crash_count` of 0 there is the bug this whole layer exists to prevent

**Checkpoint**: All three stories work. The mart no longer reports "no crashes" on dates NYC
simply has not published.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Idempotency and refresh semantics (FR-007, SC-004), plus the documentation SC-006
depends on (FR-012).

- [ ] T030 Add the internal per-source watermark columns (`_c311_loaded_at_hwm`, `_crashes_loaded_at_hwm`, `_noise_loaded_at_hwm`) to `dbt_project/models/marts/mart_cross_source_daily.sql`, each the source's `max(_loaded_at)` for that key. They are legitimately nullable and carry no `not_null` test
- [ ] T031 Switch `dbt_project/models/marts/mart_cross_source_daily.sql` from `materialized='table'` to `incremental` with `incremental_strategy='delete+insert'` and `unique_key=['activity_date', 'borough']`
- [ ] T032 Add the incremental filter's watermark arm to `dbt_project/models/marts/mart_cross_source_daily.sql`: reprocess keys whose source rows are new since **that source's own** watermark, compared inclusively (`>=`) for tie-safety. Per-source, not shared: a crash batch can land with a `_loaded_at` older than 311's watermark and a shared watermark would silently skip it. Do NOT filter on event-date recency, which never reaches month-old crash dates (FR-007)
- [ ] T033 Add the incremental filter's coverage-transition arm to `dbt_project/models/marts/mart_cross_source_daily.sql`: also reprocess keys in `{{ this }}` that were uncovered for a source whose frontier has since advanced past them. No source row touches those keys, so the watermark arm alone would leave the flags stale forever (FR-007)
- [ ] T034 Verify idempotency per [quickstart.md](./quickstart.md): snapshot the reported columns, rerun the backfill and `dbt build`, and confirm both `EXCEPT` directions return 0. Operational columns are excluded from the comparison (SC-004)
- [ ] T035 Verify late arrival per [quickstart.md](./quickstart.md): insert one new crash record into `raw_collisions.records` with a fresh `collision_id`, an old `crash_date`, and a `_loaded_at` strictly later than the captured watermark, then confirm `dbt build` re-touches that old key and its `crash_count` rises by exactly one (FR-007)
- [ ] T036 Verify coverage transition per [quickstart.md](./quickstart.md): insert a crash record dated past the current frontier, rebuild, and confirm a previously-uncovered earlier key flips to `crash_count = 0` with `crashes_covered = true` rather than staying null
- [ ] T037 [P] Update `README.md` to describe the cross-source model: the three contributing sources, the date-and-borough grain, both derived measures, the coverage flags and why a null count is not a zero, and the runnable command sequence (FR-012). SC-006 requires a first-time reader to find all of that in under 5 minutes without reading model code
- [ ] T038 Update `README.md` and `CLAUDE.md` where they still describe the taxi source or the two-load-schema split in terms of taxi, so no documentation claims a source that no longer exists (FR-011). Not parallel with T037: both edit `README.md`
- [ ] T039 Add the singular test `dbt_project/tests/assert_severity_all_or_null.sql` locking the FR-002 all-or-null severity rule, and list it in [contracts/mart_cross_source_daily.md](./contracts/mart_cross_source_daily.md). Re-derive the expected per-key severity from `stg_collisions` with `case when count(<field>) = count(*) then sum(<field>) end`, join to the mart on the covered keys, and fail on any row where the mart's `persons_injured` or `persons_killed` `is distinct from` the expected value (`is distinct from`, not `<>`, so a NULL mismatch is caught rather than swallowed). Without it, no test in the suite fails if the model regresses from the all-or-null aggregate to a plain `sum()`, which is the exact fabrication the live-API evidence in [data-model.md](./data-model.md) refuted. The test restates the model's `CASE`, so it is partly tautological and does not prove the rule correct: it pins the rule against silent regression, and the API evidence is what establishes the rule. **Must land before T040**, or the gate runs without it
- [ ] T040 Run the full gate: `dbt build --profiles-dir . --project-dir .` from `dbt_project/`, then `pytest tests/dags/ -v` from the repo root. Both must pass with no skipped tests. Runs after T039 so `dbt build` actually executes the new severity test, and after T004 so the per-source `soft_fail` default is in force
- [ ] T041 Confirm the known limitations are still accurately described in [plan.md](./plan.md) Complexity Tracking after implementation: the mutable-grain gap (a revised record leaves a stale contribution on its prior key) and the frontier-versus-last-active-day edge. Correct the text if implementation changed either
- [ ] T042 Re-reconcile the **shipped** model against the full SC-003 sample, after T040 leaves the build green (FR-009, SC-003): at least 10 keys spanning two or more boroughs, including at least one `Unknown`-borough key, at least one key inside the covered overlap region, at least one empty-but-covered key, and at least one key with an uncovered source. **Coverage is a property of a source on a key, never of the key**, so partition the check per source, not per key. For each sampled key and each source independently: where `<source>_covered` is true, that source's mart count must equal its standalone daily aggregate, including 0 on an empty-but-covered day (the flip from the NULL T020 saw is exactly what T026 bought). Where `<source>_covered` is false, the count must be NULL and must **not** be compared against the standalone aggregate, because `count(*)` over an unpublished date returns 0 while the mart correctly returns NULL, so an equality check there fails on correct data. Mixed-coverage keys are the normal case here and must stay in the sample: every date in `2026-06-11..2026-07-10` has 311 covered and crashes uncovered, so the same key is checked by equality on `complaint_count` and by null-assertion on `crash_count`. T020 cannot stand in for this task: it ran against an ungated plain table, so it never exercised the coverage strata SC-003 mandates

---

## Dependencies & Execution Order

```text
Phase 1 (Setup)
   └─> Phase 2 (Foundational)  ← BLOCKS every story
          ├─> Phase 3 (US1, P1)  ← MVP
          │      └─> Phase 4 (US2, P2)   depends on US1's counts
          │      └─> Phase 5 (US3, P3)   depends on US1's spine
          └─> Phase 6 (Polish)  ← needs US1 at minimum
                 T033, T039, T042 need US3's coverage flags (T024 to T026)
                 T039 (severity test) ──> T040 (gate) ──> T042 (reconcile)
```

**Story dependencies**: US2 and US3 both build on US1's mart (they add columns to the same
file), so they are not parallel with US1. US2 and US3 are independent of each other in principle,
but both edit `mart_cross_source_daily.sql`, so run them sequentially to avoid conflicts.

**Within Phase 6**: T033 (coverage-transition arm) requires US3's flags to exist. T037 and T038
are documentation and can run any time after US3, but they run in sequence with each other,
because both edit `README.md`.

The tail of Phase 6 is strictly ordered, and the order is the point. T039 (severity test) needs
T026's coverage flags, since it asserts only over covered keys, and T001's `tests/` directory. It
must land **before** T040, or the gate builds without ever executing it, which is the failure the
test exists to prevent. T042 (re-reconciliation) runs last of all: it must observe the shipped,
coverage-gated model on a green build, so it follows T040.

## Parallel Execution Opportunities

**Phase 2** runs in two waves, because T009, T010, and T011 all call the macro that T005
creates. The macro must exist before anything references it.

Wave A (different files, fully parallel):

```text
T004  dags/factories/source_factory.py
T005  dbt_project/macros/normalize_borough.sql   ← gates wave B
T007  dbt_project/models/staging/_sources.yml
T008  dbt_project/models/staging/stg_taxi_trips.sql (delete)
```

Wave B (after T005 lands, different files, fully parallel):

```text
T009  dbt_project/models/staging/stg_collisions.sql
T010  dbt_project/models/staging/stg_311_complaints.sql
T011  dbt_project/models/staging/stg_noise_complaints.sql
```

T003 (`include/sources.yaml`) and T006 (`dags/nyc_311_dbt.py`) are parallel with both waves.
T012 needs T009 to exist, and T013 and T014 need the whole phase.

**Phase 6**: the documentation tasks T037 and T038 are parallel with the verification tasks T034
to T036, but **not with each other**, because both edit `README.md`. Run T037 then T038.

**Not parallel**: every task touching `dbt_project/models/marts/mart_cross_source_daily.sql`
(T015 to T017, T021, T024 to T026, T030 to T033) is the same file, in sequence.

## Implementation Strategy

**MVP = Phase 1 + Phase 2 + Phase 3 (T001 to T020).** That delivers the P1 story: one daily table
comparing three NYC domains per borough with no manual join, reconciling to its inputs. It is
demonstrable and independently valuable even if nothing else ships.

**Increment 2 = Phase 4 (T021 to T023).** Adds the payoff measures that justify the join.

**Increment 3 = Phase 5 (T024 to T029).** Adds the honesty layer, so the mart stops reporting
"no crashes" on unpublished dates. Worth doing before any consumer sees the table.

**Increment 4 = Phase 6 (T030 to T042).** Adds refresh semantics and the documentation SC-006
needs, pins the all-or-null severity rule at T039, and closes SC-003 against the shipped model at
T042. T031 to T033 are where the ratified complexity actually lands, so expect this phase to
carry the most review weight.
