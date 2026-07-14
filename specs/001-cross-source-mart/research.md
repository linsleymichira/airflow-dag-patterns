# Phase 0 Research: Cross-Source Daily Mart

All items resolved. Data facts were verified against the live Socrata API on 2026-07-13.

## Decision 1: Traffic-crash source configuration

**Decision**: Land NYC Motor Vehicle Collisions (`h9gi-nx95`) through the factory with
`primary_key: collision_id`, `freshness_field: crash_date`, `asset_uri: raw_collisions`,
`schedule: "@daily"`.

**Rationale**: Verified `collision_id` is present and unique across all 2,269,187 rows (clean
natural key). `crash_date` is the event date and spans 2012-07-01 to 2026-06-11 (overlaps the
live 311 range). The dataset carries a native `borough` column.

**Alternatives considered**:

- `freshness_field: :updated_at` (rejected). The factory uses `freshness_field` for the extract
  `$where` bounds as well as the sensor, so `:updated_at` would pull rows by update date and
  staging would bucket them by `crash_date`, diverging the extract window from the mart's
  event-date join key and collapsing co-occurrence with 311. It also does not rescue a forward
  run, because `max(:updated_at)=2026-06-15` is still older than today. `crash_date` is stable,
  so a backfill re-pull upserts revised records on `collision_id` idempotently.

## Decision 2: Replace the taxi source rather than add a fourth

**Decision**: Replace the `nyc_taxi` entry (and its dbt artifacts) with collisions.

**Rationale**: The spec frames crashes as replacing taxi. The taxi source is a dead 2014
snapshot with broken config (`freshness_field` and `primary_key` name columns that do not exist
on `gkne-dk5s`). Replacing it retires that defect. Blast radius verified by grep, exactly five
touchpoints: `include/sources.yaml`, `dags/nyc_311_dbt.py`, `dbt_project/models/staging/_sources.yml`,
`dbt_project/models/staging/_schema.yml`, and `dbt_project/models/staging/stg_taxi_trips.sql`
(deleted). `mart_complaints_daily` does not reference taxi, so nothing downstream breaks.

**Alternatives considered**: Add collisions alongside taxi (rejected). Leaves a known-broken
pipeline in place and adds noise to the factory for no benefit.

## Decision 3: Shared borough normalization macro

**Decision**: Add `macros/normalize_borough.sql` that trims, uppercases, and maps any value
outside the canonical set (BROOKLYN, QUEENS, BRONX, MANHATTAN, STATEN ISLAND) to `Unknown`.
Apply it in all three staging models so join keys align.

**Rationale**: FR-005 requires normalization on every source's borough before joining. Verified
both datasets already emit the same uppercase canonical set. 311 also emits `Unspecified` and
null, crashes emit null, all of which map to `Unknown`. A single macro keeps the rule DRY
(Constitution simplicity) and testable.

## Decision 4: Cross-source join strategy

**Decision**: Build one daily aggregate CTE per source, union their `(activity_date, borough)`
keys into a key spine, then left-join each source aggregate onto the spine. Coalesce missing
counts to 0. Compute derived measures with `nullif(denominator, 0)` so they resolve to null
rather than error.

**Rationale**: The key-spine + left-join pattern satisfies FR-004 (a row exists whenever any
source reported) without dropping keys, and cleanly yields count=0 for non-reporting sources
and null for uncomputable derived measures (FR-006). It is clearer than a chained full outer
join and easy to test.

## Decision 5: Timezone handling (verified)

**Decision**: Derive the calendar date by casting each source's event timestamp to a date, with
no timezone conversion.

**Rationale**: Verified the collisions `crash_date` sample is a floating-local timestamp
(`2014-11-23T00:00:00.000`, no `Z`). The 311 `created_date` that `stg_311_complaints` already
casts is likewise floating-local NYC time. Casting a floating-local timestamp to a date yields
the America/New_York civil day directly, satisfying FR-001. The UTC `:updated_at` field is not
used for the date grain, so no conversion is needed.

## Decision 6: Crash severity metrics

**Decision**: Carry `number_of_persons_injured` and `number_of_persons_killed` (summed per
date and borough) alongside the crash record count. Expose `complaints_per_person_injured`
(`311 complaints / nullif(persons_injured, 0)`) in addition to `complaints_per_crash`.

**Rationale**: Verified both severity columns exist on `h9gi-nx95`. The `/speckit-clarify`
decision chose count plus severity. Summing severity per key mirrors the count aggregation and
inherits the same missing-source and null-denominator handling.

## Decision 7: Mart materialization and late-arrival window

**Decision**: Materialize `mart_cross_source_daily` as incremental `delete+insert` on
`unique_key=['activity_date', 'borough']`, mirroring `mart_complaints_daily`. The incremental
filter reprocesses a trailing window (`activity_date >= max(activity_date) - INTERVAL 3 DAY`)
rather than strictly `>= max`.

**Rationale**: A strict `>= max(activity_date)` filter (as `mart_complaints_daily` uses) would
never re-touch an already-built date, so a late-reported record with an older event date would
never enter the mart. That contradicts the FR-007 / FR-008 distinction between an unchanged
snapshot (zero rows change) and a newer snapshot (refresh affected keys). A short trailing
reprocess window catches late arrivals within a bounded, deterministic range. The tradeoff
(records that arrive later than the window are missed) is documented, matching the demo's
tolerance.

## Decision 8: Run strategy (backfill over the overlap window)

**Decision**: The demo runs via `airflow dags backfill` over roughly `2026-05-01` (the DAG
start date) through `2026-06-10` for both the 311 and collisions pipelines, then a `dbt build`.
Forward `@daily` runs on bleeding-edge dates are expected to reschedule and time out on the
crash sensor until NYC publishes the next monthly batch.

**Rationale**: With `catchup=False`, history is only produced by an explicit backfill. Crash
data lags 311 by ~1 month, so co-occurring rows exist only inside the overlap window. The
strict `>` sensor means the interval whose `data_interval_start` equals `max(crash_date)` does
not fire, so the window ends a day inside the published data (`~2026-06-10`). The timed-out
forward behavior is the freshness sensor (Principle II) working correctly, not a bug.

## Decision 9: dbt DAG subscription and outlet

**Decision**: In `dags/nyc_311_dbt.py`, replace `RAW_TAXI_ASSET` with
`RAW_COLLISIONS_ASSET = Asset("raw_collisions")`, set `schedule=(RAW_311_ASSET | RAW_COLLISIONS_ASSET | RAW_NOISE_ASSET)`,
and add `Asset("mart_cross_source_daily")` to the task outlets alongside the existing mart
asset.

**Rationale**: OR semantics (`|`) fire the transform whenever any source lands (Principle III).
`dbt build --select state:modified+` already rebuilds the new mart once its model file exists,
so no per-model wiring is needed in Airflow.
