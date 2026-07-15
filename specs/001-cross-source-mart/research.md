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
keys into a key spine, then left-join each source aggregate onto the spine. Compute derived
measures with `nullif(denominator, 0)` so they resolve to null rather than error.

**Amended 2026-07-14**: this decision originally said "coalesce missing counts to 0"
unconditionally. Decision 7a supersedes that. A missing count coalesces to 0 only where the
source has published the date. Where the source has not published it, the count stays null and
the source is marked uncovered.

**Rationale**: The key-spine + left-join pattern satisfies FR-004 (a row exists whenever any
source reported) without dropping keys, and cleanly separates published-and-empty (0) from
uncovered (null) once the coverage flags gate the coalesce. It is clearer than a chained full
outer join and easy to test.

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

## Decision 7: Mart materialization and late-arrival handling

**Superseded 2026-07-14.** The original decision (incremental `delete+insert` with a trailing
`activity_date >= max(activity_date) - INTERVAL 3 DAY` filter) was withdrawn after the
`/speckit-checklist` pass. It was not merely under-specified, it was structurally wrong for
this feature's data: the mart's `max(activity_date)` is driven by 311, which is fresh to today,
so the window covers roughly the last three days, while crash batches publish `crash_date`
values about a month old. Those rows fall entirely outside the window, so crash data would
never enter the mart on a forward run. The original note conceded that late records "are
missed" without recognizing that for the crash source the miss is total, not marginal.

**Decision**: Materialize `mart_cross_source_daily` as incremental `delete+insert` on
`unique_key=['activity_date', 'borough']`, with the incremental filter driven by **which source
rows are new**, not by event-date recency. Staging models expose `_loaded_at`. The mart
reprocesses the union of two key sets:

1. Keys touched by source rows whose `_loaded_at` is at or past **that source's own** high-water
   mark. Watermarks are per source, not shared: sources publish independently, so a crash batch
   can land with a `_loaded_at` older than 311's watermark, and a single shared watermark would
   silently skip it. The comparison is inclusive for tie-safety, since reprocessing is
   idempotent.
2. Keys whose coverage advanced. When a source publishes a new batch, previously-uncovered dates
   must flip to covered with a true zero. No source row touches those keys, so the watermark
   filter alone would leave the flag stale forever.

**Scope**: this governs the model layer and takes landing as given. The extract requests records
by event date for the interval being run, so a late-published record is landed when its interval
is run or re-run, which is exactly the documented backfill run strategy (Decision 8). The mart
cannot refresh a record the extract never pulled.

**Rationale**: The change signal must match the thing that actually changes. For a source that
publishes month-old event dates in monthly batches, "the row is new to us" is the only correct
trigger, whereas "the event date is recent" is uncorrelated with arrival. Keying on `_loaded_at`
satisfies FR-007's bound (refresh exactly the keys new records touch, with no recency
ceiling) and admits arbitrarily late arrivals. It is also the honest incremental pattern to
demonstrate in a portfolio repo, since the naive calendar window is exactly the trap this
dataset exposes.

**Alternatives considered**:

- **Full refresh (table)** (rejected). Correct by construction and nearly free at demo scale,
  but it sidesteps the interesting problem rather than modeling it.
- **Wider calendar lookback (e.g. 45 days)** (rejected). The same defect at a larger radius:
  still an arbitrary number, still silently misses anything older than the guess.

**Divergence from `mart_complaints_daily`** (intentional): the single-source mart keeps its
simple `>= max(activity_date)` filter, because 311 has no publication lag. Two marts with two
strategies, each matched to its inputs, is a better teaching artifact than two identical ones.

**Recorded as**: ADR `2026-07-14-cross-source-mart-incremental-strategy`.

## Decision 7a: Coverage semantics (added 2026-07-14)

**Decision**: Carry a per-source coverage indicator on every row. A per-source count is `0` only
when that source has published the date and observed no qualifying events. When the source has
not published the date, the count is `NULL` and the source is marked uncovered. Coverage is
derived from each source's maximum published event date (its publication frontier), which the
freshness field already carries.

**Scope of the frontier method**: deriving a frontier from `max(event_date)` is a proxy. It is
only valid for a source that satisfies both conditions below, and FR-004 is narrowed to sources
that do rather than claiming a general mechanism:

1. **Contiguous publication.** A frontier marks every earlier date covered, which holds only if
   the source publishes contiguously up to it. A source with interior holes would mark a skipped
   date covered and report a true zero for a date it never published.
2. **Guaranteed daily events.** The frontier is the last date with a *qualifying event*, not the
   last date *published*. Those coincide only when the source is dense enough that every
   published day has at least one event citywide. Otherwise the most recent published day, if it
   happened to have no events, reads as uncovered.

The three sources qualify: 311 and crashes both occur daily citywide, and noise takes 311's
frontier directly rather than its own (see below), so its zero-event days stay covered.

Landed-interval metadata would establish publication independently of event rows and remove the
proxy entirely. It is not available to dbt here: Airflow owns which intervals ran, and the
DuckDB landing tables record only rows, not attempted intervals. Building a publication manifest
table is the clean fix and is deferred as heavier than this demo warrants. Recording the two
conditions as an explicit scope on FR-004 is the honest alternative, since it names what would
have to be true for coverage to lie.

**Rationale**: FR-004 originally called an absent source's count "zero" without distinguishing
"published and empty" from "not published yet". Because crashes lag 311 by roughly a month,
every recent date would have read `crash_count = 0`, telling a consumer that no crashes
occurred when the truth is that none have been published. The mart would have been least
trustworthy on its freshest rows. Coverage flags also disambiguate a null derived measure,
which otherwise meant both "no crashes" and "no crash data".

**Alternatives considered**: accept and document the lag (rejected: pushes a correctness
problem onto every reader), or restrict the grain to fully-published dates (rejected: weakens
User Story 3 and hides recent 311 activity behind the crash lag).

**Recorded as**: ADR `2026-07-14-cross-source-mart-coverage-flags`.

## Decision 8: Run strategy (backfill over the overlap window)

**Decision**: The demo runs via `airflow dags backfill` from roughly `2026-05-01` (the DAG start
date), then a `dbt build`. The two pipelines get **different end dates**, deliberately:
collisions stop at `~2026-06-10` (a day inside the published crash data), while 311 runs a month
further to `~2026-07-10`. Forward `@daily` runs on bleeding-edge dates are expected to reschedule
and time out on the crash sensor until NYC publishes the next monthly batch.

**Amended 2026-07-14**: this originally backfilled both pipelines to the same `2026-06-10`. That
covers the overlap region but produces no region where a source is legitimately uncovered, so
the coverage semantics (Decision 7a) would be undemonstrable and the quickstart's coverage check
would return zero rows. Running 311 past the crash frontier yields both regions the model needs
to show.

**Rationale**: With `catchup=False`, history is only produced by an explicit backfill. Crash
data lags 311 by ~1 month, so co-occurring rows exist only inside the overlap region. The
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
