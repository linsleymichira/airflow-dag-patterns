# Quickstart: validate the Cross-Source Daily Mart

End-to-end validation once the implementation tasks are done, proving the mart works on real
data. It is a run guide, not implementation. The models and config live in `dbt_project/`,
`dags/`, and `include/sources.yaml` (see [data-model.md](./data-model.md)).

## Why a backfill, not "unpause and go"

The crash dataset (`h9gi-nx95`) lags 311 by roughly one month (`max(crash_date)` was
`2026-06-11` on 2026-07-13). The DAGs use `catchup=False`, so history is produced only by an
explicit backfill. Co-occurring 311-and-crash rows exist only inside the overlap region. A
forward `@daily` tick on a bleeding-edge date will reschedule and time out on the crash
freshness sensor until NYC publishes the next monthly batch. That is Principle II working
correctly, not a failure.

## Prerequisites

- `astro dev start` running (Airflow + Postgres in Docker).
- Optional `SOCRATA_APP_TOKEN` in `.env` to raise the API rate limit.

## Steps

### 1. Backfill the sources

Run inside the scheduler container. The two windows are deliberately different lengths:

- **Collisions** stop a day inside the published crash data (`max(crash_date)` was `2026-06-11`
  on 2026-07-13), because the freshness sensor is strict `>`.
- **311** runs a month further, past the crash frontier. That produces both regions the model
  needs to demonstrate: an overlap region where the cross-source measures are non-null, and a
  311-only region where crashes are legitimately uncovered. Without the second region the
  coverage check below has nothing to return.

```bash
docker exec -it $(docker ps -qf name=scheduler) \
  airflow dags backfill nyc_311_pipeline -s 2026-05-01 -e 2026-07-10

docker exec -it $(docker ps -qf name=scheduler) \
  airflow dags backfill nyc_collisions_pipeline -s 2026-05-01 -e 2026-06-10
```

Resulting regions: `2026-05-01` to `2026-06-10` has both sources covered, and `2026-06-11` to
`2026-07-10` has 311 covered with crashes uncovered.

Each successful load emits its Asset (`raw_311`, `raw_collisions`), which triggers `nyc_311_dbt`.

### 2. Build the models

`nyc_311_dbt` runs automatically on the Asset events. To build on demand:

```bash
docker exec -it $(docker ps -qf name=scheduler) bash -lc \
  "cd /usr/local/airflow/dbt_project && dbt build --profiles-dir . --project-dir ."
```

### 3. Inspect the mart

```bash
docker exec -it $(docker ps -qf name=scheduler) \
  duckdb /usr/local/airflow/include/nyc_311.duckdb
```

```sql
select * from mart_cross_source_daily
where borough <> 'Unknown'
order by activity_date desc, borough
limit 20;
```

## Expected outcomes (maps to Success Criteria)

- **SC-001 / SC-002**: rows exist for each `(activity_date, borough)` inside the window where any
  source reported. Boroughs are the canonical five plus `Unknown`.
- **US1**: within the overlap window, `complaint_count` and `crash_count` are both populated on
  the same row, so 311 and crashes are compared without a manual join.
- **US2**: `complaints_per_crash` and `complaints_per_person_injured` are populated where the
  denominator is non-zero.
- **US3 / SC-005**: a `(activity_date, borough)` where one source published and saw nothing still
  yields a row, with that source's count at 0, its `*_covered` flag true, and any derived measure
  it denominates null.

Coverage check (FR-004), on a date newer than the crash publication frontier. The cutoff is
taken from the upstream source, not from the mart's own flags, so the check cannot pass by
agreeing with itself:

```sql
with frontier as (
  select max(crash_date) as crashes_max_date from stg_collisions
)
select m.activity_date, m.borough, m.complaint_count, m.c311_covered,
       m.crash_count, m.crashes_covered
from mart_cross_source_daily m, frontier f
where m.activity_date > f.crashes_max_date
order by m.activity_date
limit 5;
-- Expect rows from the 311-only region (2026-06-11 to 2026-07-10), each with
-- complaint_count populated and c311_covered = true, crash_count NULL and
-- crashes_covered = false.
-- Zero rows returned means the check did not actually run: confirm the 311 backfill in
-- step 1 extended past the crash frontier.
-- A crash_count of 0 here would be the bug this distinction exists to prevent.
```

Reconciliation check (SC-003), for a sampled borough and date:

```sql
select count(*) from stg_311_complaints
where complaint_date = date '2026-06-01' and borough = 'BROOKLYN';
-- must equal complaint_count for the same key in mart_cross_source_daily
```

### 4. Run the tests

```bash
# dbt data tests (uniqueness, not_null, accepted borough values, reconciliation)
docker exec -it $(docker ps -qf name=scheduler) bash -lc \
  "cd /usr/local/airflow/dbt_project && dbt test --profiles-dir . --project-dir ."

# DAG import + factory-count contract (nyc_collisions_pipeline registers, count stays 4)
pytest tests/dags/ -v
```

## Idempotency check (FR-007, SC-004)

Re-run the backfill and `dbt build` for the same window against the same source snapshot. Every
existing row is value-identical afterwards across its reported columns:

```sql
-- Snapshot before the rerun. Operational columns are excluded, because refreshing
-- _loaded_at and the watermarks is how late-arrival tracking works (SC-004).
drop table if exists mart_before;
create table mart_before as
select activity_date, borough, complaint_count, c311_covered, crash_count, persons_injured,
       persons_killed, crashes_covered, noise_count, noise_covered,
       complaints_per_crash, complaints_per_person_injured
from mart_cross_source_daily;
```

Rerun the backfill and `dbt build`, then compare in both directions. One direction alone would
miss rows the rebuild added:

```sql
with after as (
  select activity_date, borough, complaint_count, c311_covered, crash_count, persons_injured,
         persons_killed, crashes_covered, noise_count, noise_covered,
         complaints_per_crash, complaints_per_person_injured
  from mart_cross_source_daily
)
select
  (select count(*) from (select * from mart_before except select * from after))  as lost_or_changed,
  (select count(*) from (select * from after except select * from mart_before))  as added_or_changed;
-- Both must be 0.
```

## Late-arrival check (FR-007)

The point of the change-driven incremental filter is that a genuinely new record with an old
event date still reaches the mart. Re-running an interval only refreshes `_loaded_at` on rows
the mart already counted, which does not test the thing that matters. To test it properly:

1. Note the current `crash_count` for a built key, for example Brooklyn on an early date in the
   window, and capture the current crash watermark
   (`select max(_crashes_loaded_at_hwm) from mart_cross_source_daily`).
2. Insert one new crash record into `raw_collisions.records` carrying a fresh `collision_id`, a
   `crash_date` on that old key's date, `borough = 'BROOKLYN'`, and a `_loaded_at` strictly
   later than the watermark captured in step 1 (matching the column's timestamp type, so the
   comparison is unambiguous rather than a tie). That simulates NYC publishing a month-old crash
   today.
3. Re-run `dbt build`.
4. Confirm the mart re-touched that old `activity_date` and that its `crash_count` increased by
   exactly one, with the other keys unchanged.

A calendar-window filter anchored on the freshest source would skip the key entirely, which is
the defect this design exists to avoid.

## Coverage-transition check (FR-007)

A newly-published batch must flip previously-uncovered dates to covered with a true zero, even
though no source row touches those keys.

1. Find a date past the crash frontier that has 311 activity, so the row exists with
   `crash_count IS NULL` and `crashes_covered = false`.
2. Insert one crash record dated later than that date (a new frontier), in any borough.
3. Re-run `dbt build`.
4. Confirm the earlier key now reads `crash_count = 0` with `crashes_covered = true`, rather
   than remaining null. A stale flag here means the incremental filter is missing its
   coverage-transition arm.
