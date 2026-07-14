# Quickstart: validate the Cross-Source Daily Mart

End-to-end validation once the implementation tasks are done, proving the mart works on real
data. It is a run guide, not implementation. The models and config live in `dbt_project/`,
`dags/`, and `include/sources.yaml` (see [data-model.md](./data-model.md)).

## Why a backfill, not "unpause and go"

The crash dataset (`h9gi-nx95`) lags 311 by roughly one month (`max(crash_date)` was
`2026-06-11` on 2026-07-13). The DAGs use `catchup=False`, so history is produced only by an
explicit backfill. Co-occurring 311-and-crash rows exist only inside the overlap window. A
forward `@daily` tick on a bleeding-edge date will reschedule and time out on the crash
freshness sensor until NYC publishes the next monthly batch. That is Principle II working
correctly, not a failure.

## Prerequisites

- `astro dev start` running (Airflow + Postgres in Docker).
- Optional `SOCRATA_APP_TOKEN` in `.env` to raise the API rate limit.

## Steps

### 1. Backfill both sources over the overlap window

Run inside the scheduler container. The window ends a day inside the published crash data,
because the freshness sensor is strict `>`.

```bash
docker exec -it $(docker ps -qf name=scheduler) \
  airflow dags backfill nyc_311_pipeline -s 2026-05-01 -e 2026-06-10

docker exec -it $(docker ps -qf name=scheduler) \
  airflow dags backfill nyc_collisions_pipeline -s 2026-05-01 -e 2026-06-10
```

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
- **US3 / SC-005**: a `(activity_date, borough)` with crashes but no complaints (or the reverse)
  still yields a row, with the absent source's counts at 0 and the derived measures null where
  the denominator is 0.

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

Re-run the backfill and `dbt build` for the same window against the same source snapshot. The
mart row count and values are unchanged.
