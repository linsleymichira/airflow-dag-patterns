# Phase 1 Data Model: Cross-Source Daily Mart

Design of the landed source, staging views, shared macro, and the mart. Final SQL belongs to
the implementation phase. The sketches below fix the shapes, keys, and validation rules.

## Landed source: `raw_collisions.records`

Produced by the factory (generic landing shape, identical to `raw_taxi`/`raw_noise`).

|Column|Type|Notes|
|---|---|---|
|`collision_id`|VARCHAR (PK)|Natural key, verified unique across all rows|
|`freshness_at`|TIMESTAMP|Populated from `crash_date` by the factory loader|
|`raw`|JSON|Full Socrata payload, typed fields extracted in staging|
|`_loaded_at`|TIMESTAMP|Factory default|

`include/sources.yaml` entry (replaces `nyc_taxi`):

```yaml
- name: nyc_collisions
  socrata_id: h9gi-nx95
  primary_key: collision_id
  freshness_field: crash_date
  asset_uri: raw_collisions
  schedule: "@daily"
```

## Shared macro: `normalize_borough(column)`

Canonical borough normalization applied in every staging model so join keys align.

```sql
{% macro normalize_borough(column) %}
  case
    when upper(trim({{ column }})) in
      ('BROOKLYN', 'QUEENS', 'BRONX', 'MANHATTAN', 'STATEN ISLAND')
    then upper(trim({{ column }}))
    else 'Unknown'
  end
{% endmacro %}
```

Maps blank, null, `Unspecified`, and any out-of-set value to `Unknown` (FR-005).

## Staging views

### `stg_collisions` (new)

View over `raw_collisions.records`, typed from the JSON payload, mirroring the taxi/noise
staging pattern.

|Column|Source expression|Type|
|---|---|---|
|`collision_id`|`collision_id`|VARCHAR|
|`crash_at`|`try_cast(raw ->> 'crash_date' as timestamp)`|TIMESTAMP|
|`borough`|`normalize_borough(raw ->> 'borough')`|VARCHAR|
|`persons_injured`|`try_cast(raw ->> 'number_of_persons_injured' as integer)`|INTEGER|
|`persons_killed`|`try_cast(raw ->> 'number_of_persons_killed' as integer)`|INTEGER|
|`crash_date`|`cast(try_cast(raw ->> 'crash_date' as timestamp) as date)`|DATE|

Tests: `collision_id` not_null + unique, `crash_date` not_null.

### `stg_311_complaints` and `stg_noise_complaints` (updated)

One-line change each: replace the current `nullif(trim(borough), '')` with
`{{ normalize_borough('borough') }}` (311, which reads a typed column) and
`{{ normalize_borough(raw ->> 'borough') }}` (noise, which reads the JSON payload). All other
columns unchanged, which guarantees the three sources share an identical borough domain.

## Mart: `mart_cross_source_daily` (new)

**Grain**: one row per `activity_date` (calendar date) and `borough`.

**Materialization**: `incremental`, `incremental_strategy='delete+insert'`,
`unique_key=['activity_date', 'borough']`. Incremental filter reprocesses a trailing window
(`activity_date >= (select max(activity_date) from {{ this }}) - INTERVAL 3 DAY`) so late
arrivals within the window re-enter (Decision 7 in research.md).

|Column|Type|Definition|
|---|---|---|
|`activity_date`|DATE|Shared calendar date (ET civil day)|
|`borough`|VARCHAR|Canonical borough or `Unknown`|
|`complaint_count`|BIGINT|Daily 311 complaints (0 when none)|
|`crash_count`|BIGINT|Daily crash records (0 when none)|
|`persons_injured`|BIGINT|Daily crash persons injured (0 when none)|
|`persons_killed`|BIGINT|Daily crash persons killed (0 when none)|
|`noise_count`|BIGINT|Daily noise complaints (0 when none)|
|`complaints_per_crash`|DOUBLE|`complaint_count / nullif(crash_count, 0)` (null when no crashes)|
|`complaints_per_person_injured`|DOUBLE|`complaint_count / nullif(persons_injured, 0)` (null when no injuries)|

**Shape sketch** (key-spine + left joins, per Decision 4):

```sql
with c311 as (
  select complaint_date as activity_date, borough, count(*) as complaint_count
  from {{ ref('stg_311_complaints') }} group by 1, 2
),
crashes as (
  select crash_date as activity_date, borough,
         count(*) as crash_count,
         sum(persons_injured) as persons_injured,
         sum(persons_killed) as persons_killed
  from {{ ref('stg_collisions') }} group by 1, 2
),
noise as (
  select complaint_date as activity_date, borough, count(*) as noise_count
  from {{ ref('stg_noise_complaints') }} group by 1, 2
),
spine as (
  select activity_date, borough from c311
  union select activity_date, borough from crashes
  union select activity_date, borough from noise
)
select
  s.activity_date, s.borough,
  coalesce(c311.complaint_count, 0)   as complaint_count,
  coalesce(crashes.crash_count, 0)    as crash_count,
  coalesce(crashes.persons_injured, 0) as persons_injured,
  coalesce(crashes.persons_killed, 0)  as persons_killed,
  coalesce(noise.noise_count, 0)      as noise_count,
  c311.complaint_count::double / nullif(crashes.crash_count, 0)     as complaints_per_crash,
  c311.complaint_count::double / nullif(crashes.persons_injured, 0) as complaints_per_person_injured
from spine s
left join c311    using (activity_date, borough)
left join crashes using (activity_date, borough)
left join noise   using (activity_date, borough)
```

Tests (marts `_schema.yml`): `activity_date` not_null, `borough` not_null, `complaint_count`
not_null, and `dbt_utils.unique_combination_of_columns(['activity_date', 'borough'])`.

## Entity summary (from the spec)

- **Cross-Source Daily Record**: one `(activity_date, borough)` observation carrying the five
  per-source metrics and the two derived measures. Maps to one mart row.
- **Borough**: shared spatial dimension, canonical five plus `Unknown`. Enforced by
  `normalize_borough`.
- **Source Contribution**: each per-source daily aggregate CTE, and the unit reconciled against
  the source's standalone daily aggregate (SC-003).
