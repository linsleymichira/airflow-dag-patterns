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
|`_loaded_at`|`_loaded_at`|TIMESTAMP|

Tests: `collision_id` not_null + unique, `crash_date` not_null, `_loaded_at` not_null (the
mart's incremental filter depends on it always being present).

### `stg_311_complaints` and `stg_noise_complaints` (updated)

Two changes each:

1. Replace the current `nullif(trim(borough), '')` with `{{ normalize_borough('borough') }}`
   (311, which reads a typed column) and `{{ normalize_borough(raw ->> 'borough') }}` (noise,
   which reads the JSON payload), so the three sources share an identical borough domain.
2. Project `_loaded_at` through, which the mart's change-driven incremental filter reads
   (Decision 7). The staging layer previously exposed only typed event fields, so this is a new
   obligation on staging, not a passthrough that already existed.

All other columns unchanged. Both models gain a `_loaded_at` not_null test, matching
`stg_collisions`, so the mart's incremental filter always receives a valid timestamp.

## Mart: `mart_cross_source_daily` (new)

**Grain**: one row per `activity_date` (calendar date) and `borough`.

**Materialization**: `incremental`, `incremental_strategy='delete+insert'`,
`unique_key=['activity_date', 'borough']`. The incremental filter is driven by **which source
rows are new**, not by event-date recency: reprocess the `(activity_date, borough)` keys touched
by source rows whose `_loaded_at` exceeds the previous build's high-water mark (Decision 7 in
research.md, superseding the withdrawn trailing-window filter).

**Coverage**: a per-source count is `0` only when that source has published the date. When it
has not, the count is `NULL` and the source's `*_covered` flag is false (Decision 7a, FR-004).

|Column|Type|Definition|
|---|---|---|
|`activity_date`|DATE|Shared calendar date (ET civil day)|
|`borough`|VARCHAR|Canonical borough or `Unknown`|
|`complaint_count`|BIGINT|Daily 311 complaints. 0 when published-and-empty, NULL when uncovered|
|`c311_covered`|BOOLEAN|Whether 311 has published `activity_date`|
|`crash_count`|BIGINT|Daily crash records. 0 when published-and-empty, NULL when uncovered|
|`persons_injured`|BIGINT|Daily crash persons injured. NULL when uncovered|
|`persons_killed`|BIGINT|Daily crash persons killed. NULL when uncovered|
|`crashes_covered`|BOOLEAN|Whether the crash source has published `activity_date`|
|`noise_count`|BIGINT|Daily noise complaints. 0 when published-and-empty, NULL when uncovered|
|`noise_covered`|BOOLEAN|Whether the noise source has published `activity_date`|
|`complaints_per_crash`|DOUBLE|`complaint_count / nullif(crash_count, 0)`. Null when crash_count is 0 or uncovered|
|`complaints_per_person_injured`|DOUBLE|`complaint_count / nullif(persons_injured, 0)`. Null when persons_injured is 0 or uncovered|

Reading the pair together is what makes a null interpretable: `crash_count IS NULL AND NOT
crashes_covered` means "not published yet", while `crash_count = 0 AND crashes_covered` means
"published, no crashes". Never read a null count as zero activity.

Plus three internal columns, not part of the consumer contract. Watermarks are **per source**,
not shared: sources publish independently, so a crash batch can land with a `_loaded_at` older
than 311's watermark, and a single shared watermark would silently skip it.

|Column|Type|Definition|
|---|---|---|
|`_c311_loaded_at_hwm`|TIMESTAMP|Max `_loaded_at` of the 311 rows feeding this key. Null when 311 has no rows for it|
|`_crashes_loaded_at_hwm`|TIMESTAMP|Max `_loaded_at` of the crash rows feeding this key. Null when crashes have none|
|`_noise_loaded_at_hwm`|TIMESTAMP|Max `_loaded_at` of the noise rows feeding this key. Null when noise has none|

These are legitimately nullable (a key present only in 311 has no crash watermark), so they
carry no `not_null` test.

**Shape sketch** (key-spine + left joins, per Decision 4, with coverage and change-driven
reprocessing per Decisions 7 and 7a):

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['activity_date', 'borough']
) }}

-- How far each source has actually published. Drives the covered/uncovered distinction.
with coverage as (
  select
    (select max(complaint_date) from {{ ref('stg_311_complaints') }})   as c311_max_date,
    (select max(crash_date)     from {{ ref('stg_collisions') }})       as crashes_max_date,
    (select max(complaint_date) from {{ ref('stg_noise_complaints') }}) as noise_max_date
),
c311 as (
  select complaint_date as activity_date, borough,
         count(*) as complaint_count, max(_loaded_at) as loaded_at
  from {{ ref('stg_311_complaints') }} group by 1, 2
),
crashes as (
  select crash_date as activity_date, borough,
         count(*) as crash_count,
         -- A missing severity value counts as 0 rather than nulling the whole key's sum.
         sum(coalesce(persons_injured, 0)) as persons_injured,
         sum(coalesce(persons_killed, 0))  as persons_killed,
         max(_loaded_at) as loaded_at
  from {{ ref('stg_collisions') }} group by 1, 2
),
noise as (
  select complaint_date as activity_date, borough,
         count(*) as noise_count, max(_loaded_at) as loaded_at
  from {{ ref('stg_noise_complaints') }} group by 1, 2
),
spine as (
  select activity_date, borough from c311
  union select activity_date, borough from crashes
  union select activity_date, borough from noise
),
combined as (
  select
    s.activity_date,
    s.borough,
    -- coalesce(..., false): an empty source leaves max_date null, which must read as
    -- uncovered rather than as an unknown three-valued result.
    coalesce(s.activity_date <= cov.c311_max_date, false)    as c311_covered,
    coalesce(s.activity_date <= cov.crashes_max_date, false) as crashes_covered,
    coalesce(s.activity_date <= cov.noise_max_date, false)   as noise_covered,
    -- Zero only where the source has published. Null (uncovered) otherwise.
    case when coalesce(s.activity_date <= cov.c311_max_date, false)
         then coalesce(c311.complaint_count, 0) end          as complaint_count,
    case when coalesce(s.activity_date <= cov.crashes_max_date, false)
         then coalesce(crashes.crash_count, 0) end           as crash_count,
    case when coalesce(s.activity_date <= cov.crashes_max_date, false)
         then coalesce(crashes.persons_injured, 0) end       as persons_injured,
    case when coalesce(s.activity_date <= cov.crashes_max_date, false)
         then coalesce(crashes.persons_killed, 0) end        as persons_killed,
    case when coalesce(s.activity_date <= cov.noise_max_date, false)
         then coalesce(noise.noise_count, 0) end             as noise_count,
    c311.loaded_at    as _c311_loaded_at_hwm,
    crashes.loaded_at as _crashes_loaded_at_hwm,
    noise.loaded_at   as _noise_loaded_at_hwm
  from spine s
  cross join coverage cov
  left join c311    using (activity_date, borough)
  left join crashes using (activity_date, borough)
  left join noise   using (activity_date, borough)
)
select
  *,
  -- Denominators and numerator are the coverage-normalized values, so an uncovered source
  -- yields a null measure rather than a ratio over a raw aggregate.
  complaint_count::double / nullif(crash_count, 0)     as complaints_per_crash,
  complaint_count::double / nullif(persons_injured, 0) as complaints_per_person_injured
from combined
{% if is_incremental() %}
where
  -- (a) Keys touched by source rows new since THAT SOURCE's own watermark. Per-source, because
  --     sources publish independently: a crash batch can land with a _loaded_at older than
  --     311's watermark, and a shared watermark would skip it. Event-date recency is
  --     deliberately not a filter, since crash batches carry month-old crash_date values.
  --     Comparison is inclusive (>=) for tie-safety, relying on idempotent reprocessing.
  _c311_loaded_at_hwm >= (select coalesce(max(_c311_loaded_at_hwm), '1900-01-01') from {{ this }})
  or _crashes_loaded_at_hwm >= (select coalesce(max(_crashes_loaded_at_hwm), '1900-01-01') from {{ this }})
  or _noise_loaded_at_hwm >= (select coalesce(max(_noise_loaded_at_hwm), '1900-01-01') from {{ this }})
  -- (b) Keys whose coverage advanced. A newly-published batch flips previously-uncovered dates
  --     to covered with a true 0, and no source row touches those keys, so (a) alone would
  --     leave the flag stale forever.
  or (activity_date, borough) in (
       select activity_date, borough from {{ this }} prev
       where (not prev.c311_covered
              and prev.activity_date <= (select max(complaint_date) from {{ ref('stg_311_complaints') }}))
          or (not prev.crashes_covered
              and prev.activity_date <= (select max(crash_date) from {{ ref('stg_collisions') }}))
          or (not prev.noise_covered
              and prev.activity_date <= (select max(complaint_date) from {{ ref('stg_noise_complaints') }}))
     )
{% endif %}
```

**Coverage relies on a contiguity assumption**: a source's frontier (`max` event date) marks
every earlier date covered. That is correct only if the source publishes contiguously up to it.
The NYC feeds do. A source with interior publication holes would mark a skipped date covered,
reporting a true zero for a date it never published. If that ever occurs, coverage must move
from a single frontier to an explicit per-date publication manifest. Recorded as an assumption
in the spec.

**Severity nulls** are summed as zero (`sum(coalesce(persons_injured, 0))`), so a covered key
whose crash rows all omit severity reads as 0 injuries rather than null. [Assumption] Verify
during implementation that `h9gi-nx95` omits these fields only when the true value is zero. If
it omits them for unreported severity, this conflates "no injuries" with "severity unknown" and
needs the same covered/uncovered treatment the counts get.

Tests (marts `_schema.yml`):

- `not_null` on `activity_date`, `borough`, and the three `*_covered` flags.
- `dbt_utils.unique_combination_of_columns(['activity_date', 'borough'])`.
- `dbt_utils.expression_is_true` per source, asserting a count is null if and only if its source
  is uncovered, for example `(crash_count is null) = (not crashes_covered)`.
- `dbt_utils.expression_is_true` per covered measure, asserting non-negativity where the flag is
  true, for example `not crashes_covered or (crash_count >= 0 and persons_injured >= 0 and
  persons_killed >= 0)`.

Counts are deliberately **not** `not_null`, because a null count is the meaningful uncovered
signal. The per-source watermark columns are also not `not_null`, because a key that only one
source reports legitimately has null watermarks for the others.

## Entity summary (from the spec)

- **Cross-Source Daily Record**: one `(activity_date, borough)` observation carrying the five
  per-source metrics and the two derived measures. Maps to one mart row.
- **Borough**: shared spatial dimension, canonical five plus `Unknown`. Enforced by
  `normalize_borough`.
- **Source Contribution**: each per-source daily aggregate CTE, and the unit reconciled against
  the source's standalone daily aggregate (SC-003).
