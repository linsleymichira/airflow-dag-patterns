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
|`persons_injured`|BIGINT|Daily crash persons injured. 0 when covered with no crashes, NULL when uncovered or when every crash row omits the field|
|`persons_killed`|BIGINT|Daily crash persons killed. Same null semantics as `persons_injured`|
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
-- Noise deliberately reuses 311's frontier: noise IS 311 filtered to noise complaint types
-- (same dataset erm2-nwe9), so its publication frontier is 311's. Deriving it from
-- max(noise complaint_date) would read a citywide zero-noise day as unpublished, which is
-- the exact conflation these flags exist to prevent.
with coverage as (
  select
    (select max(complaint_date) from {{ ref('stg_311_complaints') }}) as c311_max_date,
    (select max(crash_date)     from {{ ref('stg_collisions') }})     as crashes_max_date,
    (select max(complaint_date) from {{ ref('stg_311_complaints') }}) as noise_max_date
),
c311 as (
  select complaint_date as activity_date, borough,
         count(*) as complaint_count, max(_loaded_at) as loaded_at
  from {{ ref('stg_311_complaints') }} group by 1, 2
),
crashes as (
  select crash_date as activity_date, borough,
         count(*) as crash_count,
         -- All-or-null, NOT sum(): a missing severity field means UNREPORTED (verified below).
         -- count(col) counts non-nulls, so the sum survives only when every crash row that day
         -- reports the field. A plain sum() would silently skip the unreported rows and present
         -- a partial total as the day's fact, which is the same fabrication as coalescing to 0.
         case when count(persons_injured) = count(*)
              then sum(persons_injured) end as persons_injured,
         case when count(persons_killed) = count(*)
              then sum(persons_killed) end  as persons_killed,
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
    -- Severity: 0 only when the day genuinely had no crashes (the left join missed).
    -- When crashes exist, pass the sum through: null there means "reported nothing", and
    -- coalescing it to 0 would invent an injury-free day out of missing data.
    case when coalesce(s.activity_date <= cov.crashes_max_date, false)
         then case when crashes.crash_count is null then 0
                   else crashes.persons_injured end end      as persons_injured,
    case when coalesce(s.activity_date <= cov.crashes_max_date, false)
         then case when crashes.crash_count is null then 0
                   else crashes.persons_killed end end       as persons_killed,
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

The frontier is still derived from `max(event_date)` rather than from landed-interval metadata,
which equates "the source's last day with any activity" to "the source's publication frontier".
Those differ only when the frontier day itself had zero qualifying events citywide. Crashes and
311 both occur daily citywide, so the two coincide. Noise was the real exposure and is fixed
above by reusing 311's frontier. A publication manifest keyed on which intervals actually ran
would remove the residual edge, and is deferred as heavier than the demo needs.

**Severity nulls mean UNREPORTED, not zero** (verified against the live `h9gi-nx95` API on
2026-07-14). The evidence is decisive:

- The source reports zero explicitly. Of the 903 rows in the demo window
  (`crash_date >= 2026-05-01`) that carry `number_of_persons_killed`, **895 are `0`** and 8 are
  above zero. January 2020 alone has 14,349 explicit zeros and **no** nulls. A missing field is
  therefore not a sparse encoding of zero, because zero has its own representation.
- A sampled omitting row (`collision_id` 4387369, 2021) carries `number_of_cyclist_injured: 1`,
  so a person was injured while `number_of_persons_injured` is simply absent.

The two fields behave very differently, which matters for what the mart can promise:

|Field|Nulls overall|Nulls in the demo window|Reading|
|---|---|---|---|
|`number_of_persons_injured`|18 of 2,269,187, all 2016 to 2021|**0**|Effectively always reported. Within the demo window the injury metric and `complaints_per_person_injured` are unaffected. Outside it, the 18 historical omissions null their own borough-days by the same all-or-null rule|
|`number_of_persons_killed`|8,913, up to the frontier|**8,832 of 9,735 rows (90.7%)**|Recent fatality determinations are pending. NYC finalizes them later|

Consequently severity is aggregated **all-or-null**, not summed across nulls: a day's total
survives only when every crash row that day reports the field. A partial sum would present the
known 9% as the day's fatality count and understate it as fact. Recent borough-days will
therefore carry a null `persons_killed`, which is the honest answer, and this independently
vindicates the FR-002 decision not to derive any measure from persons-killed. A covered day with
no crashes at all still reads 0, because that zero is a fact rather than an absence.

**Known limitation, mutable grain** (accepted, not designed around): the incremental filter
reprocesses keys reachable from *current* source rows. If NYC revises a record so its
`crash_date` or `borough` changes (for example a previously-ungeocoded crash gains a borough,
moving it out of the `Unknown` bucket), the landing table's `INSERT OR REPLACE` on
`collision_id` overwrites the row in place, so the record's *prior* key becomes unreachable and
its stale contribution is never recomputed. The old key keeps a count that is too high. Fixing
this properly needs either an immutable-grain guarantee from the source (which NYC does not
give) or prior-key capture via a snapshot. It is the sharpest cost of the ratified incremental
design: a full refresh would not have this failure mode. Recorded as an assumption in the spec
and as an entry in the plan's Complexity Tracking.

Tests (marts `_schema.yml`):

- `not_null` on `activity_date`, `borough`, and the three `*_covered` flags.
- `dbt_utils.unique_combination_of_columns(['activity_date', 'borough'])`.
- `dbt_utils.expression_is_true` per source count, asserting a count is null if and only if its
  source is uncovered, for example `(crash_count is null) = (not crashes_covered)`.
- `dbt_utils.expression_is_true` per covered measure, asserting non-negativity where the flag is
  true and the value is present, for example `not crashes_covered or (crash_count >= 0 and
  coalesce(persons_injured, 0) >= 0 and coalesce(persons_killed, 0) >= 0)`. Severity is null-
  tolerant even when covered, because an omitted source field means unreported.
- `dbt_utils.expression_is_true` per derived measure, asserting it is null exactly when its
  numerator is unavailable or its denominator is unusable, and a real ratio otherwise. The
  numerator arm is load-bearing: a null `complaint_count` (311 uncovered) yields a null ratio
  even where the denominator is fine, so omitting it would fail the test on correct data.
  - `complaints_per_crash is null = (complaint_count is null or crash_count is null or crash_count = 0)`
  - `complaints_per_person_injured is null = (complaint_count is null or persons_injured is null or persons_injured = 0)`

Counts are deliberately **not** `not_null`, because a null count is the meaningful uncovered
signal. Severity columns are not `not_null` either, because a null there means the source did
not report it. The per-source watermark columns are also not `not_null`, because a key that only
one source reports legitimately has null watermarks for the others.

## Entity summary (from the spec)

- **Cross-Source Daily Record**: one `(activity_date, borough)` observation carrying the five
  per-source metrics and the two derived measures. Maps to one mart row.
- **Borough**: shared spatial dimension, canonical five plus `Unknown`. Enforced by
  `normalize_borough`.
- **Source Contribution**: each per-source daily aggregate CTE, and the unit reconciled against
  the source's standalone daily aggregate (SC-003).
