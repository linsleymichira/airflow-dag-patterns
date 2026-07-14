# Contract: `mart_cross_source_daily`

The consumption interface a BI or dashboard layer reads. The contract fixes the columns,
types, nullability, and the tests that enforce them.

## Grain

One row per `(activity_date, borough)`. Uniqueness is enforced by
`dbt_utils.unique_combination_of_columns(['activity_date', 'borough'])`.

## Columns

|Column|Type|Nullable|Contract|
|---|---|---|---|
|`activity_date`|DATE|no|Shared calendar date, America/New_York civil day|
|`borough`|VARCHAR|no|One of BROOKLYN, QUEENS, BRONX, MANHATTAN, STATEN ISLAND, Unknown|
|`complaint_count`|BIGINT|no|>= 0. Daily 311 complaints, 0 when the source did not report|
|`crash_count`|BIGINT|no|>= 0. Daily crash records, 0 when the source did not report|
|`persons_injured`|BIGINT|no|>= 0. Daily crash persons injured|
|`persons_killed`|BIGINT|no|>= 0. Daily crash persons killed|
|`noise_count`|BIGINT|no|>= 0. Daily noise complaints, 0 when the source did not report|
|`complaints_per_crash`|DOUBLE|yes|`complaint_count / crash_count`. Null when `crash_count = 0`|
|`complaints_per_person_injured`|DOUBLE|yes|`complaint_count / persons_injured`. Null when `persons_injured = 0`|

## Guarantees

- **Completeness (FR-004, SC-002)**: a row exists for every `(activity_date, borough)` where any
  source reported activity. No key is dropped.
- **Deterministic missing semantics (FR-004, FR-006)**: absent-source counts are 0. Derived
  measures with a zero denominator are null, never an error.
- **Reconciliation (SC-003)**: each per-source count equals that source's standalone daily
  aggregate for the same key.
- **Idempotent rebuild (FR-007, SC-004)**: rebuilding against an unchanged source snapshot
  changes zero rows. A newer snapshot refreshes only affected keys within the trailing window.
- **Borough domain (FR-005)**: `borough` is always one of the six canonical values.

## Enforcing tests (dbt)

- `not_null` on `activity_date`, `borough`, `complaint_count`, `crash_count`, `noise_count`.
- `dbt_utils.unique_combination_of_columns(['activity_date', 'borough'])`.
- `accepted_values` on `borough` = the six canonical values.
