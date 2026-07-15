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
|`complaint_count`|BIGINT|yes|>= 0 when `c311_covered`. Null when uncovered|
|`c311_covered`|BOOLEAN|no|Whether 311 has published `activity_date`|
|`crash_count`|BIGINT|yes|>= 0 when `crashes_covered`. Null when uncovered|
|`persons_injured`|BIGINT|yes|>= 0 when covered. Null when uncovered, or when **any** crash row for that same date and borough omits the field (unreported, not zero). A total survives only when every crash row on its own key reports it, so an omission in one borough never nulls another|
|`persons_killed`|BIGINT|yes|Same null semantics as `persons_injured`|
|`crashes_covered`|BOOLEAN|no|Whether the crash source has published `activity_date`|
|`noise_count`|BIGINT|yes|>= 0 when `noise_covered`. Null when uncovered|
|`noise_covered`|BOOLEAN|no|Whether the noise source has published `activity_date`|
|`complaints_per_crash`|DOUBLE|yes|`complaint_count / crash_count`. Null when the numerator is null (311 uncovered), or `crash_count` is 0 or null|
|`complaints_per_person_injured`|DOUBLE|yes|`complaint_count / persons_injured`. Null when the numerator is null, or `persons_injured` is 0 or null (uncovered, or severity unreported)|

## Reading the coverage flags (required)

A null count does **not** mean zero activity. Always read a count with its flag:

|State|Meaning|
|---|---|
|`crash_count = 0` and `crashes_covered`|The source published this date and saw no crashes|
|`crash_count IS NULL` and not `crashes_covered`|The source has not published this date yet|

Traffic crashes lag 311 by roughly one month, so recent dates are routinely uncovered for
crashes while fully covered for 311. Treating a null as zero would report "no crashes" on every
recent day.

## Guarantees

- **Completeness (FR-004, SC-002)**: a row exists for every `(activity_date, borough)` where any
  source reported activity. No key is dropped.
- **Deterministic missing semantics (FR-004, FR-006)**: a count is 0 only for
  published-and-empty, and null only for uncovered. Derived measures with a zero, missing, or
  uncovered denominator are null, never an error.
- **Reconciliation (FR-009, SC-003)**: each per-source count equals that source's standalone
  daily aggregate for the same key. Verified by sampling keys, not by an exhaustive automated
  comparison.
- **Idempotent rebuild (FR-007, SC-004)**: rebuilding against an unchanged source snapshot
  leaves every row value-identical. Newer source records refresh exactly the keys they touch,
  with no recency ceiling, so an arbitrarily late-arriving record still lands.
- **Borough domain (FR-005)**: `borough` is always one of the six canonical values, and
  `Unknown` carries the same metrics and measures as any canonical borough.
- **Non-additivity (FR-010)**: `complaint_count` and `noise_count` overlap by design. Do not sum
  them.

## Enforcing tests (dbt)

- `not_null` on `activity_date`, `borough`, and the three `*_covered` flags.
- `dbt_utils.unique_combination_of_columns(['activity_date', 'borough'])`.
- `accepted_values` on `borough` = the six canonical values.
- `dbt_utils.expression_is_true` per source, asserting the count is null if and only if the
  source is uncovered (for example `(crash_count is null) = (not crashes_covered)`).
- `dbt_utils.expression_is_true` per covered measure, asserting non-negativity wherever the
  coverage flag is true and the value is present (for example `not crashes_covered or
  (crash_count >= 0 and coalesce(persons_injured, 0) >= 0 and coalesce(persons_killed, 0) >=
  0)`). Uncovered values stay nullable, and severity stays nullable even when covered.
- `dbt_utils.expression_is_true` per derived measure, asserting it is null exactly when its
  numerator is unavailable or its denominator is unusable, and a real ratio otherwise:
  `complaints_per_crash is null = (complaint_count is null or crash_count is null or
  crash_count = 0)` and `complaints_per_person_injured is null = (complaint_count is null or
  persons_injured is null or persons_injured = 0)`. The numerator arm matters: a null
  `complaint_count` (311 uncovered) nulls the ratio even where the denominator is fine.

- Singular test `assert_severity_all_or_null.sql`, asserting that on every covered key the
  mart's `persons_injured` and `persons_killed` match a severity total re-derived from
  `stg_collisions` under the all-or-null rule (`case when count(<field>) = count(*) then
  sum(<field>) end`), compared with `is distinct from` so a NULL mismatch is caught. It is the
  only test that fails if the model regresses to a plain `sum()` across unreported rows.

Counts are deliberately not `not_null`, because a null count is the meaningful uncovered signal
rather than a defect.
