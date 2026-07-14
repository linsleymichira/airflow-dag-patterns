# Contract: traffic-crash source landing

The factory-produced landing that the mart depends on. Fixes the config surface, the landed
table shape, and the emitted Asset.

## Factory config (`include/sources.yaml`)

Replaces the `nyc_taxi` entry.

```yaml
- name: nyc_collisions
  socrata_id: h9gi-nx95
  primary_key: collision_id
  freshness_field: crash_date
  asset_uri: raw_collisions
  schedule: "@daily"
```

Contract points:

- `primary_key: collision_id` MUST be a real, unique column on the dataset (verified: unique
  across all 2,269,187 rows). The factory loader raises if no row carries this key.
- `freshness_field: crash_date` drives both the reschedule-mode freshness sensor and the extract
  `$where` bounds, so extraction is event-date aligned with the mart grain.

## Landed table: `raw_collisions.records`

|Column|Type|Contract|
|---|---|---|
|`collision_id`|VARCHAR (PK)|Natural key, `INSERT OR REPLACE` upsert target|
|`freshness_at`|TIMESTAMP|Event timestamp (`crash_date`)|
|`raw`|JSON|Full Socrata payload|
|`_loaded_at`|TIMESTAMP|Load timestamp|

## Emitted Asset

- Produces `Asset("raw_collisions")` on successful load.
- Consumer `nyc_311_dbt` subscribes via `schedule=(RAW_311 | RAW_COLLISIONS | RAW_NOISE)` (OR).

## Behavior guarantees

- **Idempotent (Constitution I)**: same interval re-pull produces the same rows via
  `INSERT OR REPLACE` on `collision_id`. A revised record (same `collision_id`, same stable
  `crash_date`) overwrites the prior version.
- **Freshness (Constitution II)**: the sensor fires only when `max(crash_date) > data_interval_start`.
  On bleeding-edge intervals it reschedules and times out until NYC publishes the next batch.
- **Alerting (Constitution IV)**: inherits `on_failure_callback=alert_callback` from the factory.

## Retired by this change

The `nyc_taxi` source, its `raw_taxi` dbt source, and `stg_taxi_trips` are removed. The taxi
entry was a dead 2014 snapshot with a non-existent `freshness_field` and `primary_key`, so the
replacement also retires that pre-existing defect.
