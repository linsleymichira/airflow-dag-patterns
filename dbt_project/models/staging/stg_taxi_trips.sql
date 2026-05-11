{{ config(materialized='view') }}

-- Factory-landed taxi trips land in raw_taxi.records with a generic (id, freshness_at, raw)
-- shape. Pull the typed fields out of the JSON payload here. Add columns as the mart needs them.
select
    trip_id,
    freshness_at as pickup_at,
    try_cast(raw ->> 'tpep_dropoff_datetime' as timestamp) as dropoff_at,
    try_cast(raw ->> 'passenger_count' as integer) as passenger_count,
    try_cast(raw ->> 'trip_distance' as double) as trip_distance,
    try_cast(raw ->> 'fare_amount' as double) as fare_amount,
    try_cast(raw ->> 'total_amount' as double) as total_amount,
    cast(freshness_at as date) as trip_date
from {{ source('raw_taxi', 'records') }}
