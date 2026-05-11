{{ config(materialized='view') }}

-- Same record shape as 311 but lands in raw_noise.records via the factory.
-- Pulled from the JSON `raw` column because the factory schema is generic.
select
    unique_key,
    try_cast(raw ->> 'created_date' as timestamp) as created_at,
    try_cast(raw ->> 'closed_date' as timestamp) as closed_at,
    try_cast(raw ->> 'due_date' as timestamp) as due_at,
    freshness_at as updated_at,
    raw ->> 'complaint_type' as complaint_type,
    raw ->> 'descriptor' as descriptor,
    nullif(trim(raw ->> 'borough'), '') as borough,
    raw ->> 'status' as status,
    raw ->> 'resolution_description' as resolution_description,
    cast(try_cast(raw ->> 'created_date' as timestamp) as date) as complaint_date
from {{ source('raw_noise', 'records') }}
