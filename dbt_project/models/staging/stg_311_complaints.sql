{{ config(materialized='view') }}

select
    unique_key,
    cast(created_date as timestamp) as created_at,
    cast(closed_date as timestamp) as closed_at,
    cast(due_date as timestamp) as due_at,
    cast(updated_at as timestamp) as updated_at,
    complaint_type,
    descriptor,
    nullif(trim(borough), '') as borough,
    status,
    resolution_description,
    cast(created_date as date) as complaint_date
from {{ source('raw_311', 'complaints') }}
