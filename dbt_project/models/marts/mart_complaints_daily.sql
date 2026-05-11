{{ config(
    materialized='incremental',
    unique_key=['complaint_date', 'borough', 'complaint_type'],
    incremental_strategy='delete+insert'
) }}

with base as (
    select
        complaint_date,
        borough,
        complaint_type,
        unique_key,
        created_at,
        closed_at,
        due_at,
        status,
        case
            when closed_at is not null then date_diff('minute', created_at, closed_at)
            else null
        end as resolution_minutes,
        case
            when closed_at is not null and due_at is not null and closed_at > due_at then 1
            else 0
        end as is_overdue
    from {{ ref('stg_311_complaints') }}
    {% if is_incremental() %}
        where complaint_date >= (select coalesce(max(complaint_date), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    complaint_date,
    borough,
    complaint_type,
    count(*) as complaint_count,
    count(closed_at) as closed_count,
    median(resolution_minutes) as median_resolution_minutes,
    quantile_cont(resolution_minutes, 0.95) as p95_resolution_minutes,
    sum(is_overdue)::double / nullif(count(closed_at), 0) as pct_overdue
from base
group by 1, 2, 3
