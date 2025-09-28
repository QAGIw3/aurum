{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ app_relation('scenarios', 'app_scenarios') }}
)
select
    cast(id as varchar) as scenario_id,
    cast(tenant_id as varchar) as tenant_id,
    trim(name) as scenario_name,
    description,
    lower(coalesce(status, 'draft')) as scenario_status,
    parameters,
    tags,
    cast(created_by as varchar) as created_by,
    try_cast(created_at as timestamp) as created_at,
    try_cast(updated_at as timestamp) as updated_at,
    try_cast(version as integer) as version,
    try_cast(archived_at as timestamp) as archived_at,
    case
        when archived_at is not null then true
        else false
    end as is_archived,
    case
        when archived_at is null and lower(coalesce(status, 'draft')) in ('active', 'running') then true
        else false
    end as is_active,
    date_trunc('day', try_cast(created_at as timestamp)) as created_date,
    date_trunc('day', try_cast(updated_at as timestamp)) as updated_date
from source_data
