{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ app_relation('scenario_runs', 'app_scenario_runs') }}
),
typed as (
    select
        cast(id as varchar) as scenario_run_id,
        cast(scenario_id as varchar) as scenario_id,
        cast(tenant_id as varchar) as tenant_id,
        lower(coalesce(status, 'queued')) as run_status,
        lower(coalesce(run_type, 'batch')) as run_type,
        lower(coalesce(priority, 'normal')) as priority,
        try_cast(started_at as timestamp) as started_at_ts,
        try_cast(completed_at as timestamp) as completed_at_ts,
        try_cast(created_at as timestamp) as created_at_ts,
        try_cast(updated_at as timestamp) as updated_at_ts,
        error_message,
        results,
        metadata
    from source_data
)
select
    scenario_run_id,
    scenario_id,
    tenant_id,
    run_status,
    run_type,
    priority,
    started_at_ts as started_at,
    completed_at_ts as completed_at,
    created_at_ts as created_at,
    updated_at_ts as updated_at,
    error_message,
    results,
    metadata,
    case
        when started_at_ts is not null and completed_at_ts is not null
            then {{ time_diff_seconds('started_at_ts', 'completed_at_ts') }}
        else null
    end as duration_seconds,
    case when run_status in ('completed', 'success') then true else false end as is_success,
    case when run_status in ('failed', 'error') then true else false end as is_failure,
    case when run_status in ('running', 'queued') then true else false end as is_inflight
from typed
