{{ config(materialized='view') }}

with runs_with_rank as (
    select
        scenario_id,
        scenario_run_id,
        tenant_id,
        run_status,
        run_type,
        priority,
        started_at,
        completed_at,
        created_at,
        updated_at,
        duration_seconds,
        is_success,
        is_failure,
        is_inflight,
        results,
        metadata,
        error_message,
        row_number() over (
            partition by scenario_id
            order by coalesce(updated_at, completed_at, started_at, created_at) desc
        ) as run_rank
    from {{ ref('stg_scenario_runs') }}
),
metrics as (
    select
        scenario_id,
        tenant_id,
        count(*) as total_run_count,
        count(*) filter (where is_success) as completed_run_count,
        count(*) filter (where is_failure) as failed_run_count,
        count(*) filter (where is_inflight) as inflight_run_count,
        avg(duration_seconds) filter (where duration_seconds is not null) as avg_duration_seconds,
        max(updated_at) as max_updated_at,
        max(started_at) as max_started_at
    from runs_with_rank
    group by scenario_id, tenant_id
),
latest as (
    select
        scenario_id,
        scenario_run_id as latest_run_id,
        run_status as latest_run_status,
        run_type as latest_run_type,
        priority as latest_run_priority,
        started_at as latest_run_started_at,
        completed_at as latest_run_completed_at,
        duration_seconds as latest_run_duration_seconds,
        updated_at as latest_run_updated_at,
        error_message as latest_run_error_message,
        results as latest_run_results,
        metadata as latest_run_metadata
    from runs_with_rank
    where run_rank = 1
)
select
    m.scenario_id,
    m.tenant_id,
    m.total_run_count,
    m.completed_run_count,
    m.failed_run_count,
    m.inflight_run_count,
    m.avg_duration_seconds,
    m.max_updated_at,
    m.max_started_at,
    l.latest_run_id,
    l.latest_run_status,
    l.latest_run_type,
    l.latest_run_priority,
    l.latest_run_started_at,
    l.latest_run_completed_at,
    l.latest_run_duration_seconds,
    l.latest_run_updated_at,
    l.latest_run_error_message,
    l.latest_run_results,
    l.latest_run_metadata
from metrics m
left join latest l on m.scenario_id = l.scenario_id
