{{ config(materialized='view') }}

with scenarios as (
    select *
    from {{ ref('stg_scenarios') }}
),
run_metrics as (
    select *
    from {{ ref('int_scenario_run_metrics') }}
),
output_metrics as (
    select
        scenario_id,
        max(asof_date) as latest_output_asof,
        max(computed_ts) as latest_output_computed_ts,
        count(*) as output_row_count,
        count(distinct metric) as output_metric_count
    from {{ ref('mart_scenario_output') }}
    group by scenario_id
)
select
    s.scenario_id,
    s.tenant_id,
    s.scenario_name,
    s.description,
    s.scenario_status,
    s.is_active,
    s.is_archived,
    s.created_at,
    s.updated_at,
    s.created_date,
    s.updated_date,
    s.version,
    s.parameters,
    s.tags,
    rm.total_run_count,
    rm.completed_run_count,
    rm.failed_run_count,
    rm.inflight_run_count,
    rm.avg_duration_seconds,
    rm.latest_run_id,
    rm.latest_run_status,
    rm.latest_run_type,
    rm.latest_run_priority,
    rm.latest_run_started_at,
    rm.latest_run_completed_at,
    rm.latest_run_duration_seconds,
    rm.latest_run_updated_at,
    rm.latest_run_error_message,
    rm.latest_run_results,
    rm.latest_run_metadata,
    coalesce(om.latest_output_asof, null) as latest_output_asof,
    om.latest_output_computed_ts,
    om.output_row_count,
    om.output_metric_count,
    case when om.latest_output_asof is not null then true else false end as has_outputs,
    current_timestamp as mart_generated_at
from scenarios s
left join run_metrics rm on s.scenario_id = rm.scenario_id
left join output_metrics om on s.scenario_id = om.scenario_id
