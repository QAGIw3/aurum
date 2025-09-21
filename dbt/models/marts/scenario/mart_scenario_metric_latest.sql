{{ config(materialized='view') }}

with ranked as (
    select
        scenario_id,
        tenant_id,
        run_id,
        metric,
        value,
        asof_date,
        computed_ts,
        tenor_label,
        row_number() over (
            partition by tenant_id, scenario_id, metric
            order by asof_date desc, computed_ts desc nulls last
        ) as rn
    from {{ ref('mart_scenario_output') }}
)
select
    scenario_id,
    tenant_id,
    run_id,
    metric,
    value as latest_value,
    asof_date as latest_asof_date,
    tenor_label as latest_tenor_label,
    computed_ts as latest_computed_ts
from ranked
where rn = 1
