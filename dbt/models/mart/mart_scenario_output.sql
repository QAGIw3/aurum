-- Minimal model to expose scenario outputs via Trino/Iceberg
-- Assumes SeaTunnel or the scenario worker lands to iceberg.market.scenario_output

{{ config(materialized='view') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by tenant_id, scenario_id, curve_key, metric, tenor_label
            order by asof_date desc, computed_ts desc nulls last
        ) as rn
    from iceberg.market.scenario_output
    where asof_date is not null
),
latest as (
    select
        scenario_id,
        tenant_id,
        asof_date,
        curve_key,
        tenor_type,
        contract_month,
        tenor_label,
        metric,
        value,
        band_lower,
        band_upper,
        attribution,
        version_hash,
        computed_ts
    from ranked
    where rn = 1
)
select
    l.scenario_id,
    l.tenant_id,
    tenants.tenant_name,
    l.asof_date,
    l.curve_key,
    l.tenor_type,
    l.contract_month,
    l.tenor_label,
    l.metric,
    metrics.description as metric_description,
    metrics.unit as metric_unit,
    l.value,
    l.band_lower,
    l.band_upper,
    l.version_hash,
    l.computed_ts
from latest l
left join {{ ref('tenant_catalog') }} tenants on l.tenant_id = tenants.tenant_id
left join {{ ref('scenario_metric_catalog') }} metrics on l.metric = metrics.metric
