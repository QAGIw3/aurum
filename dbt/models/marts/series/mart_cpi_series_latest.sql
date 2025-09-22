{{
    config(
        materialized='view',
        schema='mart',
        alias='mart_cpi_series_latest',
        tags=['cpi', 'mart']
    )
}}

with ranked as (
    select
        *,
        row_number() over (partition by tenant_id, series_id order by period_date desc nulls last, ingest_ts desc) as rn
    from {{ ref('int_cpi_series_enriched') }}
)
select
    tenant_id,
    series_id,
    period,
    period_date,
    area,
    frequency,
    seasonal_adjustment,
    value,
    units,
    source,
    metadata,
    ingest_ts,
    ingest_job_id,
    ingest_run_id,
    ingest_batch_id
from ranked
where rn = 1
