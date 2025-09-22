with series as (
    select * from {{ ref('stg_fred_series') }}
)
select
    tenant_id,
    series_id,
    obs_date,
    date_trunc('month', obs_date)::date as obs_month,
    frequency,
    seasonal_adjustment,
    value,
    raw_value,
    units,
    title,
    notes,
    metadata,
    ingest_ts,
    ingest_job_id,
    ingest_run_id,
    ingest_batch_id
from series
