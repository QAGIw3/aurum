with source as (
    select
        tenant_id,
        series_id,
        obs_date,
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
    from {{ source('timescale_fred', 'fred_series_timeseries') }}
)
select
    tenant_id,
    series_id,
    obs_date,
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
from source
