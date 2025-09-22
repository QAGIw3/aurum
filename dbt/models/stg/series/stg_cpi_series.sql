with source as (
    select
        tenant_id,
        series_id,
        period,
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
    from {{ source('timescale_cpi', 'cpi_series_timeseries') }}
)
select
    tenant_id,
    series_id,
    period,
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
from source
