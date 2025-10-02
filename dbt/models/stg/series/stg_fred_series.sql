{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_fred_series',
        tags=['series', 'fred', 'staging'],
        incremental_strategy='merge',
        unique_key=['tenant_id', 'series_id', 'obs_date']
    )
}}

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
{% if is_incremental() %}
  where ingest_ts > (
      select coalesce(max(ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) from {{ this }}
  )
{% endif %}
