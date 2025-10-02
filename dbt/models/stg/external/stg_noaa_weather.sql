{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_noaa_weather',
        tags=['external', 'noaa', 'staging'],
        incremental_strategy='merge',
        unique_key=['tenant_id', 'station_id', 'observation_date', 'element']
    )
}}

with source as (
    select
        tenant_id,
        station_id,
        observation_date,
        element,
        station_name,
        latitude,
        longitude,
        elevation_m,
        dataset,
        value,
        raw_value,
        unit,
        observation_time,
        measurement_flag,
        quality_flag,
        source_flag,
        attributes,
        ingest_ts,
        ingest_job_id,
        ingest_run_id,
        ingest_batch_id
    from {{ source('timescale_noaa', 'noaa_weather_timeseries') }}
)
select
    tenant_id,
    station_id,
    observation_date,
    element,
    station_name,
    latitude,
    longitude,
    elevation_m,
    dataset,
    value,
    raw_value,
    unit,
    observation_time,
    measurement_flag,
    quality_flag,
    source_flag,
    attributes,
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
