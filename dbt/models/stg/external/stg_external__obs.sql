--
-- Staging layer for external observations.
-- Deduplicates by natural key (tenant, provider, series, ts, asof) and passes
-- through canonical ISO fields (`iso_*`) when available from ingestion.
-- Metadata is a string map (JSON for complex structures).
--
{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_external__obs',
        tags=['external', 'staging', 'timeseries'],
        incremental_strategy='merge',
        unique_key=['tenant_id', 'provider', 'series_id', 'ts', 'asof_date']
    )
}}

WITH raw_timeseries_observations AS (
    SELECT
        tenant_id,
        provider,
        series_id,
        ts,
        asof_date,
        value,
        value_raw,
        unit_code,
        geo_id,
        dataset_code,
        frequency_code,
        status,
        quality_flag,
        ingest_ts,
        ingest_job_id,
        ingest_run_id,
        ingest_batch_id,
        source_event_id,
        metadata,
        iso_code,
        iso_market,
        iso_product,
        iso_location_type,
        iso_location_id,
        iso_location_name,
        iso_timezone,
        iso_interval_minutes,
        iso_unit,
        iso_subject,
        iso_curve_role,
        -- Add row number to handle duplicates based on natural key
        ROW_NUMBER() OVER (
            PARTITION BY tenant_id, provider, series_id, ts, asof_date
            ORDER BY ingest_ts DESC
        ) as rn
    FROM {{ source('iceberg_external', 'timeseries_observation') }}
)

SELECT
    tenant_id,
    provider,
    series_id,
    ts,
    asof_date,
    value,
    value_raw,
    unit_code,
    geo_id,
    dataset_code,
    frequency_code,
    status,
    quality_flag,
    ingest_ts,
    ingest_job_id,
    ingest_run_id,
    ingest_batch_id,
    source_event_id,
    metadata,
    iso_code,
    iso_market,
    iso_product,
    iso_location_type,
    iso_location_id,
    iso_location_name,
    iso_timezone,
    iso_interval_minutes,
    iso_unit,
    iso_subject,
    iso_curve_role
FROM raw_timeseries_observations
WHERE rn = 1
{% if is_incremental() %}
  AND ingest_ts > (
      select coalesce(max(ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) from {{ this }}
  )
{% endif %}
