--
-- Staging layer for external series catalog.
-- Deduplicates by (tenant_id, provider, series_id, version) and surfaces
-- canonical ISO fields produced by ingestion (`iso_*`). Metadata is a
-- string map; complex values are JSON-stringified upstream for Avro safety.
--
{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_external__series_catalog',
        tags=['external', 'staging'],
        incremental_strategy='merge',
        unique_key=['tenant_id', 'provider', 'series_id', 'version']
    )
}}

WITH raw_series_catalog AS (
    SELECT
        tenant_id,
        provider,
        series_id,
        dataset_code,
        title,
        description,
        unit_code,
        frequency_code,
        geo_id,
        status,
        category,
        source_url,
        notes,
        start_ts,
        end_ts,
        last_observation_ts,
        asof_date,
        created_at,
        updated_at,
        ingest_ts,
        ingest_job_id,
        ingest_run_id,
        ingest_batch_id,
        tags,
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
        version,
        -- Add row number to handle duplicates based on natural key + version
        ROW_NUMBER() OVER (
            PARTITION BY tenant_id, provider, series_id, COALESCE(version, 0)
            ORDER BY ingest_ts DESC
        ) as rn
    FROM {{ source('iceberg_external', 'series_catalog') }}
)

SELECT
    tenant_id,
    provider,
    series_id,
    dataset_code,
    title,
    description,
    unit_code,
    frequency_code,
    geo_id,
    status,
    category,
    source_url,
    notes,
    start_ts,
    end_ts,
    last_observation_ts,
    asof_date,
    created_at,
    updated_at,
    ingest_ts,
    ingest_job_id,
    ingest_run_id,
    ingest_batch_id,
    tags,
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
    version
FROM raw_series_catalog
WHERE rn = 1
{% if is_incremental() %}
  AND ingest_ts > (
      select coalesce(max(ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) from {{ this }}
  )
{% endif %}
