{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_drought_index',
        tags=['drought', 'staging'],
        incremental_strategy='merge',
        unique_key=['series_id', 'valid_date']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('iceberg_environment', 'drought_index') }}
)

SELECT
    tenant_id,
    schema_version,
    ingest_ts,
    ingest_job_id,
    series_id,
    region_type,
    region_id,
    dataset,
    "index" AS index_id,
    timescale,
    valid_date,
    as_of,
    value,
    unit,
    poc,
    source_url,
    CAST(metadata AS JSON) AS metadata_json
FROM source
{% if is_incremental() %}
  WHERE ingest_ts > (
      select coalesce(max(ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) from {{ this }}
  )
{% endif %}
