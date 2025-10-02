{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_vector_event',
        tags=['drought', 'staging', 'vector'],
        incremental_strategy='merge',
        unique_key=['layer', 'event_id', 'ingest_ts']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('iceberg_environment', 'vector_events') }}
)

SELECT
    tenant_id,
    schema_version,
    ingest_ts,
    ingest_job_id,
    layer,
    event_id,
    region_type,
    region_id,
    valid_start,
    valid_end,
    value,
    unit,
    category,
    severity,
    source_url,
    geometry_wkt,
    CAST(properties AS JSON) AS properties_json
FROM source
{% if is_incremental() %}
  WHERE ingest_ts > (
      select coalesce(max(ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) from {{ this }}
  )
{% endif %}
