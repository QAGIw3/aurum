{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_usdm_area',
        tags=['drought', 'staging'],
        incremental_strategy='merge',
        unique_key=['region_type', 'region_id', 'valid_date']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('iceberg_environment', 'usdm_area') }}
)

SELECT
    tenant_id,
    schema_version,
    ingest_ts,
    ingest_job_id,
    region_type,
    region_id,
    valid_date,
    as_of,
    d0_frac,
    d1_frac,
    d2_frac,
    d3_frac,
    d4_frac,
    source_url,
    CAST(metadata AS JSON) AS metadata_json
FROM source
{% if is_incremental() %}
  WHERE ingest_ts > (
      select coalesce(max(ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) from {{ this }}
  )
{% endif %}
