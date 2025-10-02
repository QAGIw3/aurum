{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_isone_interchange',
        tags=['isone', 'interchange', 'staging'],
        incremental_strategy='merge',
        unique_key=['iso_code', 'area', 'interval_start']
    )
}}

select
    iso_code,
    area,
    interval_start,
    interval_end,
    interval_minutes,
    mw,
    ingest_ts,
    metadata
from {{ source('external', 'isone_interchange') }}

{% if is_incremental() %}
  -- Only process records newer than the latest processed record
  where ingest_ts > (select coalesce(max(ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) from {{ this }})
{% endif %}
