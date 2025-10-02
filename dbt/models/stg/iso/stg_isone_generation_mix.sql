{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_isone_generation_mix',
        tags=['isone', 'generation_mix', 'staging'],
        incremental_strategy='merge',
        unique_key=['iso_code', 'fuel_type', 'asof_time']
    )
}}

select
    iso_code,
    asof_time,
    fuel_type,
    mw,
    unit,
    ingest_ts,
    metadata
from {{ source('external', 'isone_generation_mix') }}

{% if is_incremental() %}
  -- Only process records newer than the latest processed record
  where ingest_ts > (select coalesce(max(ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) from {{ this }})
{% endif %}
