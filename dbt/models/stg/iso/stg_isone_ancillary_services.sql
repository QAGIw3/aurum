{{ iceberg_config_staging(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_isone_ancillary_services',
        tags=['isone', 'ancillary_services', 'staging'],
        incremental_strategy='merge',
        unique_key=['iso_code', 'zone', 'product', 'interval_start', 'market']
    )
}}

select
    iso_code,
    market,
    product,
    zone,
    preliminary_final,
    interval_start,
    interval_end,
    interval_minutes,
    price_mcp,
    currency,
    uom,
    ingest_ts,
    record_hash,
    metadata
from {{ source('external', 'isone_ancillary_services') }}

{% if is_incremental() %}
  -- Only process records newer than the latest processed record
  where ingest_ts > (select coalesce(max(ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) from {{ this }})
{% endif %}
