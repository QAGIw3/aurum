{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_isone_ancillary_services',
        tags=['isone', 'ancillary_services', 'staging'],
        incremental_strategy='merge',
        unique_key=['iso_code', 'zone', 'product', 'interval_start', 'market'],
        partition_by={
            "field": "interval_start",
            "data_type": "timestamp",
            "granularity": "hour"
        }
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
  where ingest_ts > (select coalesce(max(ingest_ts), '1970-01-01T00:00:00Z') from {{ this }})
{% endif %}
