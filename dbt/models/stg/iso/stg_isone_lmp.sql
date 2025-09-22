{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_isone_lmp',
        tags=['isone', 'lmp', 'staging'],
        incremental_strategy='merge',
        unique_key=['iso_code', 'location_id', 'interval_start', 'market'],
        partition_by={
            "field": "delivery_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

select
    iso_code,
    market,
    delivery_date,
    interval_start,
    interval_end,
    interval_minutes,
    location_id,
    location_name,
    location_type,
    price_total,
    price_energy,
    price_congestion,
    price_loss,
    currency,
    uom,
    settlement_point,
    source_run_id,
    ingest_ts,
    record_hash,
    metadata
from {{ source('external', 'isone_lmp') }}

{% if is_incremental() %}
  -- Only process records newer than the latest processed record
  where ingest_ts > (select coalesce(max(ingest_ts), '1970-01-01T00:00:00Z') from {{ this }})
{% endif %}
