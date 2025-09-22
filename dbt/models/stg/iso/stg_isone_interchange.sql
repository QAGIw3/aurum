{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_isone_interchange',
        tags=['isone', 'interchange', 'staging'],
        incremental_strategy='merge',
        unique_key=['iso_code', 'area', 'interval_start'],
        partition_by={
            "field": "interval_start",
            "data_type": "timestamp",
            "granularity": "hour"
        }
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
  where ingest_ts > (select coalesce(max(ingest_ts), '1970-01-01T00:00:00Z') from {{ this }})
{% endif %}
