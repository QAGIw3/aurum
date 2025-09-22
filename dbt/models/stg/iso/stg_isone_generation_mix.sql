{{
    config(
        materialized='incremental',
        schema='stg',
        alias='stg_isone_generation_mix',
        tags=['isone', 'generation_mix', 'staging'],
        incremental_strategy='merge',
        unique_key=['iso_code', 'fuel_type', 'asof_time'],
        partition_by={
            "field": "asof_time",
            "data_type": "timestamp",
            "granularity": "hour"
        }
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
  where ingest_ts > (select coalesce(max(ingest_ts), '1970-01-01T00:00:00Z') from {{ this }})
{% endif %}
