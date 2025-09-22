{{
    config(
        materialized='incremental',
        schema='int',
        alias='int_isone_generation_mix_enriched',
        tags=['isone', 'generation_mix', 'intermediate'],
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
    metadata,
    -- Add derived fields
    mw as generation_mw,
    case when mw > 0 then true else false end as is_valid_generation,
    -- Add timezone conversion
    from_unixtime(asof_time / 1000000) as asof_time_est
from {{ ref('stg_isone_generation_mix') }}

{% if is_incremental() %}
  -- Only process records newer than the latest processed record
  where ingest_ts > (select coalesce(max(ingest_ts), '1970-01-01T00:00:00Z') from {{ this }})
{% endif %}
