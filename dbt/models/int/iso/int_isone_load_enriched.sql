{{
    config(
        materialized='incremental',
        schema='int',
        alias='int_isone_load_enriched',
        tags=['isone', 'load', 'intermediate'],
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
    metadata,
    -- Add derived fields
    mw as load_mw,
    case when mw > 0 then true else false end as is_valid_load,
    -- Add timezone conversion (ISO-NE uses Eastern Time)
    from_unixtime(interval_start / 1000000) as interval_start_est,
    from_unixtime(coalesce(interval_end, interval_start + 5 * 60 * 1000000) / 1000000) as interval_end_est
from {{ ref('stg_isone_load') }}

{% if is_incremental() %}
  -- Only process records newer than the latest processed record
  where ingest_ts > (select coalesce(max(ingest_ts), '1970-01-01T00:00:00Z') from {{ this }})
{% endif %}
