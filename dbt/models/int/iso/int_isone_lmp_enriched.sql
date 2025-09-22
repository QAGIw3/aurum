{{
    config(
        materialized='incremental',
        schema='int',
        alias='int_isone_lmp_enriched',
        tags=['isone', 'lmp', 'intermediate'],
        incremental_strategy='merge',
        unique_key=['iso_code', 'location_id', 'interval_start', 'market'],
        partition_by={
            "field": "delivery_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with lmp_stg as (
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
        zone,
        hub,
        timezone,
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
    from {{ ref('stg_isone_lmp') }}

    {% if is_incremental() %}
      -- Only process records newer than the latest processed record
      where ingest_ts > (select coalesce(max(ingest_ts), '1970-01-01T00:00:00Z') from {{ this }})
    {% endif %}
),

node_registry as (
    select
        iso_code,
        location_id,
        location_name,
        location_type,
        zone,
        hub,
        timezone
    from {{ ref('stg_isone_nodes') }}
),

enriched as (
    select
        l.iso_code,
        l.market,
        l.delivery_date,
        l.interval_start,
        l.interval_end,
        l.interval_minutes,
        l.location_id,
        coalesce(l.location_name, n.location_name) as location_name,
        coalesce(l.location_type, n.location_type, 'NODE') as location_type,
        coalesce(l.zone, n.zone) as zone,
        coalesce(l.hub, n.hub) as hub,
        coalesce(l.timezone, n.timezone, 'America/New_York') as timezone,
        l.price_total,
        l.price_energy,
        l.price_congestion,
        l.price_loss,
        l.currency,
        l.uom,
        l.settlement_point,
        l.source_run_id,
        l.ingest_ts,
        l.record_hash,
        l.metadata
    from lmp_stg l
    left join node_registry n
        on l.iso_code = n.iso_code
        and l.location_id = n.location_id
)

select
    *,
    -- Add derived fields
    case
        when price_energy is not null and price_congestion is not null and price_loss is not null
        then price_energy + price_congestion + price_loss
        else price_total
    end as price_components_total,
    -- Add data quality flags
    case when price_total > 0 then true else false end as is_valid_price
from enriched

{% if is_incremental() %}
  -- Only process records newer than the latest processed record
  where ingest_ts > (select coalesce(max(ingest_ts), '1970-01-01T00:00:00Z') from {{ this }})
{% endif %}
