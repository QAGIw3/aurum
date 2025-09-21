{{
    config(
        materialized='table',
        schema='dim',
        alias='dim_market',
        tags=['curve', 'dimension']
    )
}}

with markets as (
    select
        upper(iso_code) as iso_code,
        upper(market_code) as market_code,
        market_name,
        market_category,
        settlement_interval_minutes,
        timezone_override
    from {{ ref('market_catalog') }}
),
iso_dim as (
    select
        iso_sk,
        iso_code,
        default_timezone
    from {{ ref('dim_iso') }}
)
select
    {{ aurum_text_hash("markets.iso_code || ':' || markets.market_code") }} as market_sk,
    markets.iso_code,
    markets.market_code,
    markets.market_name,
    markets.market_category,
    coalesce(markets.timezone_override, iso_dim.default_timezone) as market_timezone,
    markets.settlement_interval_minutes,
    iso_dim.iso_sk
from markets
left join iso_dim
    on iso_dim.iso_code = markets.iso_code
