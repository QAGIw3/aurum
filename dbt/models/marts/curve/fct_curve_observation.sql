{{
    config(
        materialized='incremental',
        schema='fact',
        alias='fct_curve_observation',
        unique_key=['curve_key', 'tenor_label', 'asof_date'],
        incremental_strategy='merge',
        on_schema_change='sync',
        tags=['curve', 'fact'],
        table_properties={
            'format': 'ICEBERG',
            'partitioning': "ARRAY['days(asof_date)', 'iso_code', 'product_code']",
            'write_compression': 'ZSTD'
        }
    )
}}

{% set late_arriving_hours = var('curve_fact_late_arriving_hours', var('curve_late_arriving_hours', 6)) %}

with source_data as (
    select
        curve_key,
        tenor_label,
        asof_date,
        iso,
        market,
        product,
        block,
        price_type,
        currency,
        per_unit,
        tenor_type,
        contract_month,
        mid,
        bid,
        ask,
        asset_class,
        region,
        location,
        spark_location,
        source_file,
        sheet_name,
        version_hash,
        _ingest_ts,
        lineage_tags
    from {{ ref('publish_curve_observation') }}
),
normalized as (
    select
        *,
        upper(coalesce(iso, 'UNKNOWN')) as iso_code,
        upper(coalesce(market, 'UNKNOWN')) as market_code,
        upper(coalesce(product, 'UNKNOWN')) as product_code,
        upper(coalesce(block, 'UNKNOWN')) as block_code
    from source_data
),
joined as (
    select
        n.*,
        iso_dim.iso_sk,
        market_dim.market_sk,
        product_dim.product_sk,
        block_dim.block_sk,
        asof_dim.asof_sk
    from normalized n
    left join {{ ref('dim_iso') }} iso_dim
        on iso_dim.iso_code = n.iso_code
    left join {{ ref('dim_market') }} market_dim
        on market_dim.iso_code = n.iso_code
       and market_dim.market_code = n.market_code
    left join {{ ref('dim_product') }} product_dim
        on product_dim.product_code = n.product_code
    left join {{ ref('dim_block') }} block_dim
        on block_dim.block_code = n.block_code
    left join {{ ref('dim_asof') }} asof_dim
        on asof_dim.asof_date = n.asof_date
)
select
    {{ aurum_text_hash("curve_key || ':' || tenor_label || ':' || cast(asof_date as varchar)") }} as curve_observation_sk,
    curve_key,
    tenor_label,
    asof_date,
    tenor_type,
    contract_month,
    iso_code,
    market_code,
    product_code,
    block_code,
    iso_sk,
    market_sk,
    product_sk,
    block_sk,
    asof_sk,
    price_type,
    currency,
    per_unit,
    mid,
    bid,
    ask,
    asset_class,
    region,
    location,
    spark_location,
    source_file,
    sheet_name,
    version_hash,
    _ingest_ts,
    concat(lineage_tags, '|fact=iceberg.fact.fct_curve_observation') as lineage_tags
from joined
{% if is_incremental() %}
where _ingest_ts >= (
    select
        coalesce(max(_ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) - interval '{{ late_arriving_hours }}' hour
    from {{ this }}
)
{% endif %}
