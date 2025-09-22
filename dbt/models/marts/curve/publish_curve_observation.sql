{% if target.name == 'duckdb' %}
{{ config(materialized='table', schema='market', alias='curve_observation') }}
{% else %}
{{ iceberg_config_timeseries(
    partition_fields=['asof_date', 'iso', 'product'],
    partition_granularity='day',
    target_file_size_mb=128,
    write_compression='ZSTD',
    sort_order=['asof_date', 'iso', 'product', 'curve_key']
) }}
{{ config(
    materialized='incremental',
    schema='market',
    alias='curve_observation',
    unique_key=['curve_key', 'tenor_label', 'asof_date'],
    incremental_strategy='merge',
    on_schema_change='sync',
    tags=['publish', 'iceberg', 'timeseries']
) }}
{% endif %}

{% set late_arriving_hours = var('curve_late_arriving_hours', 6) %}


with source_rows as (
    select
        asof_date,
        source_file,
        sheet_name,
        asset_class,
        region,
        iso,
        location,
        market,
        product,
        block,
        spark_location,
        price_type,
        units_raw,
        currency,
        per_unit,
        tenor_type,
        contract_month,
        tenor_label,
        mid,
        bid,
        ask,
        curve_key,
        version_hash,
        _ingest_ts,
        'source=iceberg.raw.curve_landing|topic=aurum.curve.observation.v1|table=iceberg.market.curve_observation' as lineage_tags
    from {{ ref('stg_curve_observation') }}
)
select *
from source_rows

{% if is_incremental() %}
where _ingest_ts >= (
    select
        coalesce(max(_ingest_ts), cast('1970-01-01 00:00:00' as timestamp)) - interval '{{ late_arriving_hours }}' hour
    from {{ this }}
)
{% endif %}
