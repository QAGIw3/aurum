{% if target.name == 'duckdb' %}
{{ config(materialized='table', schema='market', alias='curve_observation') }}
{% else %}
{{ config(
    materialized='incremental',
    schema='market',
    alias='curve_observation',
    unique_key=['curve_key', 'tenor_label', 'asof_date'],
    incremental_strategy='merge',
    on_schema_change='sync',
    tags=['publish']
) }}
{% endif %}


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
        _ingest_ts
    from {{ ref('stg_curve_observation') }}
)
select *
from source_rows

{% if is_incremental() %}
where _ingest_ts > (
    select coalesce(max(_ingest_ts), timestamp '1970-01-01 00:00:00')
    from {{ this }}
)
{% endif %}
