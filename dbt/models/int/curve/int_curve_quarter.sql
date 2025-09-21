-- Derived quarterly strips aggregated from monthly curves.
with monthly as (
    select
        asof_date,
        curve_key,
        asset_class,
        region,
        iso,
        location,
        market,
        product,
        block,
        spark_location,
        currency,
        per_unit,
        units_raw,
        contract_month,
        mid,
        bid,
        ask,
        _ingest_ts
    from {{ ref('stg_curve_observation') }}
    where tenor_type = 'MONTHLY'
), quarterly as (
    select
        asof_date,
        curve_key,
        asset_class,
        region,
        iso,
        location,
        market,
        product,
        block,
        spark_location,
        currency,
        per_unit,
        units_raw,
        date_trunc('quarter', contract_month) as contract_month,
        avg(mid) as mid,
        avg(bid) as bid,
        avg(ask) as ask,
        max(_ingest_ts) as _ingest_ts
    from monthly
    group by
        asof_date,
        curve_key,
        asset_class,
        region,
        iso,
        location,
        market,
        product,
        block,
        spark_location,
        currency,
        per_unit,
        units_raw,
        date_trunc('quarter', contract_month)
)
select
    asof_date,
    curve_key,
    asset_class,
    region,
    iso,
    location,
    market,
    product,
    block,
    spark_location,
    'MID' as price_type,
    'QUARTER' as tenor_type,
    contract_month,
    concat('Q', cast(quarter(contract_month) as varchar), ' ', cast(year(contract_month) as varchar)) as tenor_label,
    currency,
    per_unit,
    units_raw,
    mid as value,
    mid,
    bid,
    ask,
    {{ aurum_text_hash("curve_key || '|quarter|' || cast(year(contract_month) as varchar) || 'Q' || cast(quarter(contract_month) as varchar) || '|' || cast(asof_date as varchar)") }} as version_hash,
    _ingest_ts
from quarterly

