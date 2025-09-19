-- Derived calendar strips aggregated from monthly curves.
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
), calendarized as (
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
        date_trunc('year', contract_month) as contract_month,
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
        date_trunc('year', contract_month)
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
    'CALENDAR' as tenor_type,
    contract_month,
    concat('Calendar ', cast(year(contract_month) as varchar)) as tenor_label,
    currency,
    per_unit,
    units_raw,
    mid as value,
    mid,
    bid,
    ask,
    lower(to_hex(md5(to_utf8(curve_key || '|calendar|' || cast(year(contract_month) as varchar) || '|' || cast(asof_date as varchar))))) as version_hash,
    _ingest_ts
from calendarized;

