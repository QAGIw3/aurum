{{
    config(
        materialized='view',
        schema='mart',
        alias='mart_curve_asof_diff',
        tags=['curve', 'mart']
    )
}}

with ordered as (
    select
        curve_key,
        tenor_label,
        asof_date,
        iso_code,
        market_code,
        product_code,
        block_code,
        mid,
        lag(asof_date) over (
            partition by curve_key, tenor_label
            order by asof_date
        ) as compare_asof_date,
        lag(mid) over (
            partition by curve_key, tenor_label
            order by asof_date
        ) as compare_mid
    from {{ ref('fct_curve_observation') }}
)
select
    curve_key,
    tenor_label,
    asof_date as current_asof_date,
    compare_asof_date,
    iso_code,
    market_code,
    product_code,
    block_code,
    mid as current_mid,
    compare_mid,
    mid - compare_mid as delta_mid
from ordered
where compare_asof_date is not null
