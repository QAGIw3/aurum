{{ config(materialized='view') }}

{% set iceberg_catalog = var('iceberg_catalog', 'iceberg') %}

select
    ppa_contract_id,
    scenario_id,
    curve_key,
    period_start,
    period_end,
    metric,
    value,
    cashflow,
    npv,
    irr,
    asof_date,
    version_hash
from {{ iceberg_catalog }}.market.ppa_valuation
