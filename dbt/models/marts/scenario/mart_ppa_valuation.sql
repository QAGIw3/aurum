{{ config(materialized='view') }}

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
from {{ iceberg_relation('market', 'ppa_valuation', 'ppa_valuation') }}
