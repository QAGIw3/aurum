select
    *
from {{ ref('mart_ppa_valuation') }}
where lower(metric) = 'cashflow'
  and cashflow is null
