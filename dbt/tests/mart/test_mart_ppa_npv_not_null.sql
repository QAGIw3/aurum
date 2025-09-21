select
    *
from {{ ref('mart_ppa_valuation') }}
where lower(metric) = 'npv'
  and npv is null
