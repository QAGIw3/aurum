select
    *
from {{ ref('mart_ppa_valuation') }}
where lower(metric) = 'irr'
  and (
        irr is null
        or value is null
        or abs(irr - cast(value as double)) > 1e-6
      )
