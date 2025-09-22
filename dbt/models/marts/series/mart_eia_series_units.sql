select distinct
    tenant_id,
    unit_raw,
    unit_normalized,
    currency_normalized,
    coalesce(conversion_factor, 1.0) as conversion_factor
from {{ ref('int_eia_series_enriched') }}
where unit_raw is not null
