select distinct
    tenant_id,
    dataset,
    frequency,
    area,
    sector,
    unit_normalized,
    currency_normalized
from {{ ref('int_eia_series_enriched') }}
