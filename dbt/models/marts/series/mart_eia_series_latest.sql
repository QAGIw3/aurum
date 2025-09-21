with ranked as (
    select
        *,
        row_number() over (partition by series_id order by period_start desc, ingest_ts desc) as rn
    from {{ ref('int_eia_series_enriched') }}
)
select
    series_id,
    period,
    period_start,
    period_end,
    frequency,
    value_converted as value,
    raw_value,
    unit_raw,
    unit_normalized,
    currency_normalized,
    conversion_factor,
    area,
    sector,
    seasonal_adjustment,
    description,
    source,
    dataset,
    metadata,
    ingest_ts,
    period_date
from ranked
where rn = 1
