with series as (
    select * from {{ ref('stg_eia_series') }}
),
units as (
    select
        units_raw,
        currency,
        per_unit
    from {{ ref('eia_units_map') }}
)
select
    s.series_id,
    s.period,
    s.period_start,
    s.period_end,
    s.frequency,
    s.value,
    s.raw_value,
    s.unit_raw,
    s.canonical_unit,
    s.canonical_currency,
    s.canonical_value,
    s.conversion_factor,
    s.area,
    s.sector,
    s.seasonal_adjustment,
    s.description,
    s.source,
    s.dataset,
    s.metadata,
    s.ingest_ts,
    coalesce(s.canonical_currency, u.currency) as currency_normalized,
    coalesce(s.canonical_unit, u.per_unit) as unit_normalized,
    case
        when s.canonical_value is not null then s.canonical_value
        when s.value is not null and s.conversion_factor is not null then s.value * s.conversion_factor
        else s.value
    end as value_converted,
    date(s.period_start) as period_date
from series s
left join units u
    on upper(s.unit_raw) = upper(u.units_raw)
