-- Business mart exposing the latest validated curve per instrument and tenor.
with ordered as (
    select
        *,
        row_number() over (
            partition by curve_key, tenor_label
            order by asof_date desc, _ingest_ts desc
        ) as rn
    from {{ ref('int_curve_monthly') }}
)
select
    curve_key,
    tenor_label,
    tenor_type,
    contract_month,
    asof_date,
    currency,
    per_unit,
    mid,
    bid,
    ask,
    version_hash
from ordered
where rn = 1
