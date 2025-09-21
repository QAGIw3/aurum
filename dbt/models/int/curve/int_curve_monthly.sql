-- Intermediate model filtering to monthly tenors and deduplicating by curve identity.
with ranked as (
    select
        *,
        row_number() over (
            partition by curve_key, tenor_label, asof_date
            order by _ingest_ts desc
        ) as rn
    from {{ ref('stg_curve_observation') }}
    where tenor_type = 'MONTHLY'
)
select
    asof_date,
    curve_key,
    tenor_label,
    tenor_type,
    contract_month,
    currency,
    per_unit,
    units_raw,
    mid,
    bid,
    ask,
    version_hash,
    _ingest_ts
from ranked
where rn = 1
