{% snapshot curve_observation_snapshot %}

{{
    config(
        strategy='timestamp',
        unique_key=['curve_key', 'tenor_label', 'asof_date'],
        updated_at='_ingest_ts',
        target_schema='snapshots',
        invalidate_hard_deletes=True,
        tags=['curve', 'snapshot']
    )
}}

select
    curve_key,
    tenor_label,
    asof_date,
    iso_code,
    market_code,
    product_code,
    block_code,
    price_type,
    currency,
    per_unit,
    tenor_type,
    contract_month,
    mid,
    bid,
    ask,
    version_hash,
    _ingest_ts
from {{ ref('fct_curve_observation') }}

{% endsnapshot %}
