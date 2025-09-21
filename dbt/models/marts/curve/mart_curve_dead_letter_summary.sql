{{
    config(
        materialized='view',
        schema='mart',
        alias='mart_curve_dead_letter_summary',
        tags=['curve', 'mart', 'ops']
    )
}}

with base as (
    select
        source,
        severity,
        error_message,
        date_trunc('day', ingest_ts) as ingest_day,
        count(*) as occurrences,
        max(ingest_ts) as last_seen_ts
    from {{ ref('stg_curve_dead_letter') }}
    group by 1, 2, 3, 4
)
select
    source,
    severity,
    ingest_day,
    sum(occurrences) as total_occurrences,
    max(last_seen_ts) as last_seen_ts,
    array_agg(error_message) filter (where occurrences > 0) as sample_errors
from base
group by 1, 2, 3
