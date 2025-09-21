with latest as (
    select max(_ingest_ts) as max_ingest_ts
    from {{ ref('fct_curve_observation') }}
)
select *
from latest
where max_ingest_ts < (current_timestamp - interval '12' hour)
