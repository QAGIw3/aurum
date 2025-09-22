with latest as (
    select max(asof_date) as max_asof
    from {{ ref('mart_curve_latest') }}
)
select *
from latest
where max_asof < (current_date - interval '3' day)
