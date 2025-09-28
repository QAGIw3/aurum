with row_count as (
    select count(*) as cnt
    from {{ ref('mart_scenario_overview') }}
)
select *
from row_count
where cnt = 0
