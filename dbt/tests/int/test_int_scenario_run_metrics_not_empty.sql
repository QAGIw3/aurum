with row_count as (
    select count(*) as cnt
    from {{ ref('int_scenario_run_metrics') }}
)
select *
from row_count
where cnt = 0
