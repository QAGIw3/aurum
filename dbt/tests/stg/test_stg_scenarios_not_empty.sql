with row_count as (
    select count(*) as cnt
    from {{ ref('stg_scenarios') }}
)
select *
from row_count
where cnt = 0
