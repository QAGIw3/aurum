{{
    config(
        materialized='view',
        schema='dim',
        alias='dim_asof',
        tags=['curve', 'dimension']
    )
}}

with distinct_asof as (
    select distinct asof_date
    from {{ ref('publish_curve_observation') }}
    where asof_date is not null
)
select
    {{ aurum_text_hash("cast(asof_date as varchar)") }} as asof_sk,
    asof_date,
    date_trunc('month', asof_date) as month_start,
    date_trunc('quarter', asof_date) as quarter_start,
    date_trunc('week', asof_date) as week_start,
    extract(year from asof_date) as year_number,
    extract(quarter from asof_date) as quarter_number,
    extract(month from asof_date) as month_number,
    extract(day from asof_date) as day_number
from distinct_asof
