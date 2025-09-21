{{
    config(
        materialized='table',
        schema='dim',
        alias='dim_block',
        tags=['curve', 'dimension']
    )
}}

select
    {{ aurum_text_hash("block_code") }} as block_sk,
    upper(block_code) as block_code,
    block_name,
    start_hour,
    end_hour,
    total_hours,
    description
from {{ ref('block_catalog') }}
