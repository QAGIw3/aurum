{{
    config(
        materialized='table',
        schema='dim',
        alias='dim_iso',
        tags=['curve', 'dimension']
    )
}}

select
    {{ aurum_text_hash("iso_code") }} as iso_sk,
    iso_code,
    iso_name,
    country,
    default_timezone,
    region,
    timezone_offsets
from {{ ref('iso_catalog') }}
