{{
    config(
        materialized='table',
        schema='dim',
        alias='dim_product',
        tags=['curve', 'dimension']
    )
}}

select
    {{ aurum_text_hash("product_code") }} as product_sk,
    upper(product_code) as product_code,
    product_name,
    tenor_type,
    commodity,
    unit,
    description
from {{ ref('product_catalog') }}
