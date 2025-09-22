{{
    config(
        materialized='table',
        schema='stg',
        alias='stg_isone_generators',
        tags=['isone', 'generators', 'staging', 'reference']
    )
}}

select
    iso_code,
    generator_id,
    generator_name,
    fuel_type,
    capacity_mw,
    uom
from {{ source('external', 'isone_generators') }}

-- Only latest version of each generator
qualify row_number() over (
    partition by iso_code, generator_id
    order by ingest_ts desc
) = 1
