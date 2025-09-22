{{
    config(
        materialized='table',
        schema='stg',
        alias='stg_isone_nodes',
        tags=['isone', 'nodes', 'staging', 'reference']
    )
}}

select
    iso_code,
    location_id,
    location_name,
    location_type,
    zone,
    hub,
    timezone
from {{ source('external', 'isone_nodes') }}

-- Only latest version of each node
qualify row_number() over (
    partition by iso_code, location_id
    order by ingest_ts desc
) = 1
