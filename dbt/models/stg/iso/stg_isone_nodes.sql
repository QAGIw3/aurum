{{
    config(
        materialized='table',
        schema='stg',
        alias='stg_isone_nodes',
        tags=['isone', 'nodes', 'staging', 'reference']
    )
}}

select
    iso as iso_code,
    location_id,
    location_name,
    location_type,
    zone,
    hub,
    timezone
from {{ ref('iso_nodes') }}
where iso = 'ISO-NE'
