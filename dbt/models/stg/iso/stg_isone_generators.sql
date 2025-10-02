{{ iceberg_config_dimension(target_file_size_mb=64, write_compression='ZSTD') }}
{{
    config(
        materialized='table',
        schema='stg',
        alias='stg_isone_generators',
        tags=['isone', 'generators', 'staging', 'reference']
    )
}}

with ranked as (
    select
        iso_code,
        generator_id,
        generator_name,
        fuel_type,
        capacity_mw,
        uom,
        ingest_ts,
        row_number() over (
            partition by iso_code, generator_id
            order by ingest_ts desc
        ) as rn
    from {{ source('external', 'isone_generators') }}
)
select
    iso_code,
    generator_id,
    generator_name,
    fuel_type,
    capacity_mw,
    uom
from ranked
where rn = 1
