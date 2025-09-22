with weather as (
    select * from {{ ref('stg_noaa_weather') }}
)
select
    tenant_id,
    station_id,
    observation_date,
    element,
    station_name,
    latitude,
    longitude,
    elevation_m,
    dataset,
    value,
    raw_value,
    unit,
    observation_time,
    measurement_flag,
    quality_flag,
    source_flag,
    attributes,
    ingest_ts,
    ingest_job_id,
    ingest_run_id,
    ingest_batch_id
from weather
