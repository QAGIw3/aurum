{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('iceberg_environment', 'vector_events') }}
)

SELECT
    tenant_id,
    schema_version,
    ingest_ts,
    ingest_job_id,
    layer,
    event_id,
    region_type,
    region_id,
    valid_start,
    valid_end,
    value,
    unit,
    category,
    severity,
    source_url,
    geometry_wkt,
    CAST(properties AS JSON) AS properties_json
FROM source
