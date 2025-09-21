{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('iceberg_environment', 'drought_index') }}
)

SELECT
    tenant_id,
    schema_version,
    ingest_ts,
    ingest_job_id,
    series_id,
    region_type,
    region_id,
    dataset,
    "index" AS index_id,
    timescale,
    valid_date,
    as_of,
    value,
    unit,
    poc,
    source_url,
    CAST(metadata AS JSON) AS metadata_json
FROM source
