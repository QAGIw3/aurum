{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('iceberg_environment', 'usdm_area') }}
)

SELECT
    tenant_id,
    schema_version,
    ingest_ts,
    ingest_job_id,
    region_type,
    region_id,
    valid_date,
    as_of,
    d0_frac,
    d1_frac,
    d2_frac,
    d3_frac,
    d4_frac,
    source_url,
    CAST(metadata AS JSON) AS metadata_json
FROM source
