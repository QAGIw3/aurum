{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.layer, s.event_id
            ORDER BY s.ingest_ts DESC
        ) AS rn
    FROM {{ ref('stg_vector_event') }} AS s
)
SELECT
    r.layer,
    r.event_id,
    r.region_type,
    r.region_id,
    g.region_name,
    g.parent_region_id,
    r.valid_start,
    r.valid_end,
    r.value,
    r.unit,
    r.category,
    r.severity,
    r.source_url,
    r.geometry_wkt,
    r.properties_json,
    r.ingest_ts,
    r.ingest_job_id,
    r.tenant_id
FROM ranked AS r
LEFT JOIN {{ source('iceberg_ref', 'geographies') }} AS g
    ON g.region_type = r.region_type
   AND g.region_id = r.region_id
WHERE r.rn = 1
