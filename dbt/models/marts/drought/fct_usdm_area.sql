{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.region_type, s.region_id, s.valid_date
            ORDER BY s.ingest_ts DESC
        ) AS rn
    FROM {{ ref('stg_usdm_area') }} AS s
)
SELECT
    r.region_type,
    r.region_id,
    g.region_name,
    g.parent_region_id,
    r.valid_date,
    r.as_of,
    r.d0_frac,
    r.d1_frac,
    r.d2_frac,
    r.d3_frac,
    r.d4_frac,
    r.source_url,
    r.metadata_json,
    r.ingest_ts,
    r.ingest_job_id,
    r.tenant_id
FROM ranked AS r
LEFT JOIN {{ source('iceberg_ref', 'geographies') }} AS g
    ON g.region_type = r.region_type
   AND g.region_id = r.region_id
WHERE r.rn = 1
