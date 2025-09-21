{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.series_id, s.valid_date
            ORDER BY s.ingest_ts DESC
        ) AS rn
    FROM {{ ref('stg_drought_index') }} AS s
),
latest AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
)
SELECT
    l.series_id,
    l.dataset,
    l.index_id,
    l.timescale,
    l.valid_date,
    l.as_of,
    l.value,
    l.unit,
    l.poc,
    l.region_type,
    l.region_id,
    g.region_name,
    g.parent_region_id,
    l.source_url,
    l.metadata_json,
    l.ingest_ts,
    l.ingest_job_id,
    l.tenant_id
FROM latest AS l
LEFT JOIN {{ iceberg_relation('ref', 'geographies', 'geographies') }} AS g
    ON g.region_type = l.region_type
   AND g.region_id = l.region_id
