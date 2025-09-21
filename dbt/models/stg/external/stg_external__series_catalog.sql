WITH raw_series_catalog AS (
    SELECT
        provider,
        series_id,
        dataset_code,
        title,
        description,
        unit_code,
        frequency_code,
        geo_id,
        status,
        category,
        source_url,
        notes,
        start_ts,
        end_ts,
        last_observation_ts,
        asof_date,
        created_at,
        updated_at,
        ingest_ts,
        tags,
        metadata,
        version,
        -- Add row number to handle duplicates based on natural key + version
        ROW_NUMBER() OVER (
            PARTITION BY provider, series_id, COALESCE(version, 0)
            ORDER BY ingest_ts DESC
        ) as rn
    FROM {{ source('iceberg_external', 'series_catalog') }}
)

SELECT
    provider,
    series_id,
    dataset_code,
    title,
    description,
    unit_code,
    frequency_code,
    geo_id,
    status,
    category,
    source_url,
    notes,
    start_ts,
    end_ts,
    last_observation_ts,
    asof_date,
    created_at,
    updated_at,
    ingest_ts,
    tags,
    metadata,
    version
FROM raw_series_catalog
WHERE rn = 1
