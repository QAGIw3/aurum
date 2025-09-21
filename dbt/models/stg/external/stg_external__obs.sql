WITH raw_timeseries_observations AS (
    SELECT
        provider,
        series_id,
        ts,
        asof_date,
        value,
        value_raw,
        unit_code,
        geo_id,
        dataset_code,
        frequency_code,
        status,
        quality_flag,
        ingest_ts,
        source_event_id,
        metadata,
        -- Add row number to handle duplicates based on natural key
        ROW_NUMBER() OVER (
            PARTITION BY provider, series_id, ts, asof_date
            ORDER BY ingest_ts DESC
        ) as rn
    FROM {{ source('iceberg_external', 'timeseries_observation') }}
)

SELECT
    provider,
    series_id,
    ts,
    asof_date,
    value,
    value_raw,
    unit_code,
    geo_id,
    dataset_code,
    frequency_code,
    status,
    quality_flag,
    ingest_ts,
    source_event_id,
    metadata
FROM raw_timeseries_observations
WHERE rn = 1
