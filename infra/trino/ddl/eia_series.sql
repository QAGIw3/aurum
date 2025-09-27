CREATE SCHEMA IF NOT EXISTS iceberg.eia;

CREATE TABLE IF NOT EXISTS iceberg.eia.series (
    series_id VARCHAR,
    period VARCHAR,
    period_start TIMESTAMP(6),
    period_end TIMESTAMP(6),
    frequency VARCHAR,
    value DOUBLE,
    raw_value VARCHAR,
    unit VARCHAR,
    canonical_unit VARCHAR,
    canonical_currency VARCHAR,
    canonical_value DOUBLE,
    conversion_factor DOUBLE,
    area VARCHAR,
    sector VARCHAR,
    seasonal_adjustment VARCHAR,
    description VARCHAR,
    source VARCHAR,
    dataset VARCHAR,
    metadata MAP<VARCHAR, VARCHAR>,
    ingest_ts TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['series_id'],
    write_sort_order = ARRAY['series_id', 'period_start']
);

CREATE OR REPLACE VIEW iceberg.eia.series_latest AS
WITH ranked AS (
    SELECT
        series_id,
        period,
        period_start,
        period_end,
        frequency,
        value,
        raw_value,
        unit,
        canonical_unit,
        canonical_currency,
        canonical_value,
        conversion_factor,
        area,
        sector,
        seasonal_adjustment,
        description,
        source,
        dataset,
        metadata,
        ingest_ts,
        ROW_NUMBER() OVER (
            PARTITION BY series_id
            ORDER BY period_start DESC, ingest_ts DESC
        ) AS rn
    FROM iceberg.eia.series
)
SELECT
    series_id,
    period,
    period_start,
    period_end,
    frequency,
    value,
    raw_value,
    unit,
    canonical_unit,
    canonical_currency,
    canonical_value,
    conversion_factor,
    area,
    sector,
    seasonal_adjustment,
    description,
    source,
    dataset,
    metadata,
    ingest_ts
FROM ranked
WHERE rn = 1;
