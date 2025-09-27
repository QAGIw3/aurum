CREATE SCHEMA IF NOT EXISTS iceberg.environment;

-- Zonal drought index time series derived from raster statistics
CREATE TABLE IF NOT EXISTS iceberg.environment.drought_index (
    tenant_id VARCHAR,
    schema_version VARCHAR,
    ingest_ts TIMESTAMP(6),
    ingest_job_id VARCHAR,
    series_id VARCHAR,
    region_type VARCHAR,
    region_id VARCHAR,
    dataset VARCHAR,
    index VARCHAR,
    timescale VARCHAR,
    valid_date DATE,
    as_of TIMESTAMP(6),
    value DOUBLE,
    unit VARCHAR,
    poc VARCHAR,
    source_url VARCHAR,
    metadata MAP<VARCHAR, VARCHAR>
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['valid_date', 'region_type'],
    write_sort_order = ARRAY['valid_date', 'region_id', 'index', 'timescale']
);

-- Weekly USDM severity shares by geography
CREATE TABLE IF NOT EXISTS iceberg.environment.usdm_area (
    tenant_id VARCHAR,
    schema_version VARCHAR,
    ingest_ts TIMESTAMP(6),
    ingest_job_id VARCHAR,
    region_type VARCHAR,
    region_id VARCHAR,
    valid_date DATE,
    as_of TIMESTAMP(6),
    d0_frac DOUBLE,
    d1_frac DOUBLE,
    d2_frac DOUBLE,
    d3_frac DOUBLE,
    d4_frac DOUBLE,
    source_url VARCHAR,
    metadata MAP<VARCHAR, VARCHAR>
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['valid_date', 'region_type'],
    write_sort_order = ARRAY['valid_date', 'region_id']
);

-- Canonicalized vector overlays (QPF, AHPS, AQI, wildfire, etc.)
CREATE TABLE IF NOT EXISTS iceberg.environment.vector_events (
    tenant_id VARCHAR,
    schema_version VARCHAR,
    ingest_ts TIMESTAMP(6),
    ingest_job_id VARCHAR,
    layer VARCHAR,
    event_id VARCHAR,
    region_type VARCHAR,
    region_id VARCHAR,
    valid_start TIMESTAMP(6),
    valid_end TIMESTAMP(6),
    value DOUBLE,
    unit VARCHAR,
    category VARCHAR,
    severity VARCHAR,
    source_url VARCHAR,
    geometry_wkt VARCHAR,
    properties MAP<VARCHAR, VARCHAR>
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['region_type'],
    write_sort_order = ARRAY['layer', 'region_id', 'valid_start']
);

-- Recommended Z-ORDER maintenance
-- ALTER TABLE iceberg.environment.drought_index EXECUTE optimize ORDER BY (region_id, index, timescale);
-- ALTER TABLE iceberg.environment.usdm_area EXECUTE optimize ORDER BY (region_id);
-- ALTER TABLE iceberg.environment.vector_events EXECUTE optimize ORDER BY (layer, region_id, valid_start);
