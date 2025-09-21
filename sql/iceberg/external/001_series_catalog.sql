CREATE TABLE IF NOT EXISTS iceberg.external.series_catalog (
    provider VARCHAR,
    series_id VARCHAR,
    dataset_code VARCHAR,
    title VARCHAR,
    description VARCHAR,
    unit_code VARCHAR,
    frequency_code VARCHAR,
    geo_id VARCHAR,
    status VARCHAR,
    category VARCHAR,
    source_url VARCHAR,
    notes VARCHAR,
    start_ts TIMESTAMP(6),
    end_ts TIMESTAMP(6),
    last_observation_ts TIMESTAMP(6),
    asof_date DATE,
    tags ARRAY(VARCHAR),
    metadata JSON,
    version BIGINT,
    updated_at TIMESTAMP(6),
    created_at TIMESTAMP(6),
    ingest_ts TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    format_version = '2',
    write_compression = 'ZSTD',
    write_target_file_size_bytes = 268435456,
    optimize_rewrite_data_file_threshold = 8,
    optimize_rewrite_delete_file_threshold = 100,
    vacuum_min_snapshots_to_keep = 4,
    vacuum_max_snapshot_age_retention = '30d'
);
