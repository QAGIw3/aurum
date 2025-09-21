CREATE TABLE IF NOT EXISTS iceberg.external.timeseries_observation (
    provider VARCHAR,
    series_id VARCHAR,
    ts TIMESTAMP(6),
    asof_date DATE,
    value DOUBLE,
    value_raw VARCHAR,
    unit_code VARCHAR,
    geo_id VARCHAR,
    dataset_code VARCHAR,
    frequency_code VARCHAR,
    status VARCHAR,
    quality_flag VARCHAR,
    ingest_ts TIMESTAMP(6),
    source_event_id VARCHAR,
    metadata JSON,
    updated_at TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    format_version = '2',
    write_compression = 'ZSTD',
    write_target_file_size_bytes = 536870912,
    optimize_rewrite_data_file_threshold = 24,
    optimize_rewrite_delete_file_threshold = 250,
    vacuum_min_snapshots_to_keep = 12,
    vacuum_max_snapshot_age_retention = '90d',
    partitioning = ARRAY['provider', 'series_id', 'days(ts)'],
    write_sort_order = ARRAY['provider', 'series_id', 'ts', 'asof_date']
);
