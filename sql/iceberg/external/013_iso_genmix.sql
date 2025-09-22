CREATE TABLE IF NOT EXISTS iceberg.external.iso_genmix (
    iso VARCHAR,
    dataset VARCHAR,
    dt DATE,

    iso_code VARCHAR,
    asof_time TIMESTAMP(6),
    fuel_type VARCHAR,
    mw DOUBLE,
    unit VARCHAR,
    ingest_ts TIMESTAMP(6),
    metadata JSON,
    created_at TIMESTAMP(6),
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
    partitioning = ARRAY['iso', 'dataset', 'dt'],
    write_sort_order = ARRAY['iso', 'dataset', 'fuel_type', 'asof_time']
);

