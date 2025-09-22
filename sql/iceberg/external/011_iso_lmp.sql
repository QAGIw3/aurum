CREATE TABLE IF NOT EXISTS iceberg.external.iso_lmp (
    iso VARCHAR,
    dataset VARCHAR,
    dt DATE,

    iso_code VARCHAR,
    market VARCHAR,
    delivery_date DATE,
    interval_start TIMESTAMP(6),
    interval_end TIMESTAMP(6),
    interval_minutes INTEGER,
    location_id VARCHAR,
    location_name VARCHAR,
    location_type VARCHAR,
    zone VARCHAR,
    hub VARCHAR,
    timezone VARCHAR,
    price_total DOUBLE,
    price_energy DOUBLE,
    price_congestion DOUBLE,
    price_loss DOUBLE,
    currency VARCHAR,
    uom VARCHAR,
    settlement_point VARCHAR,
    source_run_id VARCHAR,
    ingest_ts TIMESTAMP(6),
    record_hash VARCHAR,
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
    write_sort_order = ARRAY['iso', 'dataset', 'location_id', 'interval_start']
);

