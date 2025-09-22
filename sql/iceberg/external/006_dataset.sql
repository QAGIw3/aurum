CREATE TABLE IF NOT EXISTS iceberg.external.dataset (
    tenant_id VARCHAR,
    dataset_code VARCHAR,
    provider VARCHAR,
    name VARCHAR,
    description VARCHAR,
    topic VARCHAR,
    default_frequency_code VARCHAR,
    default_unit_code VARCHAR,
    source_url VARCHAR,
    documentation_url VARCHAR,
    license VARCHAR,
    metadata JSON,
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6),
    ingest_ts TIMESTAMP(6),
    ingest_job_id VARCHAR,
    ingest_run_id VARCHAR,
    ingest_batch_id VARCHAR
)
WITH (
    format = 'PARQUET',
    format_version = '2',
    write_compression = 'ZSTD',
    write_target_file_size_bytes = 134217728,
    optimize_rewrite_data_file_threshold = 4,
    optimize_rewrite_delete_file_threshold = 25,
    vacuum_min_snapshots_to_keep = 4,
    vacuum_max_snapshot_age_retention = '365d',
    partitioning = ARRAY['tenant_id', 'provider']
);
