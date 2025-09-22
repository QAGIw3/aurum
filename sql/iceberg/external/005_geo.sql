CREATE TABLE IF NOT EXISTS iceberg.external.geo (
    tenant_id VARCHAR,
    geo_id VARCHAR,
    geo_type VARCHAR,
    provider VARCHAR,
    iso_3166_1_alpha2 VARCHAR,
    iso_3166_1_alpha3 VARCHAR,
    iso_3166_2 VARCHAR,
    name VARCHAR,
    parent_geo_id VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    timezone VARCHAR,
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
    partitioning = ARRAY['tenant_id', 'geo_type']
);
