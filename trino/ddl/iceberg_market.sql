CREATE SCHEMA IF NOT EXISTS iceberg.market;
CREATE SCHEMA IF NOT EXISTS iceberg.raw;
CREATE SCHEMA IF NOT EXISTS iceberg.fact;

CREATE TABLE IF NOT EXISTS iceberg.raw.curve_landing (
    asof_date DATE,
    source_file VARCHAR,
    sheet_name VARCHAR,
    asset_class VARCHAR,
    region VARCHAR,
    iso VARCHAR,
    location VARCHAR,
    market VARCHAR,
    product VARCHAR,
    block VARCHAR,
    spark_location VARCHAR,
    price_type VARCHAR,
    units_raw VARCHAR,
    currency VARCHAR,
    per_unit VARCHAR,
    tenor_type VARCHAR,
    contract_month DATE,
    tenor_label VARCHAR,
    value DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    mid DOUBLE,
    curve_key VARCHAR,
    version_hash VARCHAR,
    _ingest_ts TIMESTAMP(6),
    quarantine_reason VARCHAR
)
WITH (
    format = 'PARQUET',
    format_version = '2',
    write_compression = 'ZSTD',
    write_target_file_size_bytes = 536870912,
    optimize_rewrite_data_file_threshold = 24,
    optimize_rewrite_delete_file_threshold = 250,
    vacuum_min_snapshots_to_keep = 4,
    vacuum_max_snapshot_age_retention = '7d',
    partitioning = ARRAY['days(asof_date)']
);

CREATE TABLE IF NOT EXISTS iceberg.market.curve_observation (
    asof_date DATE,
    source_file VARCHAR,
    sheet_name VARCHAR,
    asset_class VARCHAR,
    region VARCHAR,
    iso VARCHAR,
    location VARCHAR,
    market VARCHAR,
    product VARCHAR,
    block VARCHAR,
    spark_location VARCHAR,
    price_type VARCHAR,
    units_raw VARCHAR,
    currency VARCHAR,
    per_unit VARCHAR,
    tenor_type VARCHAR,
    contract_month DATE,
    tenor_label VARCHAR,
    value DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    mid DOUBLE,
    curve_key VARCHAR,
    version_hash VARCHAR,
    _ingest_ts TIMESTAMP(6),
    lineage_tags VARCHAR
)
WITH (
    format = 'PARQUET',
    format_version = '2',
    write_compression = 'ZSTD',
    write_target_file_size_bytes = 536870912,
    optimize_rewrite_data_file_threshold = 24,
    optimize_rewrite_delete_file_threshold = 250,
    vacuum_min_snapshots_to_keep = 4,
    vacuum_max_snapshot_age_retention = '7d',
    partitioning = ARRAY['days(asof_date)'],
    write_sort_order = ARRAY['asof_date', 'asset_class', 'iso', 'tenor_label']
);

CREATE TABLE IF NOT EXISTS iceberg.market.curve_observation_quarantine (
    asof_date DATE,
    source_file VARCHAR,
    sheet_name VARCHAR,
    asset_class VARCHAR,
    region VARCHAR,
    iso VARCHAR,
    location VARCHAR,
    market VARCHAR,
    product VARCHAR,
    block VARCHAR,
    spark_location VARCHAR,
    price_type VARCHAR,
    units_raw VARCHAR,
    currency VARCHAR,
    per_unit VARCHAR,
    tenor_type VARCHAR,
    contract_month DATE,
    tenor_label VARCHAR,
    value DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    mid DOUBLE,
    curve_key VARCHAR,
    version_hash VARCHAR,
    _ingest_ts TIMESTAMP(6),
    quarantine_reason VARCHAR
)
WITH (
    format = 'PARQUET',
    format_version = '2',
    write_compression = 'ZSTD',
    write_target_file_size_bytes = 536870912,
    optimize_rewrite_data_file_threshold = 24,
    optimize_rewrite_delete_file_threshold = 250,
    vacuum_min_snapshots_to_keep = 4,
    vacuum_max_snapshot_age_retention = '7d',
    partitioning = ARRAY['days(asof_date)'],
    write_sort_order = ARRAY['asof_date', 'asset_class', 'iso', 'tenor_label']
);

CREATE TABLE IF NOT EXISTS iceberg.market.scenario_output (
    asof_date DATE,
    scenario_id VARCHAR,
    tenant_id VARCHAR,
    run_id VARCHAR,
    curve_key VARCHAR,
    tenor_type VARCHAR,
    contract_month DATE,
    tenor_label VARCHAR,
    metric VARCHAR,
    value DOUBLE,
    band_lower DOUBLE,
    band_upper DOUBLE,
    attribution VARCHAR,
    version_hash VARCHAR,
    computed_ts TIMESTAMP(6),
    _ingest_ts TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    format_version = '2',
    write_compression = 'ZSTD',
    write_target_file_size_bytes = 536870912,
    optimize_rewrite_data_file_threshold = 24,
    optimize_rewrite_delete_file_threshold = 250,
    vacuum_min_snapshots_to_keep = 4,
    vacuum_max_snapshot_age_retention = '7d',
    partitioning = ARRAY['scenario_id', 'metric', 'days(asof_date)']
);

CREATE TABLE IF NOT EXISTS iceberg.market.ppa_valuation (
    asof_date DATE,
    ppa_contract_id VARCHAR,
    scenario_id VARCHAR,
    curve_key VARCHAR,
    period_start DATE,
    period_end DATE,
    cashflow DECIMAL(18, 6),
    npv DECIMAL(18, 6),
    irr DOUBLE,
    metric VARCHAR,
    value DECIMAL(18, 6),
    version_hash VARCHAR,
    _ingest_ts TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    format_version = '2',
    write_compression = 'ZSTD',
    write_target_file_size_bytes = 536870912,
    optimize_rewrite_data_file_threshold = 24,
    optimize_rewrite_delete_file_threshold = 250,
    vacuum_min_snapshots_to_keep = 4,
    vacuum_max_snapshot_age_retention = '7d',
    partitioning = ARRAY['ppa_contract_id', 'days(asof_date)']
);

CREATE TABLE IF NOT EXISTS iceberg.market.qa_checks (
    asof_date DATE,
    check_name VARCHAR,
    check_type VARCHAR,
    status VARCHAR,
    severity VARCHAR,
    curve_key VARCHAR,
    details VARCHAR,
    run_id VARCHAR,
    version_hash VARCHAR,
    _ingest_ts TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    format_version = '2',
    write_compression = 'ZSTD',
    write_target_file_size_bytes = 536870912,
    optimize_rewrite_data_file_threshold = 24,
    optimize_rewrite_delete_file_threshold = 250,
    vacuum_min_snapshots_to_keep = 4,
    vacuum_max_snapshot_age_retention = '7d',
    partitioning = ARRAY['days(asof_date)']
);

CREATE OR REPLACE VIEW iceberg.market.curve_observation_latest AS
WITH ranked AS (
    SELECT
        asof_date,
        source_file,
        sheet_name,
        asset_class,
        region,
        iso,
        location,
        market,
        product,
        block,
        spark_location,
        price_type,
        units_raw,
        currency,
        per_unit,
        tenor_type,
        contract_month,
        tenor_label,
        value,
        bid,
        ask,
        mid,
        curve_key,
        version_hash,
        _ingest_ts,
        ROW_NUMBER() OVER (
            PARTITION BY curve_key, tenor_label
            ORDER BY asof_date DESC, _ingest_ts DESC
        ) AS rn
    FROM iceberg.market.curve_observation
)
SELECT
    asof_date,
    source_file,
    sheet_name,
    asset_class,
    region,
    iso,
    location,
    market,
    product,
    block,
    spark_location,
    price_type,
    units_raw,
    currency,
    per_unit,
    tenor_type,
    contract_month,
    tenor_label,
    value,
    bid,
    ask,
    mid,
    curve_key,
    version_hash,
    _ingest_ts
FROM ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW iceberg.market.scenario_output_latest AS
WITH ranked AS (
    SELECT
        asof_date,
        scenario_id,
        tenant_id,
        run_id,
        curve_key,
        tenor_type,
        contract_month,
        tenor_label,
        metric,
        value,
        band_lower,
        band_upper,
        attribution,
        version_hash,
        computed_ts,
        _ingest_ts,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_id, scenario_id, curve_key, metric, tenor_label
            ORDER BY asof_date DESC, computed_ts DESC
        ) AS rn
    FROM iceberg.market.scenario_output
)
SELECT
    asof_date,
    scenario_id,
    tenant_id,
    run_id,
    curve_key,
    tenor_type,
    contract_month,
    tenor_label,
    metric,
    value,
    band_lower,
    band_upper,
    attribution,
    version_hash,
    computed_ts,
    _ingest_ts
FROM ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW iceberg.market.scenario_output_by_curve AS
SELECT
    scenario_id,
    tenant_id,
    curve_key,
    metric,
    tenor_label,
    max(asof_date) AS latest_asof_date,
    max_by(value, computed_ts) AS latest_value,
    max_by(band_lower, computed_ts) AS latest_band_lower,
    max_by(band_upper, computed_ts) AS latest_band_upper,
    max_by(run_id, computed_ts) AS latest_run_id
FROM iceberg.market.scenario_output
GROUP BY scenario_id, tenant_id, curve_key, metric, tenor_label;

CREATE OR REPLACE VIEW iceberg.market.scenario_output_latest_by_metric AS
WITH ranked AS (
    SELECT
        tenant_id,
        scenario_id,
        metric,
        run_id,
        value,
        band_lower,
        band_upper,
        asof_date,
        computed_ts,
        row_number() OVER (
            PARTITION BY tenant_id, scenario_id, metric
            ORDER BY asof_date DESC, computed_ts DESC
        ) AS rn
    FROM iceberg.market.scenario_output
)
SELECT
    tenant_id,
    scenario_id,
    metric,
    run_id AS latest_run_id,
    value AS latest_value,
    band_lower AS latest_band_lower,
    band_upper AS latest_band_upper,
    asof_date AS latest_asof_date,
    computed_ts AS latest_computed_ts
FROM ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW iceberg.market.maintenance_snapshot_summary AS
SELECT
    'raw.curve_landing' AS table_name,
    snapshot_id,
    committed_at,
    operation,
    TRY_CAST(summary['total-data-files'] AS BIGINT) AS total_data_files,
    TRY_CAST(summary['added-data-files'] AS BIGINT) AS added_data_files,
    TRY_CAST(summary['deleted-data-files'] AS BIGINT) AS deleted_data_files,
    TRY_CAST(summary['total-records'] AS BIGINT) AS total_records
FROM iceberg.raw.curve_landing$snapshots
UNION ALL
SELECT
    'curve_observation' AS table_name,
    snapshot_id,
    committed_at,
    operation,
    TRY_CAST(summary['total-data-files'] AS BIGINT) AS total_data_files,
    TRY_CAST(summary['added-data-files'] AS BIGINT) AS added_data_files,
    TRY_CAST(summary['deleted-data-files'] AS BIGINT) AS deleted_data_files,
    TRY_CAST(summary['total-records'] AS BIGINT) AS total_records
FROM iceberg.market.curve_observation$snapshots
UNION ALL
SELECT
    'curve_observation_quarantine' AS table_name,
    snapshot_id,
    committed_at,
    operation,
    TRY_CAST(summary['total-data-files'] AS BIGINT) AS total_data_files,
    TRY_CAST(summary['added-data-files'] AS BIGINT) AS added_data_files,
    TRY_CAST(summary['deleted-data-files'] AS BIGINT) AS deleted_data_files,
    TRY_CAST(summary['total-records'] AS BIGINT) AS total_records
FROM iceberg.market.curve_observation_quarantine$snapshots
UNION ALL
SELECT
    'scenario_output' AS table_name,
    snapshot_id,
    committed_at,
    operation,
    TRY_CAST(summary['total-data-files'] AS BIGINT) AS total_data_files,
    TRY_CAST(summary['added-data-files'] AS BIGINT) AS added_data_files,
    TRY_CAST(summary['deleted-data-files'] AS BIGINT) AS deleted_data_files,
    TRY_CAST(summary['total-records'] AS BIGINT) AS total_records
FROM iceberg.market.scenario_output$snapshots
UNION ALL
SELECT
    'ppa_valuation' AS table_name,
    snapshot_id,
    committed_at,
    operation,
    TRY_CAST(summary['total-data-files'] AS BIGINT) AS total_data_files,
    TRY_CAST(summary['added-data-files'] AS BIGINT) AS added_data_files,
    TRY_CAST(summary['deleted-data-files'] AS BIGINT) AS deleted_data_files,
    TRY_CAST(summary['total-records'] AS BIGINT) AS total_records
FROM iceberg.market.ppa_valuation$snapshots
UNION ALL
SELECT
    'qa_checks' AS table_name,
    snapshot_id,
    committed_at,
    operation,
    TRY_CAST(summary['total-data-files'] AS BIGINT) AS total_data_files,
    TRY_CAST(summary['added-data-files'] AS BIGINT) AS added_data_files,
    TRY_CAST(summary['deleted-data-files'] AS BIGINT) AS deleted_data_files,
    TRY_CAST(summary['total-records'] AS BIGINT) AS total_records
FROM iceberg.market.qa_checks$snapshots;

CREATE OR REPLACE VIEW iceberg.market.maintenance_file_metrics AS
WITH files AS (
    SELECT 'raw.curve_landing' AS table_name, file_size_in_bytes
    FROM iceberg.raw.curve_landing$files
    UNION ALL
    SELECT 'curve_observation' AS table_name, file_size_in_bytes
    FROM iceberg.market.curve_observation$files
    UNION ALL
    SELECT 'curve_observation_quarantine' AS table_name, file_size_in_bytes
    FROM iceberg.market.curve_observation_quarantine$files
    UNION ALL
    SELECT 'scenario_output' AS table_name, file_size_in_bytes
    FROM iceberg.market.scenario_output$files
    UNION ALL
    SELECT 'ppa_valuation' AS table_name, file_size_in_bytes
    FROM iceberg.market.ppa_valuation$files
    UNION ALL
    SELECT 'qa_checks' AS table_name, file_size_in_bytes
    FROM iceberg.market.qa_checks$files
)
SELECT
    table_name,
    COUNT(*) AS data_file_count,
    ROUND(SUM(file_size_in_bytes) / 1048576.0, 2) AS total_size_mb,
    ROUND(AVG(file_size_in_bytes) / 1048576.0, 2) AS avg_file_size_mb,
    ROUND(MAX(file_size_in_bytes) / 1048576.0, 2) AS max_file_size_mb,
    ROUND(MIN(file_size_in_bytes) / 1048576.0, 2) AS min_file_size_mb
FROM files
GROUP BY table_name;
