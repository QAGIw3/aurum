CREATE SCHEMA IF NOT EXISTS iceberg.market;

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
    _ingest_ts TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year(asof_date)', 'month(asof_date)']
);

CREATE TABLE IF NOT EXISTS iceberg.market.scenario_output (
    asof_date DATE,
    scenario_id VARCHAR,
    tenant_id VARCHAR,
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
    partitioning = ARRAY['scenario_id', 'year(asof_date)']
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
    partitioning = ARRAY['ppa_contract_id', 'year(asof_date)']
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
    partitioning = ARRAY['year(asof_date)']
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
