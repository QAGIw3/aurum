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
    curve_key VARCHAR,
    tenor_type VARCHAR,
    contract_month DATE,
    tenor_label VARCHAR,
    metric VARCHAR,
    value DOUBLE,
    band_lower DOUBLE,
    band_upper DOUBLE,
    attribution MAP(VARCHAR, DOUBLE),
    version_hash VARCHAR,
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
