from __future__ import annotations

from pathlib import Path
import re


def test_latest_views_declared() -> None:
    sql = Path("trino/ddl/iceberg_market.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS iceberg.raw.curve_landing" in sql
    assert "CREATE TABLE IF NOT EXISTS iceberg.market.curve_observation_quarantine" in sql
    assert "CREATE OR REPLACE VIEW iceberg.market.curve_observation_latest" in sql
    assert "CREATE OR REPLACE VIEW iceberg.market.scenario_output_latest" in sql
    assert "CREATE OR REPLACE VIEW iceberg.market.maintenance_snapshot_summary" in sql
    assert "CREATE OR REPLACE VIEW iceberg.market.maintenance_file_metrics" in sql


def test_curve_observation_properties() -> None:
    sql = Path("trino/ddl/iceberg_market.sql").read_text(encoding="utf-8")
    match = re.search(r"CREATE TABLE IF NOT EXISTS iceberg\.market\.curve_observation \((?:.|\n)*?WITH \((.*?)\);", sql, re.DOTALL)
    assert match, 'curve_observation table definition missing'
    props = match.group(1)
    assert "format_version = '2'" in props
    assert "write_compression = 'ZSTD'" in props
    assert "partitioning = ARRAY['days(asof_date)']" in props
    assert "write_sort_order = ARRAY['asof_date', 'asset_class', 'iso', 'tenor_label']" in props
    assert "vacuum_max_snapshot_age_retention = '7d'" in props
    assert "optimize_rewrite_delete_file_threshold = 250" in props


def test_curve_observation_quarantine_partitioning() -> None:
    sql = Path("trino/ddl/iceberg_market.sql").read_text(encoding="utf-8")
    match = re.search(r"CREATE TABLE IF NOT EXISTS iceberg\.market\.curve_observation_quarantine \((?:.|\n)*?WITH \((.*?)\);", sql, re.DOTALL)
    assert match, 'curve_observation_quarantine table definition missing'
    props = match.group(1)
    assert "partitioning = ARRAY['days(asof_date)']" in props
    assert "format_version = '2'" in props


def test_curve_observation_columns() -> None:
    sql = Path("trino/ddl/iceberg_market.sql").read_text(encoding="utf-8")
    match = re.search(r"CREATE TABLE IF NOT EXISTS iceberg\.market\.curve_observation \((.*?)\)\nWITH ", sql, re.DOTALL)
    assert match, 'curve_observation column block missing'
    column_block = [line.strip() for line in match.group(1).strip().splitlines() if line.strip()]
    columns = [line.split()[0] for line in column_block]
    assert columns == [
        'asof_date',
        'source_file',
        'sheet_name',
        'asset_class',
        'region',
        'iso',
        'location',
        'market',
        'product',
        'block',
        'spark_location',
        'price_type',
        'units_raw',
        'currency',
        'per_unit',
        'tenor_type',
        'contract_month',
        'tenor_label',
        'value',
        'bid',
        'ask',
        'mid',
        'curve_key',
        'version_hash',
        '_ingest_ts',
    ]
