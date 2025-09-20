from __future__ import annotations

from pathlib import Path


def test_latest_views_declared() -> None:
    sql = Path("trino/ddl/iceberg_market.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE VIEW iceberg.market.curve_observation_latest" in sql
    assert "CREATE OR REPLACE VIEW iceberg.market.scenario_output_latest" in sql
