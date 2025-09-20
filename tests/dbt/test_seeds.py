from __future__ import annotations

import csv
from pathlib import Path


def _load_headers(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        try:
            headers = next(reader)
        except StopIteration:  # pragma: no cover - empty seed should fail test
            return []
    return headers


def test_scenario_metric_seed_columns() -> None:
    seed_path = Path("dbt/seeds/scenario_metric_catalog.csv")
    assert seed_path.exists()
    headers = _load_headers(seed_path)
    assert headers == ["metric", "description", "unit"]


def test_tenant_catalog_seed_columns() -> None:
    seed_path = Path("dbt/seeds/tenant_catalog.csv")
    assert seed_path.exists()
    headers = _load_headers(seed_path)
    assert headers == ["tenant_id", "tenant_name"]
