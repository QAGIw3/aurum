from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List


def _curves_asof(dataset: List[Dict[str, object]], tenant_id: str) -> List[Dict[str, object]]:
    return [
        {
            "tenant_id": row["tenant_id"],
            "curve_key": row["curve_key"],
            "contract_month": row["contract_month"],
            "asof_date": row["asof_date"],
            "mid": row["mid"],
            "bid": row["bid"],
            "ask": row["ask"],
        }
        for row in dataset
        if row["tenant_id"] == tenant_id
    ]


def _curves_asof_diff(dataset: List[Dict[str, object]], tenant_id: str) -> List[Dict[str, object]]:
    filtered = [row for row in dataset if row["tenant_id"] == tenant_id]
    index = {
        (row["curve_key"], row["contract_month"], row["asof_date"]): row
        for row in filtered
    }

    diffs: List[Dict[str, object]] = []
    for row in filtered:
        prior_key = (
            row["curve_key"],
            row["contract_month"],
            row["asof_date"] - timedelta(days=1),
        )
        previous = index.get(prior_key)
        if previous is None:
            continue
        diffs.append(
            {
                "tenant_id": tenant_id,
                "curve_key": row["curve_key"],
                "contract_month": row["contract_month"],
                "asof_date_new": row["asof_date"],
                "mid_new": row["mid"],
                "asof_date_old": previous["asof_date"],
                "mid_old": previous["mid"],
                "mid_diff": row["mid"] - previous["mid"],
            }
        )
    diffs.sort(key=lambda entry: (entry["curve_key"], entry["contract_month"], entry["asof_date_new"]))
    return diffs


def test_market_api_views_include_tenant_id() -> None:
    sql = Path("trino/ddl/api_views.sql").read_text(encoding="utf-8")
    assert "SELECT\n    tenant_id,\n    curve_key" in sql
    assert "JOIN base b2\n      ON b1.tenant_id = b2.tenant_id" in sql


def test_curves_asof_golden_rows_match() -> None:
    dataset = [
        {
            "tenant_id": "tenant-a",
            "curve_key": "curve-x",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 1),
            "mid": 100.0,
            "bid": 99.5,
            "ask": 100.5,
        },
        {
            "tenant_id": "tenant-a",
            "curve_key": "curve-x",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 2),
            "mid": 101.25,
            "bid": 100.75,
            "ask": 101.75,
        },
        {
            "tenant_id": "tenant-b",
            "curve_key": "curve-y",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 2),
            "mid": 88.0,
            "bid": 87.5,
            "ask": 88.5,
        },
    ]

    expected = [
        {
            "tenant_id": "tenant-a",
            "curve_key": "curve-x",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 1),
            "mid": 100.0,
            "bid": 99.5,
            "ask": 100.5,
        },
        {
            "tenant_id": "tenant-a",
            "curve_key": "curve-x",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 2),
            "mid": 101.25,
            "bid": 100.75,
            "ask": 101.75,
        },
    ]

    result = _curves_asof(dataset, tenant_id="tenant-a")
    assert result == expected


def test_curves_asof_diff_golden_rows_match() -> None:
    dataset = [
        {
            "tenant_id": "tenant-a",
            "curve_key": "curve-x",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 1),
            "mid": 100.0,
        },
        {
            "tenant_id": "tenant-a",
            "curve_key": "curve-x",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 2),
            "mid": 101.5,
        },
        {
            "tenant_id": "tenant-a",
            "curve_key": "curve-x",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 3),
            "mid": 99.0,
        },
        {
            "tenant_id": "tenant-b",
            "curve_key": "curve-y",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 2),
            "mid": 77.0,
        },
    ]

    expected = [
        {
            "tenant_id": "tenant-a",
            "curve_key": "curve-x",
            "contract_month": date(2024, 1, 1),
            "asof_date_new": date(2024, 1, 2),
            "mid_new": 101.5,
            "asof_date_old": date(2024, 1, 1),
            "mid_old": 100.0,
            "mid_diff": 1.5,
        },
        {
            "tenant_id": "tenant-a",
            "curve_key": "curve-x",
            "contract_month": date(2024, 1, 1),
            "asof_date_new": date(2024, 1, 3),
            "mid_new": 99.0,
            "asof_date_old": date(2024, 1, 2),
            "mid_old": 101.5,
            "mid_diff": -2.5,
        },
    ]

    result = _curves_asof_diff(dataset, tenant_id="tenant-a")
    assert result == expected
