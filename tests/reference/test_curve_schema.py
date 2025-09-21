from __future__ import annotations

import json
from pathlib import Path

from aurum.reference.curve_schema import (
    CANONICAL_CURVE_COLUMNS,
    CURVE_IDENTITY_FIELDS,
    CurveAssetClass,
    CurvePriceType,
    CurveTenorType,
)


def test_canonical_curve_columns_match_expected() -> None:
    assert CANONICAL_CURVE_COLUMNS == (
        "asof_date",
        "source_file",
        "sheet_name",
        "asset_class",
        "region",
        "iso",
        "location",
        "market",
        "product",
        "block",
        "spark_location",
        "price_type",
        "units_raw",
        "currency",
        "per_unit",
        "tenor_type",
        "contract_month",
        "tenor_label",
        "value",
        "bid",
        "ask",
        "mid",
        "curve_key",
        "version_hash",
        "_ingest_ts",
    )


def test_curve_enums_cover_core_values() -> None:
    asset_classes = {item.value for item in CurveAssetClass}
    assert {"power", "renewable"}.issubset(asset_classes)

    price_types = {item.value for item in CurvePriceType}
    assert {"MID", "BID", "ASK"}.issubset(price_types)

    tenor_types = {item.value for item in CurveTenorType}
    assert {"MONTHLY", "QUARTER", "SEASON", "CALENDAR"}.issubset(tenor_types)


def test_curve_identity_fields_match_compute_order() -> None:
    assert CURVE_IDENTITY_FIELDS == (
        "asset_class",
        "iso",
        "region",
        "location",
        "market",
        "product",
        "block",
        "spark_location",
    )


def test_curve_landing_expectation_suite_values() -> None:
    suite_path = Path("ge/expectations/curve_landing.json")
    suite = json.loads(suite_path.read_text())
    expectations = {item["kwargs"]["column"]: item for item in suite.get("expectations", [])}
    assert set(expectations.keys()).issuperset({"curve_key", "currency", "per_unit", "price_type", "tenor_type"})
