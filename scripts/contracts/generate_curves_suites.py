#!/usr/bin/env python3
"""Generate curves-domain GE suites from fragments.

Outputs overwrite canonical suites in libs/contracts/ge/expectations and remain
content-compatible with existing definitions.
"""
from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FRAG = REPO_ROOT / "libs/contracts/ge/fragments/curves"
OUT = REPO_ROOT / "libs/contracts/ge/expectations"


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_pretty(path: Path, data: dict):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def build_curve_schema() -> dict:
    columns = _read_json(FRAG / "canonical_columns.json")
    tenor_types = _read_json(FRAG / "tenor_types_schema_regex.json")
    regex = "^(" + "|".join(tenor_types) + ")$"
    return {
        "expectation_suite_name": "curve_schema",
        "meta": {
            "notes": "Ensures canonical curve schema is present after parsing vendor workbooks."
        },
        "expectations": [
            {
                "expectation_type": "expect_table_columns_to_match_set",
                "kwargs": {"column_set": columns},
            },
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "curve_key"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "tenor_label"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "currency"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "per_unit"}},
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {"column": "tenor_type", "regex": regex},
            },
        ],
    }


def build_curve_landing() -> dict:
    price_types = _read_json(FRAG / "price_types.json")
    tenor_types = _read_json(FRAG / "tenor_types_landing.json")
    return {
        "expectation_suite_name": "curve_landing",
        "meta": {
            "notes": "Validates enriched canonical curve rows prior to persistence."
        },
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "curve_key"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "currency"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "per_unit"}},
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "price_type", "value_set": price_types},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "tenor_type", "value_set": tenor_types},
            },
        ],
    }


def build_publish_curve_observation() -> dict:
    columns = _read_json(FRAG / "publish_columns.json")
    return {
        "data_asset_name": "publish_curve_observation",
        "expectations": [
            {
                "expectation_type": "expect_table_columns_to_match_set",
                "kwargs": {"column_set": columns},
            },
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "curve_key"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "tenor_label"}},
            {
                "expectation_type": "expect_multicolumn_values_to_be_unique",
                "kwargs": {"column_list": ["curve_key", "tenor_label", "asof_date"]},
            },
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "mid"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "currency"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "per_unit"}},
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "lineage_tags",
                    "regex": r"^source=iceberg\.raw\.curve_landing",
                    "meta": {"description": "Ensure lineage tags capture raw source provenance"},
                },
            },
        ],
    }


def build_fct_curve_observation() -> dict:
    price_types = _read_json(FRAG / "price_types.json")
    return {
        "expectation_suite_name": "fct_curve_observation",
        "meta": {
            "notes": "Checks for canonical curve fact table ensuring grain and value sanity."
        },
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "curve_key"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "tenor_label"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "asof_date"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_code", "mostly": 0.99}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "mid"}},
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "mid", "mostly": 0.995, "min_value": -10000, "max_value": 10000},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "price_type", "value_set": price_types, "mostly": 0.99},
            },
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "lineage_tags"}},
            {
                "expectation_type": "expect_compound_columns_to_be_unique",
                "kwargs": {"column_list": ["curve_key", "tenor_label", "asof_date"]},
            },
        ],
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    suites = {
        "curve_schema.json": build_curve_schema(),
        "curve_landing.json": build_curve_landing(),
        "publish_curve_observation.json": build_publish_curve_observation(),
        "fct_curve_observation.json": build_fct_curve_observation(),
    }
    # Optional: generate a JSON variant of curve_business rules (Tier A)
    suites["curve_business.json"] = {
        "expectation_suite_name": "curve_business",
        "meta": {"notes": "Business rules capturing relationships between bid, ask, and mid prices."},
        "expectations": [
            {
                "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                "kwargs": {"column_A": "ask", "column_B": "bid", "or_equal": True},
            },
            {
                "expectation_type": "expect_multicolumn_values_to_be_unique",
                "kwargs": {"column_list": ["curve_key", "tenor_label", "asof_date"]},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "mid", "min_value": 0, "mostly": 0.95},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "asof_date", "min_value": "2015-01-01"},
            },
        ],
    }
    for name, data in suites.items():
        write_pretty(OUT / name, data)
    print(f"Generated {len(suites)} curves suites → {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
