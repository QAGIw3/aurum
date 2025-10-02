#!/usr/bin/env python3
"""Generate mart-domain GE suites from fragments.

Builds:
- mart_curve_latest.json
- mart_curve_asof_diff.json
- mart_cpi_series_latest.json
- mart_eia_series_latest.json
- mart_fred_series_latest.json
- mart_series_curve_mapping.json
- mart_external_series_catalog.json
- mart_scenario_output.json
"""
from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
FR_CURVES = REPO_ROOT / "libs/contracts/ge/fragments/curves"
FR_MART = REPO_ROOT / "libs/contracts/ge/fragments/mart"
OUT = REPO_ROOT / "libs/contracts/ge/expectations"


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_pretty(path: Path, data: dict):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def build_mart_curve_latest() -> dict:
    tenor_types = _read_json(FR_CURVES / "tenor_types_landing.json")
    return {
        "expectation_suite_name": "mart_curve_latest",
        "meta": {"notes": "Quality checks for the mart_curve_latest view."},
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "curve_key"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "tenor_label"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "mid"}},
            {"expectation_type": "expect_compound_columns_to_be_unique", "kwargs": {"column_list": ["curve_key", "tenor_label"]}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "tenor_type", "value_set": tenor_types}},
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "mid", "mostly": 0.99, "min_value": -10000, "max_value": 10000}},
        ],
    }


def build_mart_curve_asof_diff() -> dict:
    return {
        "expectation_suite_name": "mart_curve_asof_diff",
        "meta": {"notes": "Sanity checks for pre-computed as-of curve deltas."},
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "current_asof_date"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "compare_asof_date"}},
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "delta_mid", "mostly": 0.995, "min_value": -20000, "max_value": 20000}},
        ],
    }


def build_mart_cpi_series_latest() -> dict:
    return {
        "expectation_suite_name": "mart_cpi_series_latest",
        "meta": {"notes": "Validates mart layer for latest CPI observations."},
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "tenant_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "series_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "period"}},
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "value", "min_value": 0, "max_value": 1000, "mostly": 0.99}},
        ],
    }


def build_mart_eia_series_latest() -> dict:
    freq = _read_json(FR_MART / "frequency_codes.json")
    return {
        "expectation_suite_name": "mart_eia_series_latest",
        "meta": {"notes": "Quality checks for the mart_eia_series_latest view."},
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "tenant_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "series_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "value"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "unit_normalized", "mostly": 0.95}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "currency_normalized", "mostly": 0.95}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "frequency", "value_set": freq}},
        ],
    }


def build_mart_fred_series_latest() -> dict:
    return {
        "expectation_suite_name": "mart_fred_series_latest",
        "meta": {"notes": "Validates mart layer for latest FRED observations."},
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "tenant_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "series_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "value"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "unit_normalized", "mostly": 0.95}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "currency_normalized", "mostly": 0.95}},
        ],
    }


def build_mart_series_curve_mapping() -> dict:
    return {
        "expectation_suite_name": "mart_series_curve_mapping",
        "meta": {"notes": "Quality checks for series to curve mapping mart."},
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "external_provider"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "external_series_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "curve_key"}},
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "mapping_confidence", "min_value": 0, "max_value": 1}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "mapping_method", "value_set": ["manual", "automated", "heuristic"]}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "is_active"}},
            {"expectation_type": "expect_multicolumn_values_to_be_unique", "kwargs": {"column_list": ["external_provider", "external_series_id", "curve_key"]}},
        ],
    }


def build_mart_external_series_catalog() -> dict:
    freq = _read_json(FR_MART / "frequency_codes.json")
    statuses = _read_json(FR_MART / "status_codes.json")
    iso_markets = _read_json(FR_MART / "iso_markets.json")
    return {
        "expectation_suite_name": "mart_external_series_catalog",
        "meta": {"notes": "Data quality checks for the external series catalog mart. Validates canonical ISO fields (iso_code/market/product/location/timezone/interval/unit/subject/curve_role) are present or have UNKNOWN fallback; mapping_status is constrained."},
        "expectations": [
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "status", "value_set": statuses, "mostly": 0.95}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "frequency", "value_set": freq}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "mapping_status", "value_set": _read_json(FR_MART / "mapping_status.json")}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "iso_market", "value_set": iso_markets}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_code"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_product"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_location_type"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_location_name"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_location_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_timezone"}},
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "iso_interval_minutes", "min_value": 0, "max_value": 1440}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_unit"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_subject"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_curve_role"}},
            {"expectation_type": "expect_multicolumn_values_to_be_unique", "kwargs": {"column_list": ["provider", "series_id"]}},
        ],
    }


def build_mart_scenario_output() -> dict:
    return {
        "expectation_suite_name": "mart_scenario_output",
        "meta": {"notes": "Quality checks for the mart_scenario_output view."},
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "scenario_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "tenant_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "metric"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "value"}},
            {"expectation_type": "expect_compound_columns_to_be_unique", "kwargs": {"column_list": ["tenant_id", "scenario_id", "run_id", "metric", "tenor_label"]}},
            {"expectation_type": "expect_column_proportion_of_nonnulls_to_be_between", "kwargs": {"column": "band_lower", "min_value": 0.8, "max_value": 1.0}},
            {"expectation_type": "expect_column_proportion_of_nonnulls_to_be_between", "kwargs": {"column": "band_upper", "min_value": 0.8, "max_value": 1.0}},
        ],
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    suites = {
        "mart_curve_latest.json": build_mart_curve_latest(),
        "mart_curve_asof_diff.json": build_mart_curve_asof_diff(),
        "mart_cpi_series_latest.json": build_mart_cpi_series_latest(),
        "mart_eia_series_latest.json": build_mart_eia_series_latest(),
        "mart_fred_series_latest.json": build_mart_fred_series_latest(),
        "mart_series_curve_mapping.json": build_mart_series_curve_mapping(),
        "mart_external_series_catalog.json": build_mart_external_series_catalog(),
        "mart_scenario_output.json": build_mart_scenario_output(),
    }
    for name, data in suites.items():
        write_pretty(OUT / name, data)
    print(f"Generated {len(suites)} mart suites → {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

