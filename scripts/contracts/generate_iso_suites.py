#!/usr/bin/env python3
"""Generate ISO-domain GE suites from fragments.

Builds:
- iso_lmp.json
- iso_lmp_raw.json
- iso_genmix_raw.json
- iso_load_raw.json
"""
from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
FRAG = REPO_ROOT / "libs/contracts/ge/fragments/iso"
OUT = REPO_ROOT / "libs/contracts/ge/expectations"


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_pretty(path: Path, data: dict):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def build_iso_lmp() -> dict:
    cols = _read_json(FRAG / "lmp_columns.json")
    iso_codes = _read_json(FRAG / "iso_codes.json")
    currency = ["USD", "CAD"]
    uom = ["MWh"]
    exps = [
        {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": cols}},
        {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "iso_code", "value_set": iso_codes}},
        {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "market", "value_set": ["DAY_AHEAD", "REAL_TIME", "FIFTEEN_MINUTE", "FIVE_MINUTE", "HOUR_AHEAD", "SETTLEMENT", "UNKNOWN"]}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "interval_start"}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "record_hash"}},
        {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "price_total", "min_value": -1000, "max_value": 10000, "mostly": 0.99}},
        {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "price_energy", "min_value": -1000, "max_value": 10000, "mostly": 0.98}},
        {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "price_congestion", "min_value": -1000, "max_value": 10000, "mostly": 0.98}},
        {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "price_loss", "min_value": -1000, "max_value": 10000, "mostly": 0.98}},
        {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "interval_minutes", "min_value": 0, "max_value": 180, "mostly": 0.95}},
        {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "currency", "value_set": currency}},
        {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "uom", "value_set": uom}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ingest_ts"}},
        {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "record_hash"}},
    ]
    return {"expectation_suite_name": "iso_lmp", "meta": {"notes": "Data quality checks for normalized ISO LMP observations pushed to Kafka."}, "expectations": exps}


def build_iso_lmp_raw() -> dict:
    cols = _read_json(FRAG / "lmp_raw_columns.json")
    timezones = _read_json(FRAG / "timezones.json")
    exps = [
        {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": cols}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_code", "mostly": 0.99}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "interval_start", "mostly": 0.999}},
        {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "price_total", "min_value": -10000, "max_value": 10000, "mostly": 0.995}},
        {"expectation_type": "expect_compound_columns_to_be_unique", "kwargs": {"column_list": ["iso_code", "location_id", "interval_start", "market"]}},
        {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "timezone", "value_set": timezones, "mostly": 0.95}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ingest_ts", "mostly": 1.0}},
    ]
    return {"expectation_suite_name": "iso_lmp_raw", "expectations": exps}


def build_iso_genmix_raw() -> dict:
    cols = _read_json(FRAG / "genmix_raw_columns.json")
    exps = [
        {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": cols}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_code", "mostly": 0.99}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "asof_time", "mostly": 0.999}},
        {"expectation_type": "expect_compound_columns_to_be_unique", "kwargs": {"column_list": ["iso_code", "fuel_type", "asof_time"]}},
        {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "mw", "min_value": 0, "max_value": 1000000, "mostly": 0.999}},
        {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "unit", "value_set": ["MW"], "mostly": 0.99}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ingest_ts", "mostly": 1.0}},
    ]
    return {"expectation_suite_name": "iso_genmix_raw", "expectations": exps}


def build_iso_load_raw() -> dict:
    cols = _read_json(FRAG / "load_raw_columns.json")
    timezones = _read_json(FRAG / "timezones.json")
    exps = [
        {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": cols}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "iso_code", "mostly": 0.99}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "interval_start", "mostly": 0.999}},
        {"expectation_type": "expect_compound_columns_to_be_unique", "kwargs": {"column_list": ["iso_code", "location_id", "interval_start"]}},
        {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "timezone", "value_set": timezones, "mostly": 0.95}},
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ingest_ts", "mostly": 1.0}},
    ]
    return {"expectation_suite_name": "iso_load_raw", "expectations": exps}


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    suites = {
        "iso_lmp.json": build_iso_lmp(),
        "iso_lmp_raw.json": build_iso_lmp_raw(),
        "iso_genmix_raw.json": build_iso_genmix_raw(),
        "iso_load_raw.json": build_iso_load_raw(),
    }
    for name, data in suites.items():
        write_pretty(OUT / name, data)
    print(f"Generated {len(suites)} ISO suites → {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

