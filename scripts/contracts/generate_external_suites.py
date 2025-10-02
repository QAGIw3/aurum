#!/usr/bin/env python3
"""Generate external-domain GE suites from fragments.

Builds:
- external_timeseries_obs.json
- external_series_catalog.json
- external_obs_conformed.json
"""
from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
FRAG = REPO_ROOT / "libs/contracts/ge/fragments/external"
OUT = REPO_ROOT / "libs/contracts/ge/expectations"


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_pretty(path: Path, data: dict):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def build_external_timeseries_obs() -> dict:
    providers = _read_json(FRAG / "provider_set.json")
    cols = _read_json(FRAG / "timeseries_columns.json")
    required = _read_json(FRAG / "timeseries_required.json")
    exps = [
        {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": cols}},
    ]
    for col in required:
        exps.append({"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": col}})

    exps.extend([
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "provider", "value_set": providers},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "ts", "min_value": "1900-01-01 00:00:00", "max_value": "2030-12-31 23:59:59"},
            "meta": {"notes": "Observation timestamps should be within historical range"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "asof_date", "min_value": "1900-01-01", "max_value": "2030-12-31"},
            "meta": {"notes": "As-of dates should be within historical range"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "ingest_ts", "min_value": "2024-01-01 00:00:00", "max_value": "2025-12-31 23:59:59"},
            "meta": {"notes": "Ingest timestamps should be recent"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "value", "min_value": -1000000, "max_value": 1000000},
            "meta": {"notes": "Values should be within plausible numeric range"},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "value"},
            "mostly": 0.9,
            "meta": {"notes": "Most observations should have values (90% threshold)"},
        },
    ])

    return {
        "expectation_suite_name": "external_timeseries_obs",
        "meta": {"notes": "Validates external timeseries observations from Kafka to Iceberg."},
        "expectations": exps,
    }


def build_external_series_catalog() -> dict:
    providers = _read_json(FRAG / "provider_set.json")
    cols = _read_json(FRAG / "catalog_columns.json")
    required = _read_json(FRAG / "catalog_required.json")
    exps = [
        {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": cols}},
    ]
    for col in required:
        exps.append({"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": col}})
    exps.append({"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "provider", "value_set": providers}})
    exps.extend([
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "version"},
            "meta": {"notes": "Version required for optimistic concurrency"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "version", "min_value": 1, "max_value": 1000000},
            "meta": {"notes": "Version should be a reasonable positive number"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "ingest_ts", "min_value": "2024-01-01 00:00:00", "max_value": "2025-12-31 23:59:59"},
            "meta": {"notes": "Ingest timestamps should be within reasonable date range"},
        },
    ])
    return {
        "expectation_suite_name": "external_series_catalog",
        "meta": {"notes": "Validates external series catalog data from Kafka to Iceberg."},
        "expectations": exps,
    }


def build_external_obs_conformed() -> dict:
    enums = _read_json(FRAG / "conformed_enums.json")
    required = [
        "provider",
        "series_id",
        "ts_utc",
        "asof_date",
        "value_usd_per_mwh",
        "frequency_code_normalized",
        "unit_code_canonical",
        "quality_status",
        "conformed_at",
    ]
    exps = []
    for col in required:
        exps.append({"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": col}})
    exps.extend([
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "frequency_code_normalized", "value_set": enums["frequency_code_normalized"]},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "unit_code_canonical", "value_set": enums["unit_code_canonical"]},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {"column": "quality_status", "value_set": enums["quality_status"]},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "value_usd_per_mwh", "min_value": -10000, "max_value": 10000},
            "meta": {"notes": "Standardized values should be in plausible USD/MWh range"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "ts_utc", "min_value": "1900-01-01 00:00:00", "max_value": "2030-12-31 23:59:59"},
            "meta": {"notes": "UTC timestamps should be within historical range"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "asof_date", "min_value": "1900-01-01", "max_value": "2030-12-31"},
            "meta": {"notes": "As-of dates should be within historical range"},
        },
    ])
    return {
        "expectation_suite_name": "external_obs_conformed",
        "meta": {"notes": "Validates conformed external observations after standardization."},
        "expectations": exps,
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    suites = {
        "external_timeseries_obs.json": build_external_timeseries_obs(),
        "external_series_catalog.json": build_external_series_catalog(),
        "external_obs_conformed.json": build_external_obs_conformed(),
    }
    for name, data in suites.items():
        write_pretty(OUT / name, data)
    print(f"Generated {len(suites)} external suites → {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

