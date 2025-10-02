#!/usr/bin/env python3
"""Generate drought-domain GE suites from fragments.

Builds:
- drought_index.json
- drought_usdm_area.json
- drought_vector_event.json
"""
from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
FR = REPO_ROOT / "libs/contracts/ge/fragments/drought"
OUT = REPO_ROOT / "libs/contracts/ge/expectations"


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_pretty(path: Path, data: dict):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def build_drought_index() -> dict:
    cols = _read_json(FR / "index_columns.json")
    enums = _read_json(FR / "index_datasets.json")
    region_types = _read_json(FR / "region_types.json")
    return {
        "expectation_suite_name": "drought_index",
        "meta": {"notes": "Contracts for drought index zonal statistics ingested from raster pipelines."},
        "expectations": [
            {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": cols}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "series_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "valid_date"}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "region_type", "value_set": region_types}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "dataset", "value_set": enums["dataset"]}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "index", "value_set": enums["index"]}},
            {"expectation_type": "expect_column_values_to_match_regex", "kwargs": {"column": "timescale", "regex": r"^(\\\\d+[MW])$"}},
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "value", "min_value": -10.0, "max_value": 10.0, "mostly": 0.99}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ingest_ts"}},
        ],
    }


def build_drought_usdm_area() -> dict:
    cols = _read_json(FR / "usdm_columns.json")
    return {
        "expectation_suite_name": "drought_usdm_area",
        "meta": {"notes": "Weekly USDM area fraction checks by geography."},
        "expectations": [
            {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": cols}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "region_type"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "region_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "valid_date"}},
            *[
                {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": col, "min_value": 0, "max_value": 1, "mostly": 0.999}}
                for col in ["d0_frac", "d1_frac", "d2_frac", "d3_frac", "d4_frac"]
            ],
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ingest_ts"}},
        ],
    }


def build_drought_vector_event() -> dict:
    cols = _read_json(FR / "vector_event_columns.json")
    layers = _read_json(FR / "vector_layers.json")
    return {
        "expectation_suite_name": "drought_vector_event",
        "meta": {"notes": "Structure checks for vector overlay events pushed from Airflow ingestion."},
        "expectations": [
            {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": cols}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "layer"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "event_id"}},
            {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "layer", "value_set": layers}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ingest_ts"}},
            {"expectation_type": "expect_column_values_to_match_regex", "kwargs": {"column": "geometry_wkt", "regex": r"^(POINT|LINESTRING|POLYGON|MULTI).*", "mostly": 0.95}},
        ],
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    suites = {
        "drought_index.json": build_drought_index(),
        "drought_usdm_area.json": build_drought_usdm_area(),
        "drought_vector_event.json": build_drought_vector_event(),
    }
    for name, data in suites.items():
        write_pretty(OUT / name, data)
    print(f"Generated {len(suites)} drought suites → {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

