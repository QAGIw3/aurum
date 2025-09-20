from __future__ import annotations

from datetime import date
from pathlib import Path

import json
import pandas as pd
import pytest

from aurum.parsers.runner import main as runner_main


def _build_simple_workbook(path: Path) -> None:
    data = [
        ["ISO:", None, "PJM"],
        ["Market:", None, "DA"],
        ["Hours:", None, "ON_PEAK"],
        ["Location:", None, "AECO"],
        ["Product:", None, "power"],
        ["Units:", None, "USD/MWh"],
        [None, pd.Timestamp("2025-01-01"), 45.5],
    ]
    with pd.ExcelWriter(path, engine="openpyxl") as writer:  # type: ignore[arg-type]
        pd.DataFrame(data).to_excel(writer, index=False, header=False, sheet_name="Fixed Prices - Mid")


def _write_minimal_suite(path: Path) -> None:
    # Expect the output to have required canonical columns
    suite = {
        "expectations": [
            {"expectation_type": "expect_table_columns_to_match_set", "kwargs": {"column_set": [
                "asof_date", "source_file", "sheet_name", "asset_class", "region", "iso", "location",
                "market", "product", "block", "spark_location", "price_type", "units_raw", "currency",
                "per_unit", "tenor_type", "contract_month", "tenor_label", "value", "bid", "ask", "mid",
                "curve_key", "version_hash", "_ingest_ts"
            ]}}
        ]
    }
    path.write_text(json.dumps(suite), encoding="utf-8")


def test_runner_with_validation_succeeds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    xlsx = tmp_path / "EOD_SIMPLE_20250101.xlsx"
    _build_simple_workbook(xlsx)
    suite = tmp_path / "suite.json"
    _write_minimal_suite(suite)

    out_dir = tmp_path / "out"
    rc = runner_main([
        str(xlsx),
        "--as-of", "2025-01-01",
        "--output-dir", str(out_dir),
        "--format", "csv",
        "--validate",
        "--suite", str(suite),
    ])
    assert rc == 0
    # Ensure an output file was produced
    outputs = list(out_dir.glob("*.csv"))
    assert outputs, "Expected a CSV output file to be created"

