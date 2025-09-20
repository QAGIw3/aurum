from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from aurum.parsers.runner import parse_files


def _build_simple_workbook(path: Path) -> None:
    data = [
        ["ISO:", None, "PJM"],
        ["Market:", None, "DA"],
        ["Hours:", None, "ON_PEAK"],
        ["Location:", None, "AECO"],
        ["Product:", None, "power"],
        ["Units:", None, "USD/MWh"],
        [None, pd.Timestamp("2025-01-01"), 45.5],
        [None, pd.Timestamp("2025-02-01"), 46.0],
    ]
    df = pd.DataFrame(data)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:  # type: ignore[arg-type]
        df.to_excel(writer, index=False, header=False, sheet_name="Fixed Prices - Mid")


def test_runner_detects_simple_vendor(tmp_path: Path) -> None:
    xlsx = tmp_path / "EOD_SIMPLE_20250101.xlsx"
    _build_simple_workbook(xlsx)
    asof = date(2025, 1, 1)
    df = parse_files([xlsx], as_of=asof)
    assert not df.empty
    assert set(["iso", "market", "block"]).issubset(df.columns)
    assert df.iloc[0]["iso"] == "PJM"
