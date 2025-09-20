from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from aurum.parsers.vendor_curves.parse_simple import parse as parse_simple


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


def test_parse_simple_workbook(tmp_path: Path) -> None:
    xlsx = tmp_path / "EOD_SIMPLE_20250101.xlsx"
    _build_simple_workbook(xlsx)
    asof = date(2025, 1, 1)
    df = parse_simple(str(xlsx), asof)
    assert not df.empty
    assert len(df) == 2
    assert set(["mid", "value", "currency", "per_unit", "curve_key"]).issubset(df.columns)
    assert df.loc[0, "iso"] == "PJM"
    assert df.loc[0, "market"] == "DA"
    assert df.loc[0, "block"] == "ON_PEAK"
    assert df.loc[0, "location"] == "AECO"
    assert df.loc[0, "currency"] == "USD"
    assert df.loc[0, "per_unit"] == "MWh"
    assert df.loc[0, "mid"] == 45.5
    assert df.loc[1, "mid"] == 46.0
    assert df.loc[0, "tenor_type"] == "MONTHLY"
    assert str(df.loc[0, "contract_month"]) == "2025-01-01"
