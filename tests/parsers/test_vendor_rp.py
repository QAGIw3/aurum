from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from aurum.parsers.vendor_curves.parse_rp import parse as parse_rp


def _build_rp_workbook(path: Path) -> None:
    # RP uses 'Location:' and 'Product:' headers; region is optional
    data_mid = [
        ["Location:", None, "PJM"],
        ["Product:", None, "REC"],
        ["Units:", None, "USD/MWh"],
        [None, pd.Timestamp("2025-01-01"), 30.0],
        [None, pd.Timestamp("2025-02-01"), 31.0],
    ]
    with pd.ExcelWriter(path, engine="openpyxl") as writer:  # type: ignore[arg-type]
        pd.DataFrame(data_mid).to_excel(writer, index=False, header=False, sheet_name="Fixed Prices - Mid")


def test_parse_rp_workbook(tmp_path: Path) -> None:
    xlsx = tmp_path / "EOD_RP_20250101.xlsx"
    _build_rp_workbook(xlsx)
    asof = date(2025, 1, 1)
    df = parse_rp(str(xlsx), asof)
    assert not df.empty
    assert len(df) == 2
    # RP infers iso from location (US unless Canada)
    assert df.loc[0, "iso"] == "US"
    assert df.loc[0, "market"] == "PJM"
    assert df.loc[0, "location"] == "PJM"
    assert df.loc[0, "currency"] == "USD"
    assert df.loc[0, "per_unit"] == "MWh"
    assert df.loc[0, "mid"] == 30.0
    assert str(df.loc[0, "contract_month"]) == "2025-01-01"

