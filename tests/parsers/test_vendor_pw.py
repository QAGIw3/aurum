from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import json

import pandas as pd

from aurum.parsers.vendor_curves.parse_pw import parse as parse_pw


def _build_pw_workbook(path: Path) -> None:
    # Headers + two tenor rows for Fixed Prices - Mid
    data_mid = [
        ["ISO:", None, "PJM"],
        ["Market:", None, "DA"],
        ["Hours:", None, "ON_PEAK"],
        ["Location:", None, "AECO"],
        ["Product:", None, "power"],
        ["Units:", None, "USD/MWh"],
        [None, pd.Timestamp("2025-01-01"), 45.5],
        [None, pd.Timestamp("2025-02-01"), 46.0],
    ]
    # Bid/Ask sheet with matching structure (optional)
    data_ba = [
        ["ISO:", None, "PJM"],
        ["Market:", None, "DA"],
        ["Hours:", None, "ON_PEAK"],
        ["Location:", None, "AECO"],
        ["Product:", None, "power"],
        ["Units:", None, "USD/MWh"],
        [None, pd.Timestamp("2025-01-01"), "45.0 / 46.0"],
        [None, pd.Timestamp("2025-02-01"), "46.0 / 47.0"],
    ]

    with pd.ExcelWriter(path, engine="openpyxl") as writer:  # type: ignore[arg-type]
        pd.DataFrame(data_mid).to_excel(writer, index=False, header=False, sheet_name="Fixed Prices - Mid")
        pd.DataFrame(data_ba).to_excel(writer, index=False, header=False, sheet_name="Fixed Prices - BidAsk")


def test_parse_pw_workbook(tmp_path: Path) -> None:
    xlsx = tmp_path / "EOD_PW_20250101.xlsx"
    _build_pw_workbook(xlsx)
    asof = date(2025, 1, 1)
    df = parse_pw(str(xlsx), asof)
    assert not df.empty
    assert len(df) == 2
    assert df.loc[0, "iso"] == "PJM"
    assert df.loc[0, "market"] == "DA"
    assert df.loc[0, "block"] == "ON_PEAK"
    assert df.loc[0, "location"] == "AECO"
    assert df.loc[0, "currency"] == "USD"
    assert df.loc[0, "per_unit"] == "MWh"
    assert df.loc[0, "mid"] == 45.5  # mid sheet takes precedence; bid/ask merges when missing
    assert df.loc[0, "price_type"] == "MID"
    assert df.loc[0, "tenor_type"] == "MONTHLY"
    assert str(df.loc[0, "contract_month"]) == "2025-01-01"


def _canonicalise(df: pd.DataFrame) -> list[dict[str, object]]:
    frame = df.copy()
    if "_ingest_ts" in frame.columns:
        frame = frame.drop(columns=["_ingest_ts"])
    sort_cols = [col for col in ("sheet_name", "tenor_label", "price_type") if col in frame.columns]
    if sort_cols:
        frame = frame.sort_values(sort_cols).reset_index(drop=True)
    frame = frame.where(pd.notna(frame), None)
    records: list[dict[str, object]] = []
    for raw in frame.to_dict(orient="records"):
        record: dict[str, object] = {}
        for key, value in raw.items():
            if value is None:
                record[key] = None
                continue
            if isinstance(value, pd.Timestamp):
                if pd.isna(value):
                    record[key] = None
                    continue
                if value.tzinfo is not None:
                    value = value.tz_convert(None)
                record[key] = value.date().isoformat()
                continue
            if isinstance(value, datetime):
                record[key] = value.isoformat()
                continue
            if isinstance(value, date):
                record[key] = value.isoformat()
                continue
            record[key] = value
        records.append(record)
    return records


def test_pw_golden_fixture() -> None:
    fixture_dir = Path("tests/parsers/data")
    workbook = fixture_dir / "pw_golden.xlsx"
    expected_path = fixture_dir / "pw_golden_expected.json"
    expected = json.loads(expected_path.read_text())

    df = parse_pw(str(workbook), date(2025, 1, 1))
    actual = _canonicalise(df)
    assert actual == expected
