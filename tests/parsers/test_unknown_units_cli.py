from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from aurum.parsers.unknown_units_cli import main


def _build_workbook(path: Path, *, units: str) -> None:
    data = [
        ["ISO:", None, "PJM"],
        ["Market:", None, "DA"],
        ["Hours:", None, "ON_PEAK"],
        ["Location:", None, "AECO"],
        ["Product:", None, "power"],
        ["Units:", None, units],
        [None, pd.Timestamp("2025-01-01"), 45.5],
    ]
    with pd.ExcelWriter(path, engine="openpyxl") as writer:  # type: ignore[arg-type]
        pd.DataFrame(data).to_excel(writer, index=False, header=False, sheet_name="Fixed Prices - Mid")


def test_cli_reports_unknown_units(tmp_path: Path, capsys) -> None:
    workbook = tmp_path / "EOD_PW_UNKNOWN.xlsx"
    _build_workbook(workbook, units="Z$ / Foo")

    exit_code = main([str(workbook), "--as-of", "2025-01-01", "--json"])
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "Foo" in out
    assert "PJM" in out


def test_cli_handles_no_unknown_units(tmp_path: Path, capsys) -> None:
    workbook = tmp_path / "EOD_PW_KNOWN.xlsx"
    _build_workbook(workbook, units="USD/MWh")

    exit_code = main([str(workbook), "--as-of", "2025-01-01", "--json"])
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "No unknown units" in out or out.strip() == "[]"
