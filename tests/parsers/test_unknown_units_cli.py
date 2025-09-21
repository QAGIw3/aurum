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


def test_cli_emits_patch(tmp_path: Path, capsys) -> None:
    workbook = tmp_path / "EOD_PW_UNKNOWN.xlsx"
    _build_workbook(workbook, units="Z$ / Foo")

    units_map_copy = tmp_path / "units_map.csv"
    original = Path("config/units_map.csv").read_text(encoding="utf-8")
    units_map_copy.write_text(original, encoding="utf-8")

    exit_code = main(
        [
            str(workbook),
            "--as-of",
            "2025-01-01",
            "--emit-patch",
            "--units-map",
            str(units_map_copy),
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "+++" in captured.out
    assert "Foo" in captured.out


def test_cli_emit_patch_no_unknowns(tmp_path: Path, capsys) -> None:
    workbook = tmp_path / "EOD_PW_KNOWN.xlsx"
    _build_workbook(workbook, units="USD/MWh")

    units_map_copy = tmp_path / "units_map.csv"
    units_map_copy.write_text(Path("config/units_map.csv").read_text(encoding="utf-8"), encoding="utf-8")

    exit_code = main(
        [
            str(workbook),
            "--as-of",
            "2025-01-01",
            "--emit-patch",
            "--units-map",
            str(units_map_copy),
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "No unknown units" in captured.err
    assert captured.out.strip() == ""
