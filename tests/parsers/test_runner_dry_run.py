from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

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
        [None, pd.Timestamp("2025-02-01"), 46.0],
    ]
    df = pd.DataFrame(data)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:  # type: ignore[arg-type]
        df.to_excel(writer, index=False, header=False, sheet_name="Fixed Prices - Mid")


def test_runner_dry_run_prints_summary(tmp_path: Path, capsys) -> None:
    xlsx = tmp_path / "EOD_SIMPLE_20250101.xlsx"
    _build_simple_workbook(xlsx)
    out_dir = tmp_path / "out"
    rc = runner_main([
        str(xlsx),
        "--as-of", "2025-01-01",
        "--output-dir", str(out_dir),
        "--format", "csv",
        "--dry-run",
    ])
    assert rc == 0
    # no outputs should be written
    assert not any(out_dir.glob("*.csv"))
    captured = capsys.readouterr()
    assert "Parsed" in captured.out and "distinct curves" in captured.out
