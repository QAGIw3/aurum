from __future__ import annotations

import csv
import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "reference" / "iso_lookup.py"


def _run(args: list[str]) -> str:
    env = os.environ.copy()
    result = subprocess.run(
        ["python", str(SCRIPT), *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def test_iso_lookup_list_filters_iso(tmp_path: Path) -> None:
    out = _run(["--iso", "CAISO"])
    rows = list(csv.DictReader(out.splitlines()))
    assert rows
    assert all(r["iso"].upper() == "CAISO" for r in rows)
    assert any("SP15" in r["location_name"] for r in rows)


def test_iso_lookup_by_id(tmp_path: Path) -> None:
    out = _run(["--iso", "PJM", "--id", "AECO"])
    rows = list(csv.DictReader(out.splitlines()))
    assert len(rows) == 1
    row = rows[0]
    assert row["location_id"] == "AECO"
    assert row["location_name"] == "AECO Zone"

