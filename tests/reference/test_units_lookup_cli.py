from __future__ import annotations

import csv
import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "reference" / "units_lookup.py"


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


def test_units_lookup_mapping_filters_prefix():
    out = _run(["--mode", "mapping", "--prefix", "USD"])
    rows = list(csv.DictReader(out.splitlines()))
    assert rows
    assert all(r["units_raw"].upper().startswith("USD") or r["units_raw"].startswith("$") for r in rows)


def test_units_lookup_canonical_lists():
    out = _run(["--mode", "canonical"]) 
    rows = list(csv.DictReader(out.splitlines()))
    assert any(r["type"] == "currency" and r["value"] == "USD" for r in rows)
    assert any(r["type"] == "unit" and r["value"] == "MWh" for r in rows)

