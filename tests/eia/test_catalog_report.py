from __future__ import annotations

import json
from pathlib import Path

import pytest

REPORT_PATH = Path(__file__).resolve().parents[2] / "artifacts" / "eia_catalog_report.json"


@pytest.mark.skipif(not REPORT_PATH.exists(), reason="catalog report not generated")
def test_catalog_report_has_no_drift() -> None:
    payload = json.loads(REPORT_PATH.read_text())
    diff = payload.get("diff", {})
    assert diff.get("added_paths") in ([], None)
    assert diff.get("removed_paths") in ([], None)
    assert diff.get("changed") in ([], None)
