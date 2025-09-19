from __future__ import annotations

from scripts.ingest.miso_marketreports_to_kafka import iter_rows, to_record


SAMPLE = """CPNode,CPNode ID,Name,Time,LMP,MCC,MLC
N1,1001,Node1,2024-01-01 00:00,30.0,1.0,0.5
"""


def test_miso_to_record() -> None:
    rows = list(iter_rows(SAMPLE.encode("utf-8")))
    rec = to_record(rows[0], market="RT")
    assert rec["iso_code"] == "MISO"
    assert rec["interval_minutes"] == 5
    assert rec["price_total"] == 30.0
    assert abs(rec["price_energy"] - (30.0 - 1.0 - 0.5)) < 1e-6
