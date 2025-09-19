from __future__ import annotations

from scripts.ingest.isone_ws_to_kafka import to_record


def test_isone_to_record() -> None:
    item = {
        "begin": "2024-01-01T00:00:00Z",
        "end": "2024-01-01T01:00:00Z",
        "lmp": 28.5,
        "energy": 27.0,
        "congestion": 1.0,
        "loss": 0.5,
        "ptid": 12345,
        "name": "N1",
        "locationType": "NODE",
    }
    rec = to_record(item, market="DA")
    assert rec["iso_code"] == "ISONE"
    assert rec["interval_minutes"] == 60
    assert rec["price_total"] == 28.5
