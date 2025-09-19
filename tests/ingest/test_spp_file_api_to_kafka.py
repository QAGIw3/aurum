from __future__ import annotations

from scripts.ingest.spp_file_api_to_kafka import to_record


def test_spp_to_record() -> None:
    row = {
        "Interval": "2024-01-01T00:00:00Z",
        "Settlement Location": "P_NODE_1",
        "LMP": "41.2",
        "Energy": "39.5",
        "Congestion": "1.0",
        "Loss": "0.7",
        "Location Type": "HUB",
    }
    rec = to_record(row, iso="SPP", interval_minutes=5)
    assert rec["iso_code"] == "SPP"
    assert rec["price_total"] == 41.2
    assert rec["location_type"] == "HUB"
