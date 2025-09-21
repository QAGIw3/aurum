from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

import pytest

from scripts.ingest.miso_marketreports_to_kafka import _parse_dt, to_record


def test_to_record_maps_row_fields() -> None:
    row = {
        "Time": "2024-01-01 00:00",
        "LMP": "35.50",
        "MCC": "1.25",
        "MLC": "0.30",
        "CPNode": "ALTE",
        "CPNode ID": "12345",
        "Version": "ver-1",
    }

    record = to_record(row, market="DA")

    start = _parse_dt("2024-01-01 00:00")
    end = start + timedelta(minutes=60)
    expected_hash = hashlib.sha256(b"2024-01-01 00:00|12345|35.5").hexdigest()

    assert record["iso_code"] == "MISO"
    assert record["market"] == "DAY_AHEAD"
    assert record["interval_start"] == int(start.timestamp() * 1_000_000)
    assert record["interval_minutes"] == 60
    assert record["interval_end"] == int(end.timestamp() * 1_000_000)
    assert record["location_id"] == "12345"
    assert record["location_name"] == "ALTE"
    assert record["price_total"] == 35.5
    assert record["price_congestion"] == 1.25
    assert record["price_loss"] == 0.3
    assert record["price_energy"] == pytest.approx(35.5 - 1.25 - 0.3)
    assert record["currency"] == "USD"
    assert record["uom"] == "MWh"
    assert record["settlement_point"] == "ALTE"
    assert record["source_run_id"] == "ver-1"
    assert record["record_hash"] == expected_hash


def test_to_record_real_time_interval() -> None:
    row = {
        "Time": "2024-01-01 00:05",
        "LMP": "22.10",
    }
    record = to_record(row, market="RT")
    assert record["interval_minutes"] == 5
