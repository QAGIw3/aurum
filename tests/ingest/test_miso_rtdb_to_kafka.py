from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

import pytest

from scripts.ingest.miso_rtdb_to_kafka import (
    combine_interval,
    iter_lmp_rows,
    normalize_market,
    to_record,
)


@pytest.fixture()
def sample_row() -> dict[str, object]:
    return {
        "RefId": "2024-05-01T10:00:00-05:00",
        "HourAndMin": "10:05",
        "Location": "ILLINOIS.HUB",
        "LocationId": "ILLHUB",
        "Type": "Hub",
        "SystemMarginalPrice": "32.50",
        "MarginalCongestionComponent": "1.20",
        "MarginalLossComponent": "-0.50",
        "TimeZone": "America/Chicago",
    }


def test_combine_interval_handles_timezone() -> None:
    start, end, minutes = combine_interval(
        "2024-05-01T10:00:00-05:00", "10:05", interval_seconds=300
    )

    assert start == datetime(2024, 5, 1, 15, 5, tzinfo=timezone.utc)
    assert end == start + timedelta(minutes=5)
    assert minutes == 5


def test_to_record_maps_fields(sample_row: dict[str, object]) -> None:
    record = to_record(sample_row, market="RTM", region="ALL", interval_seconds=300)

    expected_start = datetime(2024, 5, 1, 15, 5, tzinfo=timezone.utc)
    expected_hash = hashlib.sha256(b"2024-05-01T10:00:00-05:00|10:05|ILLHUB|32.500000").hexdigest()

    assert record["iso_code"] == "MISO"
    assert record["market"] == "REAL_TIME"
    assert record["interval_start"] == int(expected_start.timestamp() * 1_000_000)
    assert record["interval_end"] == int((expected_start + timedelta(minutes=5)).timestamp() * 1_000_000)
    assert record["interval_minutes"] == 5
    assert record["location_id"] == "ILLHUB"
    assert record["location_name"] == "ILLINOIS.HUB"
    assert record["location_type"] == "HUB"
    assert record["price_total"] == pytest.approx(32.5)
    assert record["price_congestion"] == pytest.approx(1.2)
    assert record["price_loss"] == pytest.approx(-0.5)
    assert record["price_energy"] == pytest.approx(32.5 - 1.2 - (-0.5))
    assert record["currency"] == "USD"
    assert record["uom"] == "MWh"
    assert record["settlement_point"] == "ILLINOIS.HUB"
    assert record["metadata"] == {"region": "ALL"}
    assert record["record_hash"] == expected_hash


def test_iter_lmp_rows_extracts_nested_payload(sample_row: dict[str, object]) -> None:
    payload = {
        "Items": [
            {
                "Meta": {"Region": "ALL"},
                "Rows": [sample_row],
            }
        ]
    }

    rows = list(iter_lmp_rows(payload))
    assert rows == [sample_row]


def test_normalize_market_variants() -> None:
    assert normalize_market("rtm") == "REAL_TIME"
    assert normalize_market("DA") == "DAY_AHEAD"
    assert normalize_market("RTPD") == "FIVE_MINUTE"
