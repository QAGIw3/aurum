from datetime import date

import pytest

from aurum.parsers.utils import (
    compute_curve_key,
    compute_version_hash,
    detect_asof,
    parse_bid_ask,
)


def test_compute_curve_key_stable_hash():
    identity = {
        "asset_class": "Power",
        "iso": "PJM",
        "region": "West",
        "location": "Test Hub",
        "market": "DA",
        "product": "ATC",
        "block": "ON_PEAK",
        "spark_location": "HubX",
    }
    key1 = compute_curve_key(identity)
    key2 = compute_curve_key(identity)
    assert key1 == key2
    assert len(key1) == 64


def test_compute_version_hash_changes_with_input():
    base = compute_version_hash("file", "sheet", date(2024, 5, 1))
    updated = compute_version_hash("file", "sheet", date(2024, 5, 2))
    assert base != updated


def test_parse_bid_ask_handles_string():
    bid, ask = parse_bid_ask("45.25 / 47.75")
    assert bid == pytest.approx(45.25)
    assert ask == pytest.approx(47.75)


def test_parse_bid_ask_handles_single_number():
    bid, ask = parse_bid_ask("42")
    assert bid == ask == pytest.approx(42.0)


def test_parse_bid_ask_none():
    bid, ask = parse_bid_ask(None)
    assert bid is None and ask is None


def test_detect_asof_finds_iso_format():
    rows = [
        "Curve Report",
        "Generated: 2024-06-29",
        "As-Of: 2024-07-01",
    ]
    assert detect_asof(rows) == date(2024, 7, 1)


def test_detect_asof_finds_slash_format():
    rows = [
        "some header",
        "data as of 07/02/2024",
    ]
    assert detect_asof(rows) == date(2024, 7, 2)


def test_detect_asof_missing_raises():
    with pytest.raises(ValueError):
        detect_asof(["no date here"])
