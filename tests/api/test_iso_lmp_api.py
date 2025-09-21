import os
from datetime import date, datetime

import pytest

from aurum.tests.api.test_curves_api import _ensure_opentelemetry

os.environ.setdefault("AURUM_API_AUTH_DISABLED", "1")


def _sample_lmp_row() -> dict:
    return {
        "iso_code": "PJM",
        "market": "DA",
        "delivery_date": date(2024, 1, 1),
        "interval_start": datetime(2024, 1, 1, 0, 0, 0),
        "interval_end": datetime(2024, 1, 1, 1, 0, 0),
        "interval_minutes": 60,
        "location_id": "AECO",
        "location_name": "AECO",
        "location_type": "NODE",
        "price_total": 42.5,
        "price_energy": 40.0,
        "price_congestion": 1.5,
        "price_loss": 1.0,
        "currency": "USD",
        "uom": "MWh",
        "settlement_point": "AECO",
        "source_run_id": "run-1",
        "ingest_ts": datetime(2024, 1, 1, 1, 5, 0),
        "record_hash": "abc123",
        "metadata": {"quality": "FINAL"},
    }


def _sample_lmp_aggregate_row() -> dict:
    return {
        "iso_code": "PJM",
        "market": "DA",
        "interval_start": datetime(2024, 1, 1, 0, 0, 0),
        "location_id": "AECO",
        "currency": "USD",
        "uom": "MWh",
        "price_avg": 42.5,
        "price_min": 30.0,
        "price_max": 60.0,
        "price_stddev": 5.0,
        "sample_count": 12,
    }


def test_iso_lmp_last_24h_etag(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)

    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query(**kwargs):
        return ([_sample_lmp_row()], 4.2)

    monkeypatch.setattr(service, "query_iso_lmp_last_24h", fake_query)

    client = TestClient(api_app.app)
    response = client.get("/v1/iso/lmp/last-24h", params={"iso_code": "pjm"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["iso_code"] == "PJM"
    etag = response.headers.get("ETag")
    assert etag

    cached = client.get(
        "/v1/iso/lmp/last-24h",
        headers={"If-None-Match": etag},
    )
    assert cached.status_code == 304
    assert cached.text == ""


def test_iso_lmp_hourly_with_range(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)

    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    captured: dict = {}

    def fake_query(**kwargs):
        captured.update(kwargs)
        return ([_sample_lmp_aggregate_row()], 3.4)

    monkeypatch.setattr(service, "query_iso_lmp_hourly", fake_query)

    client = TestClient(api_app.app)
    start = "2024-01-01T00:00:00+00:00"
    end = "2024-01-07T00:00:00+00:00"
    response = client.get(
        "/v1/iso/lmp/hourly",
        params={"start": start, "end": end, "limit": 1},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["price_avg"] == pytest.approx(42.5)
    assert captured["start"] == datetime.fromisoformat(start)
    assert captured["end"] == datetime.fromisoformat(end)

    invalid = client.get(
        "/v1/iso/lmp/hourly",
        params={"start": end, "end": start},
    )
    assert invalid.status_code == 400
    assert invalid.json()["detail"] == "start_after_end"


def test_iso_lmp_daily_with_range(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)

    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    captured: dict = {}

    def fake_query(**kwargs):
        captured.update(kwargs)
        return ([_sample_lmp_aggregate_row()], 1.2)

    monkeypatch.setattr(service, "query_iso_lmp_daily", fake_query)

    client = TestClient(api_app.app)
    start = "2023-01-01T00:00:00+00:00"
    end = "2023-12-31T23:59:59+00:00"
    response = client.get(
        "/v1/iso/lmp/daily",
        params={"start": start, "end": end},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload
    assert payload["data"][0]["price_avg"] == pytest.approx(42.5)
    assert captured["start"] == datetime.fromisoformat(start)
    assert captured["end"] == datetime.fromisoformat(end)


def test_iso_lmp_last_24h_csv(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)

    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query(**kwargs):
        return ([_sample_lmp_row()], 2.8)

    monkeypatch.setattr(service, "query_iso_lmp_last_24h", fake_query)

    client = TestClient(api_app.app)
    response = client.get("/v1/iso/lmp/last-24h", params={"format": "csv"})
    assert response.status_code == 200
    assert response.headers["Content-Type"].startswith("text/csv")
    assert "iso_code" in response.text
    etag = response.headers.get("ETag")
    assert etag

    cached = client.get(
        "/v1/iso/lmp/last-24h",
        params={"format": "csv"},
        headers={"If-None-Match": etag},
    )
    assert cached.status_code == 304
    assert cached.text == ""
