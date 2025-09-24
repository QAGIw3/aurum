import types
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_iso_lmp_last24h_router(monkeypatch):
    from aurum.api.v2 import iso as iso_router

    class FakeSvc:
        async def lmp_last_24h(self, **kwargs):
            return [
                {"timestamp": "2024-01-01T00:00:00Z", "location_id": "L1", "value": 10.0, "unit": "USD/MWh"}
            ]

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(iso_router, "get_iso_service", fake_factory)

    app = FastAPI()
    app.include_router(iso_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/iso/lmp/last-24h",
        params={"tenant_id": "t1", "iso": "PJM", "limit": 1},
    )
    assert res.status_code == 200
    payload = res.json()
    assert payload["data"][0]["location_id"] == "L1"
    assert "ETag" in res.headers


@pytest.mark.unit
def test_iso_lmp_hourly_router(monkeypatch):
    from aurum.api.v2 import iso as iso_router

    class FakeSvc:
        async def lmp_hourly(self, **kwargs):
            return [
                {"interval_start": "2024-01-01T00:00:00Z", "location_id": "L1", "price_avg": 12.3}
            ]

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(iso_router, "get_iso_service", fake_factory)

    app = FastAPI()
    app.include_router(iso_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/iso/lmp/hourly",
        params={"tenant_id": "t1", "iso": "PJM", "date": "2024-01-01", "limit": 1},
    )
    assert res.status_code == 200
    assert "ETag" in res.headers


@pytest.mark.unit
def test_iso_lmp_daily_router(monkeypatch):
    from aurum.api.v2 import iso as iso_router

    class FakeSvc:
        async def lmp_daily(self, **kwargs):
            return [
                {"interval_start": "2024-01-01T00:00:00Z", "location_id": "L1", "price_avg": 15.0}
            ]

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(iso_router, "get_iso_service", fake_factory)

    app = FastAPI()
    app.include_router(iso_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/iso/lmp/daily",
        params={"tenant_id": "t1", "iso": "PJM", "date": "2024-01-01", "limit": 1},
    )
    assert res.status_code == 200
    assert "ETag" in res.headers
