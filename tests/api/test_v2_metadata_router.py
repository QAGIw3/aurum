import types
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_metadata_dimensions_router(monkeypatch):
    from aurum.api.v2 import metadata as meta_router

    class FakeSvc:
        async def list_dimensions(self, *, asof, offset, limit):
            items = [
                {"dimension": "iso", "values": ["PJM"], "asof": asof or "latest"}
            ]
            return items, 1

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(meta_router, "get_metadata_service", fake_factory)

    app = FastAPI()
    app.include_router(meta_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/metadata/dimensions",
        params={"tenant_id": "t1", "limit": 1},
    )
    assert res.status_code == 200
    payload = res.json()
    assert payload["data"][0]["dimension"] == "iso"
    assert "ETag" in res.headers


@pytest.mark.unit
def test_metadata_locations_router(monkeypatch):
    from aurum.api.v2 import metadata as meta_router

    class FakeSvc:
        async def list_locations(self, *, iso, offset, limit):
            return ([{"iso": iso, "location_id": "L1", "name": "Test"}], 1)

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(meta_router, "get_metadata_service", fake_factory)

    app = FastAPI()
    app.include_router(meta_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/metadata/locations",
        params={"tenant_id": "t1", "iso": "PJM", "limit": 1},
    )
    assert res.status_code == 200
    assert "ETag" in res.headers


@pytest.mark.unit
def test_metadata_units_router(monkeypatch):
    from aurum.api.v2 import metadata as meta_router

    class FakeSvc:
        async def list_units(self, *, offset, limit):
            return ([{"currency": "USD", "per_unit": "MWh"}], 1)

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(meta_router, "get_metadata_service", fake_factory)

    app = FastAPI()
    app.include_router(meta_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/metadata/units",
        params={"tenant_id": "t1", "limit": 1},
    )
    assert res.status_code == 200
    assert "ETag" in res.headers


@pytest.mark.unit
def test_metadata_calendars_router(monkeypatch):
    from aurum.api.v2 import metadata as meta_router

    class FakeSvc:
        async def list_calendars(self, *, offset, limit):
            return ([{"name": "us", "timezone": "UTC", "blocks": ["ON_PEAK"]}], 1)

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(meta_router, "get_metadata_service", fake_factory)

    app = FastAPI()
    app.include_router(meta_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/metadata/calendars",
        params={"tenant_id": "t1", "limit": 1},
    )
    assert res.status_code == 200
    assert "ETag" in res.headers
