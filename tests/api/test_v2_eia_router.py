import types
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_eia_datasets_router_uses_service(monkeypatch):
    from aurum.api.v2 import eia as eia_router

    class FakeItem:
        def __init__(self, path: str):
            self.dataset_path = path
            self.title = f"Title {path}"
            self.description = f"Desc {path}"
            self.last_updated = "2024-01-01"

    class FakeSvc:
        async def list_datasets(self, *, offset: int, limit: int):
            return [FakeItem("cat/0")], 10

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(eia_router, "get_eia_service", fake_factory)

    app = FastAPI()
    app.include_router(eia_router.router)
    client = TestClient(app)

    res = client.get("/v2/metadata/eia/datasets", params={"tenant_id": "t1", "limit": 1})
    assert res.status_code == 200
    payload = res.json()
    assert payload["data"][0]["dataset_path"] == "cat/0"
    assert "ETag" in res.headers


@pytest.mark.unit
def test_eia_series_router_uses_service(monkeypatch):
    from aurum.api.v2 import eia as eia_router

    point = types.SimpleNamespace(
        series_id="S",
        period="P1",
        period_start="2024-01-01",
        value=1.23,
        unit="MW",
    )

    class FakeSvc:
        async def get_series(self, **kwargs):
            return [point]

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(eia_router, "get_eia_service", fake_factory)

    app = FastAPI()
    app.include_router(eia_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/ref/eia/series",
        params={"tenant_id": "t1", "series_id": "S", "limit": 1},
    )
    assert res.status_code == 200
    payload = res.json()
    assert payload["data"][0]["series_id"] == "S"
    assert "ETag" in res.headers


@pytest.mark.unit
def test_eia_series_dimensions_router(monkeypatch):
    from aurum.api.v2 import eia as eia_router

    values = {
        "dataset": ["d1"],
        "area": ["a1"],
        "sector": ["s1"],
        "unit": ["u1"],
        "canonical_unit": ["cu"],
        "canonical_currency": ["USD"],
        "frequency": ["M"],
        "source": ["eia"],
    }

    class FakeSvc:
        async def get_series_dimensions(self, **kwargs):
            return values

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(eia_router, "get_eia_service", fake_factory)

    app = FastAPI()
    app.include_router(eia_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/ref/eia/series/dimensions",
        params={"tenant_id": "t1", "series_id": "S"},
    )
    assert res.status_code == 200
    assert "ETag" in res.headers
