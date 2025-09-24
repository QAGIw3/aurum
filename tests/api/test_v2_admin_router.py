import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_invalidate_scenario_cache_router(monkeypatch):
    from aurum.api.v2 import admin as admin_router

    # No-op invalidation
    monkeypatch.setattr(admin_router, "invalidate_scenario_outputs_cache", lambda *a, **k: None)
    # Bypass CacheConfig.from_settings
    class DummyCfg:
        pass

    class DummyCacheConfig:
        @classmethod
        def from_settings(cls, settings):
            return DummyCfg()

    monkeypatch.setattr(admin_router, "CacheConfig", DummyCacheConfig)
    # Simplify get_settings
    monkeypatch.setattr(admin_router, "get_settings", lambda: object())

    app = FastAPI()
    app.include_router(admin_router.router)
    client = TestClient(app)

    res = client.post("/v2/admin/cache/scenario/s123/invalidate", params={"tenant_id": "t1"})
    assert res.status_code == 204
    # Headers convey metadata
    assert "X-Request-Id" in res.headers
    assert res.headers.get("X-Tenant-Id") == "t1"


@pytest.mark.unit
def test_invalidate_curves_cache_router(monkeypatch):
    from aurum.api.v2 import admin as admin_router

    async def fake_invalidate():
        return None

    monkeypatch.setattr(admin_router, "invalidate_curve_cache", fake_invalidate)

    app = FastAPI()
    app.include_router(admin_router.router)
    client = TestClient(app)

    res = client.post("/v2/admin/cache/curves/invalidate", params={"tenant_id": "t1"})
    assert res.status_code == 200
    payload = res.json()
    assert payload["cache_type"] == "curves"
    assert "ETag" in res.headers


@pytest.mark.unit
def test_list_mappings_router(monkeypatch):
    from aurum.api.v2 import admin as admin_router

    class FakeMapper:
        async def initialize(self):
            return None

        async def list_mappings(self, **kwargs):
            return ([
                {
                    "external_provider": "eia",
                    "external_series_id": "S",
                    "curve_key": "CURVE",
                    "mapping_method": "manual",
                }
            ], 1)

    monkeypatch.setattr(admin_router, "get_database_mapper", lambda: FakeMapper())

    app = FastAPI()
    app.include_router(admin_router.router)
    client = TestClient(app)

    res = client.get("/v2/admin/mappings", params={"tenant_id": "t1", "limit": 1})
    assert res.status_code == 200
    payload = res.json()
    assert payload["data"][0]["provider"] == "eia"
    assert "ETag" in res.headers

