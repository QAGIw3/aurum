from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _bypass_rbac(monkeypatch):
    # Replace RBAC dependency to allow tests to call admin endpoints without real auth
    def no_op_require_permissions(*_args, **_kwargs):
        async def _dep(request):
            request.state.principal = {"roles": ["admin"], "permissions": ["admin", "admin:config"]}
            return request.state.principal

        return _dep

    monkeypatch.setattr("aurum.security.rbac.require_permissions", no_op_require_permissions)


@pytest.fixture
def client(monkeypatch):
    _bypass_rbac(monkeypatch)
    from aurum.api.v2 import admin as admin_router
    app = FastAPI()
    app.include_router(admin_router.router)
    return TestClient(app)


def test_invalidate_curves_cache_v2(client, monkeypatch):
    # Stub invalidate_curve_cache to avoid external effects
    from aurum.api.v2 import admin as admin_router

    async def fake_invalidate():
        return None

    monkeypatch.setattr(admin_router, "invalidate_curve_cache", fake_invalidate)

    res = client.post("/v2/admin/cache/curves/invalidate", params={"tenant_id": "t1"})
    assert res.status_code == 200
    payload = res.json()
    assert payload["cache_type"] == "curves"
    # ETag should be present on v2 JSON admin responses
    assert "ETag" in res.headers


def test_invalidate_scenario_cache_v2(client, monkeypatch):
    # Bypass CacheConfig.from_settings and get_settings
    from aurum.api.v2 import admin as admin_router

    class DummyCfg:
        pass

    class DummyCacheConfig:
        @classmethod
        def from_settings(cls, settings):
            return DummyCfg()

    monkeypatch.setattr(admin_router, "CacheConfig", DummyCacheConfig)
    monkeypatch.setattr(admin_router, "get_settings", lambda: object())
    monkeypatch.setattr(admin_router, "invalidate_scenario_outputs_cache", lambda *a, **k: None)

    res = client.post("/v2/admin/cache/scenario/scn-1/invalidate", params={"tenant_id": "t1"})
    assert res.status_code == 204
    # Metadata headers present
    assert "X-Request-Id" in res.headers
    assert res.headers.get("X-Aurum-Tenant") == "t1"


def test_admin_mappings_list_v2(client, monkeypatch):
    from aurum.api.v2 import admin as admin_router

    class FakeMapper:
        async def initialize(self):
            return None

        async def list_mappings(self, **kwargs):
            return ([{
                "external_provider": "eia",
                "external_series_id": "S",
                "curve_key": "CURVE",
                "mapping_method": "manual",
            }], 1)

    monkeypatch.setattr(admin_router, "get_database_mapper", lambda: FakeMapper())
    res = client.get("/v2/admin/mappings", params={"tenant_id": "t1", "limit": 1})
    assert res.status_code == 200
    body = res.json()
    assert body["data"][0]["provider"] == "eia"
    assert "ETag" in res.headers


def test_admin_endpoints_require_tenant_param_when_applicable(client):
    # curves invalidate requires tenant_id
    res = client.post("/v2/admin/cache/curves/invalidate")
    assert res.status_code in (400, 422)

    # metadata units requires tenant_id
    res2 = client.post("/v2/admin/cache/metadata/units/invalidate")
    assert res2.status_code in (400, 422)

    # metadata dimensions requires tenant_id
    res3 = client.post("/v2/admin/cache/metadata/dimensions/invalidate")
    assert res3.status_code in (400, 422)

    # eia series requires tenant_id
    res4 = client.post("/v2/admin/cache/eia/series/invalidate")
    assert res4.status_code in (400, 422)


def test_invalidate_metadata_units_dimensions_locations_and_eia(client):
    # Units
    r_units = client.post("/v2/admin/cache/metadata/units/invalidate", params={"tenant_id": "t1"})
    assert r_units.status_code == 200
    assert r_units.json()["cache_type"] == "metadata-units"
    assert "ETag" in r_units.headers

    # Dimensions
    r_dims = client.post("/v2/admin/cache/metadata/dimensions/invalidate", params={"tenant_id": "t1"})
    assert r_dims.status_code == 200
    assert r_dims.json()["cache_type"] == "metadata-dimensions"
    assert "ETag" in r_dims.headers

    # Locations (requires iso)
    r_locs = client.post(
        "/v2/admin/cache/metadata/locations/invalidate",
        params={"tenant_id": "t1", "iso": "PJM"},
    )
    assert r_locs.status_code == 200
    assert r_locs.json()["cache_type"] == "metadata-locations"
    assert "ETag" in r_locs.headers

    # EIA series
    r_eia = client.post("/v2/admin/cache/eia/series/invalidate", params={"tenant_id": "t1"})
    assert r_eia.status_code == 200
    assert r_eia.json()["cache_type"] == "eia-series"
    assert "ETag" in r_eia.headers


def test_counts_reflect_unified_cache_manager_results_for_units(client, monkeypatch):
    from aurum.api.v2 import admin as admin_router

    class StubManager:
        async def invalidate_pattern(self, pattern: str, namespace=None) -> int:
            # Return deterministic count for assertion
            if pattern.startswith("units:"):
                return 3
            return 0

    monkeypatch.setattr(admin_router, "get_unified_cache_manager", lambda: StubManager())

    res = client.post("/v2/admin/cache/metadata/units/invalidate", params={"tenant_id": "t1"})
    assert res.status_code == 200
    body = res.json()
    assert body["keys_purged"] == 3


def test_counts_reflect_unified_cache_manager_results_for_curves(client, monkeypatch):
    from aurum.api.v2 import admin as admin_router

    class StubManager:
        async def invalidate_pattern(self, pattern: str, namespace=None) -> int:
            if pattern == "curves:*":
                return 2
            if pattern == "curves-diff:*":
                return 5
            return 0

    monkeypatch.setattr(admin_router, "get_unified_cache_manager", lambda: StubManager())

    res = client.post("/v2/admin/cache/curves/invalidate", params={"tenant_id": "t1"})
    assert res.status_code == 200
    assert res.json()["keys_purged"] == 7
