import json
import os
import sys
import types
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.api.test_curves_api import _ensure_opentelemetry

# Ensure optional heavy dependencies are stubbed so the API module can import cleanly
if "geopandas" not in sys.modules:
    geopandas_stub = types.ModuleType("geopandas")

    class _GeoDataFrame:  # pragma: no cover - simple placeholder
        pass

    geopandas_stub.GeoDataFrame = _GeoDataFrame
    sys.modules["geopandas"] = geopandas_stub

if "rasterio" not in sys.modules:
    rasterio_stub = types.ModuleType("rasterio")
    rasterio_stub.open = lambda *args, **kwargs: None  # pragma: no cover
    rasterio_stub.enums = types.SimpleNamespace(Resampling=types.SimpleNamespace(bilinear=1))
    rasterio_stub.mask = types.SimpleNamespace(mask=lambda *args, **kwargs: (None, None))
    sys.modules["rasterio"] = rasterio_stub
    sys.modules["rasterio.enums"] = rasterio_stub.enums
    sys.modules["rasterio.mask"] = rasterio_stub.mask

if "shapely" not in sys.modules:
    shapely_stub = types.ModuleType("shapely")
    shapely_stub.geometry = types.SimpleNamespace(mapping=lambda geom: geom)
    sys.modules["shapely"] = shapely_stub
    sys.modules["shapely.geometry"] = shapely_stub.geometry

if "psycopg" not in sys.modules:
    psycopg_stub = types.SimpleNamespace(connect=lambda *args, **kwargs: None)
    sys.modules["psycopg"] = psycopg_stub
    sys.modules.setdefault("psycopg.rows", types.SimpleNamespace(dict_row=lambda *_a, **_kw: None))


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}

    def get(self, key: str):
        return self.store.get(key)

    def setex(self, key: str, _ttl: int, value: str) -> None:
        self.store[key] = value

    def sadd(self, key: str, *members: str) -> None:
        target = self.sets.setdefault(key, set())
        for member in members:
            member_str = member.decode("utf-8") if isinstance(member, bytes) else str(member)
            target.add(member_str)

    def expire(self, key: str, _ttl: int) -> None:
        return None

    def smembers(self, key: str):
        members = self.sets.get(key, set())
        return {member.encode("utf-8") for member in members}

    def delete(self, *keys: str):
        removed = 0
        for key in keys:
            key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            if key_str in self.store:
                self.store.pop(key_str, None)
                removed += 1
            if key_str in self.sets:
                self.sets.pop(key_str, None)
        return removed

    def scan_iter(self, pattern: str):
        from fnmatch import fnmatch

        for key in list(self.store.keys()):
            if fnmatch(key, pattern):
                yield key

    def srem(self, key: str, *members: Any) -> int:
        target = self.sets.get(key)
        if not target:
            return 0
        removed = 0
        for member in members:
            member_str = member.decode("utf-8") if isinstance(member, bytes) else str(member)
            if member_str in target:
                target.remove(member_str)
                removed += 1
        if not target:
            self.sets.pop(key, None)
        return removed

    def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))


@pytest.fixture
def api_client(monkeypatch):
    _ensure_opentelemetry(monkeypatch)
    from aurum.api import app as api_app

    api_app._CACHE.clear()
    client = TestClient(api_app.app)
    yield client, api_app
    client.app.dependency_overrides.clear()
    api_app._CACHE.clear()


def _set_admin(client, api_app):
    client.app.dependency_overrides[api_app._get_principal] = lambda: {"groups": ["aurum-admins"], "tenant": "admin"}


def test_metadata_locations_cache_invalidate(monkeypatch, api_client):
    from aurum.api import service

    client, api_app = api_client
    fake_redis = FakeRedis()
    monkeypatch.setattr(service, "_maybe_redis_client", lambda cfg: fake_redis)
    monkeypatch.setattr(api_app, "ADMIN_GROUPS", {"aurum-admins"})

    api_app._metadata_cache_get_or_set("iso:locations:PJM", lambda: [{"iso": "PJM"}])
    api_app._metadata_cache_get_or_set("iso:loc:PJM:AECO", lambda: {"iso": "PJM", "location_id": "AECO"})

    assert fake_redis.store
    assert fake_redis.sets

    _set_admin(client, api_app)
    resp = client.post("/v1/admin/cache/metadata/locations/invalidate", params={"iso": "PJM"})
    assert resp.status_code == 200
    payload = resp.json()
    detail = payload["data"][0]
    assert detail["redis_keys_removed"] == 2
    assert detail["local_entries_removed"] == 2
    assert api_app._CACHE.get("iso:locations:PJM") is None
    assert not fake_redis.store
    assert not fake_redis.sets


def test_metadata_units_cache_invalidate(monkeypatch, api_client):
    from aurum.api import service

    client, api_app = api_client
    fake_redis = FakeRedis()
    monkeypatch.setattr(service, "_maybe_redis_client", lambda cfg: fake_redis)
    monkeypatch.setattr(api_app, "ADMIN_GROUPS", {"aurum-admins"})

    api_app._metadata_cache_get_or_set("units:canonical", lambda: {"currencies": ["USD"], "units": ["MWh"]})
    api_app._metadata_cache_get_or_set("units:mapping", lambda: [("USD/MWh", "USD", "MWh")])

    _set_admin(client, api_app)
    resp = client.post("/v1/admin/cache/metadata/units/invalidate")
    assert resp.status_code == 200
    payload = resp.json()
    detail = payload["data"][0]
    assert detail["redis_keys_removed"] == 2
    assert detail["local_entries_removed"] == 2
    assert api_app._CACHE.get("units:canonical") is None
    assert not fake_redis.store
    assert not fake_redis.sets


def test_metadata_dimensions_cache_invalidate(monkeypatch, api_client):
    from aurum.api import service

    client, api_app = api_client
    fake_redis = FakeRedis()
    monkeypatch.setattr(service, "_maybe_redis_client", lambda cfg: fake_redis)
    monkeypatch.setattr(api_app, "ADMIN_GROUPS", {"aurum-admins"})

    fake_redis.store["aurum:dimensions:test"] = json.dumps({"values": {"iso": ["PJM"]}, "counts": None})
    fake_redis.sets["aurum:dimensions:index"] = {"aurum:dimensions:test"}

    _set_admin(client, api_app)
    resp = client.post("/v1/admin/cache/metadata/dimensions/invalidate")
    assert resp.status_code == 200
    payload = resp.json()
    detail = payload["data"][0]
    assert detail["redis_keys_removed"] == 1
    assert detail["local_entries_removed"] == 0
    assert not fake_redis.store
    assert not fake_redis.sets


def test_eia_series_cache_invalidate(monkeypatch, api_client):
    from aurum.api import service

    client, api_app = api_client
    fake_redis = FakeRedis()
    monkeypatch.setattr(service, "_maybe_redis_client", lambda cfg: fake_redis)
    monkeypatch.setattr(api_app, "ADMIN_GROUPS", {"aurum-admins"})

    fake_redis.store["aurum:eia-series:hash"] = "{}"
    fake_redis.store["aurum:eia-series-dimensions:hash"] = "{}"
    fake_redis.sets["aurum:eia-series:index"] = {"aurum:eia-series:hash"}
    fake_redis.sets["aurum:eia-series-dimensions:index"] = {"aurum:eia-series-dimensions:hash"}

    _set_admin(client, api_app)
    resp = client.post("/v1/admin/cache/eia/series/invalidate")
    assert resp.status_code == 200
    payload = resp.json()
    scopes = {entry["scope"]: entry for entry in payload["data"]}
    assert scopes["eia-series"]["redis_keys_removed"] == 1
    assert scopes["eia-series-dimensions"]["redis_keys_removed"] == 1
    assert not fake_redis.store
    assert not fake_redis.sets


def test_curve_cache_invalidate(monkeypatch, api_client):
    from aurum.api import service

    client, api_app = api_client
    fake_redis = FakeRedis()
    monkeypatch.setattr(service, "_maybe_redis_client", lambda cfg: fake_redis)
    monkeypatch.setattr(api_app, "ADMIN_GROUPS", {"aurum-admins"})

    fake_redis.store["curves:hash1"] = "[]"
    fake_redis.store["curves-diff:hash2"] = "[]"

    _set_admin(client, api_app)
    resp = client.post("/v1/admin/cache/curves/invalidate")
    assert resp.status_code == 200
    payload = resp.json()
    scopes = {entry["scope"]: entry for entry in payload["data"]}
    assert scopes["curves"]["redis_keys_removed"] == 1
    assert scopes["curves-diff"]["redis_keys_removed"] == 1
    assert not fake_redis.store

def test_metadata_invalidate_requires_admin(monkeypatch, api_client):
    client, api_app = api_client
    monkeypatch.setattr(api_app, "ADMIN_GROUPS", {"aurum-admins"})

    resp = client.post("/v1/admin/cache/metadata/units/invalidate")
    assert resp.status_code == 403
    client.app.dependency_overrides.clear()
