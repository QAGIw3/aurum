import os
import sys
import types
from typing import Any, Dict, Optional

import pytest

# Ensure optional telemetry stays disabled enough for tests
os.environ.setdefault("AURUM_API_METRICS_ENABLED", "1")

# Stub heavy optional dependencies so importing the API does not fail in test environments
if "geopandas" not in sys.modules:
    geopandas_stub = types.ModuleType("geopandas")

    class _GeoDataFrame:  # pragma: no cover - simple stub
        pass

    geopandas_stub.GeoDataFrame = _GeoDataFrame
    sys.modules["geopandas"] = geopandas_stub

if "rasterio" not in sys.modules:
    rasterio_stub = types.ModuleType("rasterio")
    rasterio_stub.open = lambda *args, **kwargs: None  # pragma: no cover - unused in tests
    resampling_stub = types.SimpleNamespace(bilinear=1)
    rasterio_stub.enums = types.SimpleNamespace(Resampling=resampling_stub)
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
    sys.modules["psycopg.rows"] = types.SimpleNamespace(dict_row=lambda *_a, **_kw: None)

from fastapi.testclient import TestClient  # noqa: E402
import httpx  # noqa: E402
from aurum.api import app as api_app  # noqa: E402
from aurum.api import app  # noqa: E402


class FakeCache:
    def __init__(self) -> None:
        self._store: Dict[str, Any] = {}

    def get(self, key: str) -> Any:
        return self._store.get(key)

    def setex(self, key: str, _ttl: int, value: Any) -> None:
        self._store[key] = value

    def expire(self, _key: str, _ttl: int) -> None:
        return None


class FakeResponse:
    def __init__(
        self,
        *,
        content: bytes = b"",
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
        json_data: Any = None,
        text: Optional[str] = None,
    ) -> None:
        self.content = content
        self.status_code = status_code
        self.headers = headers or {}
        self._json = json_data
        self._text = text or ""

    def json(self) -> Any:
        return self._json

    @property
    def text(self) -> str:
        return self._text


def _metric_value(counter, *, endpoint: str, result: str) -> float:
    if counter is None:
        return 0.0
    for sample in counter.collect()[0].samples:  # pragma: no branch - limited sample set
        if sample.labels.get("endpoint") == endpoint and sample.labels.get("result") == result:
            return sample.value
    return 0.0


def _histogram_count(histogram, *, endpoint: str, status: str) -> float:
    if histogram is None:
        return 0.0
    for sample in histogram.collect()[0].samples:  # pragma: no branch - limited sample set
        if (
            sample.name.endswith("_count")
            and sample.labels.get("endpoint") == endpoint
            and sample.labels.get("status") == status
        ):
            return sample.value
    return 0.0


class _FakeCatalog:
    class _FakeRaster:
        def xyz_url(self, *_args) -> str:
            return "http://example.com/tile.png"

        def info_url(self, *_args) -> str:
            return "http://example.com/info.json"

    def raster_by_name(self, _name: str) -> "_FakeCatalog._FakeRaster":
        return self._FakeRaster()


def test_drought_tile_cache_metrics(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    pytest.importorskip("prometheus_client")

    fake_cache = FakeCache()
    monkeypatch.setattr(api_app, "_tile_cache", lambda: fake_cache)
    monkeypatch.setattr(api_app, "_drought_catalog", lambda: _FakeCatalog())

    class _FakeTileClient:
        calls = 0

        def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - construction
            pass

        def __enter__(self) -> "_FakeTileClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str) -> FakeResponse:
            _FakeTileClient.calls += 1
            return FakeResponse(content=b"PNGDATA", headers={"Content-Type": "image/png"})

    monkeypatch.setattr(httpx, "Client", _FakeTileClient)

    client = TestClient(api_app.app)

    baseline_miss = _metric_value(app.TILE_CACHE_COUNTER, endpoint="tile", result="miss")
    baseline_hit = _metric_value(app.TILE_CACHE_COUNTER, endpoint="tile", result="hit")
    baseline_fetch = _histogram_count(app.TILE_FETCH_LATENCY, endpoint="tile", status="success")

    resp = client.get(
        "/v1/drought/tiles/nclimgrid/SPI/3M/1/2/3.png",
        params={"valid_date": "2024-01-01"},
    )
    assert resp.status_code == 200
    assert resp.content == b"PNGDATA"

    assert _metric_value(app.TILE_CACHE_COUNTER, endpoint="tile", result="miss") == baseline_miss + 1
    assert _histogram_count(app.TILE_FETCH_LATENCY, endpoint="tile", status="success") == baseline_fetch + 1
    assert _FakeTileClient.calls == 1

    # Second request should be served from cache
    resp_cached = client.get(
        "/v1/drought/tiles/nclimgrid/SPI/3M/1/2/3.png",
        params={"valid_date": "2024-01-01"},
    )
    assert resp_cached.status_code == 200
    assert resp_cached.content == b"PNGDATA"
    assert _metric_value(app.TILE_CACHE_COUNTER, endpoint="tile", result="hit") == baseline_hit + 1
    assert _FakeTileClient.calls == 1


def test_drought_info_cache_metrics(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    pytest.importorskip("prometheus_client")

    fake_cache = FakeCache()
    monkeypatch.setattr(api_app, "_tile_cache", lambda: fake_cache)
    monkeypatch.setattr(api_app, "_drought_catalog", lambda: _FakeCatalog())

    class _FakeInfoClient:
        calls = 0

        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "_FakeInfoClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str) -> FakeResponse:
            _FakeInfoClient.calls += 1
            payload = {"tilejson": "2.2.0", "tiles": ["http://example.com/tile"]}
            text = json.dumps(payload)
            return FakeResponse(
                content=text.encode("utf-8"),
                headers={"Content-Type": "application/json"},
                json_data=payload,
                text=text,
            )

    import json  # local import to avoid polluting module scope unnecessarily

    monkeypatch.setattr(httpx, "Client", _FakeInfoClient)

    client = TestClient(api_app.app)

    baseline_miss = _metric_value(app.TILE_CACHE_COUNTER, endpoint="info", result="miss")
    baseline_hit = _metric_value(app.TILE_CACHE_COUNTER, endpoint="info", result="hit")
    baseline_fetch = _histogram_count(app.TILE_FETCH_LATENCY, endpoint="info", status="success")

    resp = client.get(
        "/v1/drought/info/nclimgrid/SPI/3M",
        params={"valid_date": "2024-01-01"},
    )
    assert resp.status_code == 200
    assert resp.json()["data"]["tilejson"] == "2.2.0"

    assert _metric_value(app.TILE_CACHE_COUNTER, endpoint="info", result="miss") == baseline_miss + 1
    assert _histogram_count(app.TILE_FETCH_LATENCY, endpoint="info", status="success") == baseline_fetch + 1
    assert _FakeInfoClient.calls == 1

    resp_cached = client.get(
        "/v1/drought/info/nclimgrid/SPI/3M",
        params={"valid_date": "2024-01-01"},
    )
    assert resp_cached.status_code == 200
    assert resp_cached.json()["data"]["tilejson"] == "2.2.0"
    assert _metric_value(app.TILE_CACHE_COUNTER, endpoint="info", result="hit") == baseline_hit + 1
    assert _FakeInfoClient.calls == 1
