import os
import sys
import types
from datetime import datetime
from typing import Any

import pytest

os.environ.setdefault("AURUM_API_AUTH_DISABLED", "1")


def _ensure_opentelemetry(monkeypatch):
    stub_span = types.SimpleNamespace(set_attribute=lambda *args, **kwargs: None, is_recording=lambda: False)

    trace_module = types.ModuleType("opentelemetry.trace")
    trace_module.get_current_span = lambda: stub_span
    trace_module.get_tracer_provider = lambda: None
    trace_module.get_tracer = lambda *_args, **_kwargs: None
    trace_module.set_tracer_provider = lambda *_args, **_kwargs: None

    propagate_module = types.ModuleType("opentelemetry.propagate")
    propagate_module.inject = lambda *_args, **_kwargs: None

    resources_module = types.ModuleType("opentelemetry.sdk.resources")

    class _Resource:
        @staticmethod
        def create(attrs):
            return attrs

    resources_module.Resource = _Resource

    sdk_trace_module = types.ModuleType("opentelemetry.sdk.trace")

    class _TracerProvider:
        def __init__(self, resource=None, sampler=None):
            self.resource = resource
            self.sampler = sampler

        def add_span_processor(self, _processor):
            return None

    sdk_trace_module.TracerProvider = _TracerProvider

    trace_export_module = types.ModuleType("opentelemetry.sdk.trace.export")

    class _BatchSpanProcessor:
        def __init__(self, _exporter):
            pass

    trace_export_module.BatchSpanProcessor = _BatchSpanProcessor

    sampling_module = types.ModuleType("opentelemetry.sdk.trace.sampling")

    class _TraceIdRatioBased:
        def __init__(self, _ratio):
            self.ratio = _ratio

    sampling_module.TraceIdRatioBased = _TraceIdRatioBased

    otlp_trace_module = types.ModuleType("opentelemetry.exporter.otlp.proto.grpc.trace_exporter")

    class _OTLPSpanExporter:
        def __init__(self, **_kwargs):
            pass

    otlp_trace_module.OTLPSpanExporter = _OTLPSpanExporter

    logs_module = types.ModuleType("opentelemetry._logs")
    logs_module.set_logger_provider = lambda *_args, **_kwargs: None

    sdk_logs_module = types.ModuleType("opentelemetry.sdk._logs")

    class _LoggerProvider:
        def __init__(self, **_kwargs):
            pass

        def add_log_record_processor(self, _processor):
            return None

    sdk_logs_module.LoggerProvider = _LoggerProvider

    sdk_logs_export_module = types.ModuleType("opentelemetry.sdk._logs.export")

    class _BatchLogRecordProcessor:
        def __init__(self, _exporter):
            pass

    sdk_logs_export_module.BatchLogRecordProcessor = _BatchLogRecordProcessor

    otlp_logs_module = types.ModuleType("opentelemetry.exporter.otlp.proto.grpc._log_exporter")

    class _OTLPLogExporter:
        def __init__(self, **_kwargs):
            pass

    otlp_logs_module.OTLPLogExporter = _OTLPLogExporter

    logging_instr_module = types.ModuleType("opentelemetry.instrumentation.logging")

    class _LoggingInstrumentor:
        def instrument(self, **_kwargs):
            return None

    logging_instr_module.LoggingInstrumentor = _LoggingInstrumentor

    requests_instr_module = types.ModuleType("opentelemetry.instrumentation.requests")

    class _RequestsInstrumentor:
        def instrument(self, **_kwargs):
            return None

    requests_instr_module.RequestsInstrumentor = _RequestsInstrumentor

    fastapi_instr_module = types.ModuleType("opentelemetry.instrumentation.fastapi")
    fastapi_instr_module.FastAPIInstrumentor = None

    psycopg_instr_module = types.ModuleType("opentelemetry.instrumentation.psycopg")
    psycopg_instr_module.PsycopgInstrumentation = None

    base_module = types.ModuleType("opentelemetry")
    base_module.trace = trace_module
    base_module.propagate = propagate_module

    monkeypatch.setitem(sys.modules, "opentelemetry", base_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", trace_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.propagate", propagate_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk.resources", resources_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk.trace", sdk_trace_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk.trace.export", trace_export_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk.trace.sampling", sampling_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.exporter.otlp.proto.grpc.trace_exporter", otlp_trace_module)
    monkeypatch.setitem(sys.modules, "opentelemetry._logs", logs_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk._logs", sdk_logs_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk._logs.export", sdk_logs_export_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.exporter.otlp.proto.grpc._log_exporter", otlp_logs_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.logging", logging_instr_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.requests", requests_instr_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.fastapi", fastapi_instr_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.psycopg", psycopg_instr_module)

    if "psycopg" not in sys.modules:
        psycopg_stub = types.ModuleType("psycopg")

        class _StubConnection:
            def cursor(self):  # pragma: no cover - safety fallback
                raise RuntimeError("psycopg stub cursor used in tests")

            def close(self) -> None:  # pragma: no cover - safety fallback
                return None

        def _stub_connect(*_args, **_kwargs):  # pragma: no cover - safety fallback
            return _StubConnection()

        psycopg_stub.connect = _stub_connect  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "psycopg", psycopg_stub)

    if "psycopg.rows" not in sys.modules:
        rows_stub = types.ModuleType("psycopg.rows")
        rows_stub.dict_row = lambda *_args, **_kwargs: None  # type: ignore[assignment]
        monkeypatch.setitem(sys.modules, "psycopg.rows", rows_stub)

    if "geopandas" not in sys.modules:
        geopandas_stub = types.ModuleType("geopandas")

        class _GeoDataFrame:  # pragma: no cover - simple placeholder
            pass

        geopandas_stub.GeoDataFrame = _GeoDataFrame  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "geopandas", geopandas_stub)

    if "rasterio" not in sys.modules:
        rasterio_stub = types.ModuleType("rasterio")
        rasterio_stub.open = lambda *args, **kwargs: None  # pragma: no cover
        rasterio_stub.enums = types.SimpleNamespace(Resampling=types.SimpleNamespace(bilinear=1))
        rasterio_stub.mask = types.SimpleNamespace(mask=lambda *args, **kwargs: (None, None))
        monkeypatch.setitem(sys.modules, "rasterio", rasterio_stub)
        monkeypatch.setitem(sys.modules, "rasterio.enums", rasterio_stub.enums)
        monkeypatch.setitem(sys.modules, "rasterio.mask", rasterio_stub.mask)

    if "shapely" not in sys.modules:
        shapely_stub = types.ModuleType("shapely")
        shapely_stub.geometry = types.SimpleNamespace(mapping=lambda geom: geom)
        monkeypatch.setitem(sys.modules, "shapely", shapely_stub)
        monkeypatch.setitem(sys.modules, "shapely.geometry", shapely_stub.geometry)


@pytest.fixture(autouse=True)
def reset_store():
    from aurum.api.scenario_service import STORE

    STORE.reset()
    yield
    STORE.reset()


def test_list_curves_basic(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    # stub service.query_curves to avoid Trino dependency
    def fake_query_curves(*args, **kwargs):
        assert kwargs.get("cursor_after") is None
        rows = [
            {
                "curve_key": "abc",
                "tenor_label": "2025-09",
                "tenor_type": "MONTHLY",
                "contract_month": "2025-09-01",
                "asof_date": "2025-09-12",
                "mid": 42.0,
                "bid": 41.9,
                "ask": 42.1,
                "price_type": "MID",
            }
        ]
        return rows, 5.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)

    client = TestClient(api_app.app)
    resp = client.get("/v1/curves", params={"limit": 1})
    assert resp.status_code == 200
    assert resp.headers.get("etag")
    assert resp.headers.get("cache-control")
    payload = resp.json()
    assert "meta" in payload and "data" in payload
    assert payload["data"] and payload["data"][0]["curve_key"] == "abc"
    assert payload["meta"].get("next_cursor") is None


def test_list_curves_csv(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_curves(*args, **kwargs):
        rows = [
            {
                "curve_key": "abc",
                "tenor_label": "2025-09",
                "tenor_type": "MONTHLY",
                "contract_month": "2025-09-01",
                "asof_date": "2025-09-12",
                "mid": 42.0,
                "bid": 41.9,
                "ask": 42.1,
                "price_type": "MID",
            }
        ]
        return rows, 5.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)

    client = TestClient(api_app.app)
    resp = client.get("/v1/curves", params={"limit": 1, "format": "csv"})
    assert resp.status_code == 200
    assert resp.headers.get("content-type", "").startswith("text/csv")
    body_lines = [line for line in resp.text.splitlines() if line]
    assert body_lines[0].startswith("curve_key")
    assert "abc" in body_lines[1]
    assert resp.headers.get("x-next-cursor") is None


def test_list_curves_diff(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_curves_diff(*args, **kwargs):
        assert kwargs.get("cursor_after") is None
        rows = [
            {
                "curve_key": "abc",
                "tenor_label": "2025-09",
                "tenor_type": "MONTHLY",
                "contract_month": "2025-09-01",
                "asof_a": "2025-09-11",
                "mid_a": 41.0,
                "asof_b": "2025-09-12",
                "mid_b": 42.0,
                "diff_abs": 1.0,
                "diff_pct": 1.0/41.0,
            }
        ]
        return rows, 7.0

    monkeypatch.setattr(service, "query_curves_diff", fake_query_curves_diff)
    client = TestClient(api_app.app)
    resp = client.get("/v1/curves/diff", params={"asof_a": "2025-09-11", "asof_b": "2025-09-12", "limit": 1})
    assert resp.status_code == 200
    assert resp.headers.get("etag")
    payload = resp.json()
    assert payload["data"][0]["diff_abs"] == 1.0


def test_cursor_pagination_diff(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    rows = [
        {
            "curve_key": "a",
            "tenor_label": "2025-01",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-01-01",
            "asof_a": "2025-09-01",
            "mid_a": 10.0,
            "asof_b": "2025-09-02",
            "mid_b": 11.0,
            "diff_abs": 1.0,
            "diff_pct": 0.1,
        },
        {
            "curve_key": "b",
            "tenor_label": "2025-02",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-02-01",
            "asof_a": "2025-09-01",
            "mid_a": 12.0,
            "asof_b": "2025-09-02",
            "mid_b": 13.0,
            "diff_abs": 1.0,
            "diff_pct": 1.0/12.0,
        },
    ]

    captured_cursors: list = []

    def fake_query_curves_diff(*args, **kwargs):
        cursor_after = kwargs.get("cursor_after")
        captured_cursors.append(cursor_after)
        req_limit = kwargs.get("limit", 1)
        start = 0
        if cursor_after:
            for idx, item in enumerate(rows):
                if {
                    "curve_key": item["curve_key"],
                    "tenor_label": item["tenor_label"],
                    "contract_month": item.get("contract_month"),
                } == cursor_after:
                    start = idx + 1
                    break
        subset = rows[start : start + req_limit]
        return subset, 5.0

    monkeypatch.setattr(service, "query_curves_diff", fake_query_curves_diff)
    client = TestClient(api_app.app)

    r1 = client.get(
        "/v1/curves/diff",
        params={"asof_a": "2025-09-01", "asof_b": "2025-09-02", "limit": 1},
    )
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["meta"]["next_cursor"]
    assert captured_cursors[0] is None

    cursor = body1["meta"]["next_cursor"]
    r2 = client.get(
        "/v1/curves/diff",
        params={
            "asof_a": "2025-09-01",
            "asof_b": "2025-09-02",
            "limit": 1,
            "cursor": cursor,
        },
    )
    assert r2.status_code == 200
    assert captured_cursors[-1] == {
        "curve_key": "a",
        "tenor_label": "2025-01",
        "contract_month": "2025-01-01",
    }


def test_cursor_pagination(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    rows = [
        {
            "curve_key": "a",
            "tenor_label": "2025-01",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-01-01",
            "asof_date": "2025-09-12",
            "mid": 1.0,
            "bid": 1.0,
            "ask": 1.0,
            "price_type": "MID",
        },
        {
            "curve_key": "b",
            "tenor_label": "2025-02",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-02-01",
            "asof_date": "2025-09-12",
            "mid": 2.0,
            "bid": 2.0,
            "ask": 2.0,
            "price_type": "MID",
        },
    ]

    captured_cursors = []

    def fake_query_curves(*args, **kwargs):
        cursor_after = kwargs.get("cursor_after")
        cursor_before = kwargs.get("cursor_before")
        descending = kwargs.get("descending")
        captured_cursors.append((cursor_after, cursor_before, descending))
        req_limit = kwargs.get("limit", 1)

        if cursor_before:
            idx = 0
            for i, item in enumerate(rows):
                payload = {
                    "curve_key": item["curve_key"],
                    "tenor_label": item["tenor_label"],
                    "contract_month": item.get("contract_month"),
                    "asof_date": item.get("asof_date"),
                    "price_type": item.get("price_type"),
                }
                if payload == cursor_before:
                    idx = i
                    break
            start = max(0, idx - req_limit)
            subset = rows[start:idx]
            if descending:
                subset = list(reversed(subset))
            return subset, 3.0

        start = 0
        if cursor_after:
            for idx, item in enumerate(rows):
                payload = {
                    "curve_key": item["curve_key"],
                    "tenor_label": item["tenor_label"],
                    "contract_month": item.get("contract_month"),
                    "asof_date": item.get("asof_date"),
                    "price_type": item.get("price_type"),
                }
                if payload == cursor_after:
                    start = idx + 1
                    break
        subset = rows[start : start + req_limit]
        return subset, 3.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)
    client = TestClient(api_app.app)

    # First page limit=1, should include next_cursor
    r1 = client.get("/v1/curves", params={"limit": 1})
    assert r1.status_code == 200
    p1 = r1.json()
    assert len(p1["data"]) == 1
    assert p1["meta"].get("next_cursor")
    assert p1["meta"].get("prev_cursor") is None
    assert captured_cursors[0] == (None, None, False)

    # Second page using cursor
    cursor = p1["meta"]["next_cursor"]
    r2 = client.get("/v1/curves", params={"limit": 1, "cursor": cursor})
    assert r2.status_code == 200
    p2 = r2.json()
    assert len(p2["data"]) == 1
    assert captured_cursors[-1][0] == {
        "curve_key": "a",
        "tenor_label": "2025-01",
        "contract_month": "2025-01-01",
        "asof_date": "2025-09-12",
        "price_type": "MID",
    }
    assert captured_cursors[-1][1] is None
    assert p2["meta"].get("prev_cursor")


def test_cursor_prev_cursor(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    rows = [
        {
            "curve_key": "a",
            "tenor_label": "2025-01",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-01-01",
            "asof_date": "2025-09-12",
            "mid": 1.0,
            "price_type": "MID",
        },
        {
            "curve_key": "b",
            "tenor_label": "2025-02",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-02-01",
            "asof_date": "2025-09-12",
            "mid": 2.0,
            "price_type": "MID",
        },
    ]

    def fake_query_curves(*args, **kwargs):
        cursor_after = kwargs.get("cursor_after")
        cursor_before = kwargs.get("cursor_before")
        descending = kwargs.get("descending")
        limit = kwargs.get("limit", 1)
        if cursor_before:
            idx = 0
            for i, item in enumerate(rows):
                payload = {
                    "curve_key": item["curve_key"],
                    "tenor_label": item["tenor_label"],
                    "contract_month": item.get("contract_month"),
                    "asof_date": item.get("asof_date"),
                    "price_type": item.get("price_type"),
                }
                if payload == cursor_before:
                    idx = i
                    break
            subset = rows[max(0, idx - limit):idx]
            if descending:
                subset = list(reversed(subset))
            return subset, 3.0
        start = 0
        if cursor_after:
            for i, item in enumerate(rows):
                payload = {
                    "curve_key": item["curve_key"],
                    "tenor_label": item["tenor_label"],
                    "contract_month": item.get("contract_month"),
                    "asof_date": item.get("asof_date"),
                    "price_type": item.get("price_type"),
                }
                if payload == cursor_after:
                    start = i + 1
                    break
        subset = rows[start : start + limit]
        return subset, 3.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)
    client = TestClient(api_app.app)

    first = client.get("/v1/curves", params={"limit": 1}).json()
    cursor = first["meta"]["next_cursor"]
    second = client.get("/v1/curves", params={"limit": 1, "cursor": cursor}).json()
    prev_token = second["meta"].get("prev_cursor")
    assert prev_token
    back = client.get("/v1/curves", params={"limit": 1, "prev_cursor": prev_token})
    assert back.status_code == 200
    payload = back.json()["data"]
    assert payload and payload[0]["curve_key"] == "a"


def test_cursor_pagination_alias(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    rows = [
        {
            "curve_key": "a",
            "tenor_label": "2025-01",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-01-01",
            "asof_date": "2025-09-12",
            "mid": 1.0,
            "bid": 1.0,
            "ask": 1.0,
            "price_type": "MID",
        },
        {
            "curve_key": "b",
            "tenor_label": "2025-02",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-02-01",
            "asof_date": "2025-09-12",
            "mid": 2.0,
            "bid": 2.0,
            "ask": 2.0,
            "price_type": "MID",
        },
    ]

    captured = []

    def fake_query_curves(*args, **kwargs):
        captured.append(kwargs.get("cursor_after"))
        req_limit = kwargs.get("limit", 1)
        cursor_before = kwargs.get("cursor_before")
        if cursor_before:
            return [], 1.0
        start = 0
        cursor_after = kwargs.get("cursor_after")
        if cursor_after:
            for idx, row in enumerate(rows):
                snapshot = {
                    "curve_key": row["curve_key"],
                    "tenor_label": row["tenor_label"],
                    "contract_month": row.get("contract_month"),
                    "asof_date": row.get("asof_date"),
                    "price_type": row.get("price_type"),
                }
                if snapshot == cursor_after:
                    start = idx + 1
                    break
        subset = rows[start : start + req_limit]
        return subset, 1.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)
    client = TestClient(api_app.app)

    first = client.get("/v1/curves", params={"limit": 1})
    cursor = first.json()["meta"]["next_cursor"]
    second = client.get("/v1/curves", params={"limit": 1, "since_cursor": cursor})
    assert second.status_code == 200
    assert captured[-1] == {
        "curve_key": "a",
        "tenor_label": "2025-01",
        "contract_month": "2025-01-01",
        "asof_date": "2025-09-12",
        "price_type": "MID",
    }


def test_dimensions_endpoint(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_dimensions(*args, **kwargs):
        assert kwargs.get("include_counts") is False
        return {
            "asset_class": ["power"],
            "iso": ["PJM"],
            "market": ["DA"],
            "product": ["ATC"],
            "block": ["ON_PEAK"],
            "tenor_type": ["MONTHLY"],
            "location": ["West"],
        }, None

    monkeypatch.setattr(service, "query_dimensions", fake_query_dimensions)
    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/dimensions")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["data"]["iso"] == ["PJM"]
    assert payload.get("counts") is None


def test_dimensions_endpoint_with_counts(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_dimensions(*args, **kwargs):
        assert kwargs.get("include_counts") is True
        return (
            {
                "asset_class": ["power"],
                "iso": ["PJM"],
                "location": ["West"],
                "market": ["DA"],
                "product": ["ATC"],
                "block": ["ON_PEAK"],
                "tenor_type": ["MONTHLY"],
            },
            {
                "iso": [{"value": "PJM", "count": 10}],
                "market": [{"value": "DA", "count": 8}],
            },
        )

    monkeypatch.setattr(service, "query_dimensions", fake_query_dimensions)
    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/dimensions", params={"include_counts": "true"})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["counts"]["iso"][0]["value"] == "PJM"
    assert payload["counts"]["market"][0]["count"] == 8


def test_health_endpoint():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_ready_endpoint(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    monkeypatch.setattr(api_app, "_check_trino_ready", lambda cfg: True)
    client = TestClient(api_app.app)
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"


def test_ready_endpoint_unavailable(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    monkeypatch.setattr(api_app, "_check_trino_ready", lambda cfg: False)
    client = TestClient(api_app.app)
    resp = client.get("/ready")
    assert resp.status_code == 503


def test_strips_endpoint(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_curves(*args, **kwargs):
        rows = [
            {
                "curve_key": "abc",
                "tenor_label": "Calendar 2025",
                "tenor_type": "CALENDAR",
                "contract_month": None,
                "asof_date": "2025-09-12",
                "mid": 50.0,
                "bid": 49.9,
                "ask": 50.1,
                "price_type": "MID",
            }
        ]
        return rows, 4.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)
    client = TestClient(api_app.app)
    resp = client.get("/v1/curves/strips", params={"type": "CALENDAR", "limit": 1})
    assert resp.status_code == 200
    assert resp.headers.get("etag")
    assert resp.headers.get("etag")
    payload = resp.json()
    assert payload["data"][0]["tenor_type"] == "CALENDAR"


def test_scenarios_create_and_run():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)

    # Create scenario
    payload = {
        "tenant_id": "t1",
        "name": "My Scenario",
        "assumptions": [
            {"driver_type": "policy", "payload": {"policy_name": "RPS", "start_year": 2025}}
        ],
    }
    r = client.post("/v1/scenarios", json=payload)
    assert r.status_code == 201
    scenario_body = r.json()["data"]
    scenario_id = scenario_body["scenario_id"]
    assert scenario_id
    assert scenario_body["tenant_id"] == "t1"
    assert scenario_body["status"] == "CREATED"

    # Run scenario
    r2 = client.post(f"/v1/scenarios/{scenario_id}/run", json={"code_version": "abc"})
    assert r2.status_code == 202
    run_body = r2.json()["data"]
    assert run_body["state"] == "QUEUED"
    assert run_body["scenario_id"] == scenario_id


def test_ppa_valuate(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service
    from datetime import date

    client = TestClient(api_app.app)
    client.headers.update({"X-Aurum-Tenant": "tenant-42"})

    create_resp = client.post(
        "/v1/ppa/contracts",
        json={"instrument_id": "instr-1", "terms": {"strike": 42.5}},
    )
    assert create_resp.status_code == 201
    contract_id = create_resp.json()["data"]["ppa_contract_id"]

    def fake_query(trino_cfg, *, scenario_id, tenant_id=None, asof_date=None, metric="mid", options=None):
        assert scenario_id == "scn-123"
        assert tenant_id == "tenant-42"
        assert metric == "mid"
        assert options == {"foo": "bar"}
        return (
            [
                {
                    "period_start": date(2025, 1, 1),
                    "period_end": date(2025, 1, 1),
                    "metric": "mid",
                    "value": 12.5,
                    "currency": "USD",
                    "unit": "USD",
                    "curve_key": "curve-a",
                    "tenor_type": "MONTHLY",
                    "run_id": "run-xyz",
                }
            ],
            11.2,
        )

    monkeypatch.setattr(service, "query_ppa_valuation", fake_query)

    persisted: dict[str, Any] = {}

    def fake_persist(**kwargs):
        persisted.update(kwargs)

    monkeypatch.setattr(api_app, "_persist_ppa_valuation_records", fake_persist)

    payload = {"ppa_contract_id": contract_id, "scenario_id": "scn-123", "options": {"foo": "bar"}}
    r = client.post("/v1/ppa/valuate", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body["data"], list)
    assert body["data"][0]["value"] == pytest.approx(12.5)
    assert body["data"][0]["unit"] == "USD"
    assert persisted["ppa_contract_id"] == contract_id
    assert persisted["scenario_id"] == "scn-123"
    assert persisted["tenant_id"] == "tenant-42"


def test_persist_ppa_valuation_records_formats_decimal(monkeypatch):
    pd = pytest.importorskip("pandas", reason="pandas not installed")
    _ensure_opentelemetry(monkeypatch)
    from decimal import Decimal
    from datetime import date
    from aurum.api import app as api_app

    captured_frames = []

    def fake_write(frame):
        captured_frames.append(frame.copy(deep=True))

    monkeypatch.setenv("AURUM_API_PPA_WRITE_ENABLED", "1")
    monkeypatch.setattr("aurum.parsers.iceberg_writer.write_ppa_valuation", fake_write)

    metrics = [
        {
            "metric": "cashflow",
            "value": 100.1234567,
            "period_start": date(2025, 1, 1),
            "period_end": date(2025, 1, 31),
            "curve_key": "curve-a",
        },
        {
            "metric": "NPV",
            "value": 1234.56789,
            "period_start": date(2025, 1, 1),
            "period_end": date(2025, 12, 31),
            "curve_key": None,
        },
        {
            "metric": "IRR",
            "value": 0.1255554,
            "period_start": date(2025, 1, 1),
            "period_end": date(2025, 12, 31),
        },
    ]

    api_app._persist_ppa_valuation_records(
        ppa_contract_id="ppa-1",
        scenario_id="scn-1",
        tenant_id="tenant-1",
        asof_date=date(2025, 1, 1),
        metrics=metrics,
    )

    assert captured_frames, "write_ppa_valuation should receive a dataframe"
    records = captured_frames[0].to_dict(orient="records")

    assert all(row["tenant_id"] == "tenant-1" for row in records)

    cashflow_row = next(row for row in records if row["metric"] == "cashflow")
    assert cashflow_row["cashflow"] == Decimal("100.123457")
    assert cashflow_row["value"] == Decimal("100.123457")
    assert cashflow_row["npv"] is None
    assert pd.isna(cashflow_row["irr"])

    npv_row = next(row for row in records if row["metric"] == "NPV")
    assert npv_row["npv"] == Decimal("1234.567890")
    assert npv_row["value"] == Decimal("1234.567890")

    irr_row = next(row for row in records if row["metric"].lower() == "irr")
    assert irr_row["value"] == Decimal("0.125555")
    assert irr_row["irr"] == pytest.approx(0.1255554)

def test_ppa_valuate_requires_scenario(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    client.headers.update({"X-Aurum-Tenant": "tenant-99"})
    create_resp = client.post(
        "/v1/ppa/contracts",
        json={"instrument_id": None, "terms": {}},
    )
    assert create_resp.status_code == 201
    contract_id = create_resp.json()["data"]["ppa_contract_id"]

    payload = {"ppa_contract_id": contract_id}
    resp = client.post("/v1/ppa/valuate", json=payload)
    assert resp.status_code == 400
    assert resp.json()["detail"] == "scenario_id is required for valuation"



def test_ppa_contract_crud(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    client.headers.update({"X-Aurum-Tenant": "tenant-007"})

    create_resp = client.post(
        "/v1/ppa/contracts",
        json={"instrument_id": "instr-1", "terms": {"strike": 39.5}},
    )
    assert create_resp.status_code == 201
    created = create_resp.json()["data"]
    contract_id = created["ppa_contract_id"]
    assert created["instrument_id"] == "instr-1"

    get_resp = client.get(f"/v1/ppa/contracts/{contract_id}")
    assert get_resp.status_code == 200
    assert get_resp.headers.get("etag")

    list_resp = client.get("/v1/ppa/contracts", params={"limit": 1})
    assert list_resp.status_code == 200
    body = list_resp.json()
    assert body["data"][0]["ppa_contract_id"] == contract_id

    update_resp = client.patch(
        f"/v1/ppa/contracts/{contract_id}",
        json={"instrument_id": "instr-2", "terms": {"strike": 41.0}},
    )
    assert update_resp.status_code == 200
    updated = update_resp.json()["data"]
    assert updated["instrument_id"] == "instr-2"
    assert updated["terms"]["strike"] == 41.0

    delete_resp = client.delete(f"/v1/ppa/contracts/{contract_id}")
    assert delete_resp.status_code == 204

    missing = client.get(f"/v1/ppa/contracts/{contract_id}")
    assert missing.status_code == 404


def test_ppa_contract_create_requires_admin(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    client.headers.update({"X-Aurum-Tenant": "tenant-111"})

    monkeypatch.setattr(api_app, "ADMIN_GROUPS", {"aurum-admins"})

    resp = client.post(
        "/v1/ppa/contracts",
        json={"instrument_id": "instr-9", "terms": {}},
    )
    assert resp.status_code == 403


def test_ppa_contract_valuations_list(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    client = TestClient(api_app.app)
    client.headers.update({"X-Aurum-Tenant": "tenant-555"})

    create_resp = client.post(
        "/v1/ppa/contracts",
        json={"instrument_id": "instr-9", "terms": {}},
    )
    assert create_resp.status_code == 201
    contract_id = create_resp.json()["data"]["ppa_contract_id"]

    def fake_query(
        trino_cfg,
        *,
        ppa_contract_id,
        scenario_id=None,
        metric=None,
        limit,
        offset,
        tenant_id=None,
    ):
        assert ppa_contract_id == contract_id
        assert limit == 2  # limit + 1 from handler
        assert offset == 0
        assert tenant_id == "tenant-555"
        return (
            [
                {
                    "asof_date": datetime(2025, 9, 12).date(),
                    "period_start": datetime(2025, 9, 1).date(),
                    "period_end": datetime(2025, 9, 30).date(),
                    "scenario_id": "scn-1",
                    "tenant_id": "tenant-555",
                    "metric": "NPV",
                    "value": 1100.0,
                    "cashflow": 1250.0,
                    "npv": 1100.0,
                    "irr": 0.08,
                    "curve_key": "curve-a",
                    "version_hash": "hash-a",
                    "_ingest_ts": datetime(2025, 9, 12, 0, 0, 0),
                },
                {
                    "asof_date": datetime(2025, 10, 12).date(),
                    "period_start": datetime(2025, 10, 1).date(),
                    "period_end": datetime(2025, 10, 31).date(),
                    "scenario_id": "scn-1",
                    "tenant_id": "tenant-555",
                    "metric": "IRR",
                    "value": 0.08,
                    "cashflow": None,
                    "npv": None,
                    "irr": 0.0825,
                    "curve_key": "curve-a",
                    "version_hash": "hash-b",
                    "_ingest_ts": datetime(2025, 10, 12, 0, 0, 0),
                },
            ],
            12.3,
        )

    monkeypatch.setattr(service, "query_ppa_contract_valuations", fake_query)

    resp = client.get(
        f"/v1/ppa/contracts/{contract_id}/valuations",
        params={"limit": 1},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert len(payload["data"]) == 1
    assert payload["meta"]["next_cursor"]
    assert payload["data"][0]["metric"] == "NPV"


def test_metadata_units_etag(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/units")
    assert resp.status_code == 200
    etag = resp.headers.get("etag")
    assert etag

    cached = client.get("/v1/metadata/units", headers={"If-None-Match": etag})
    assert cached.status_code == 304


def test_list_eia_series_csv(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service
    from datetime import datetime

    def fake_query_series(*_args, **_kwargs):
        rows = [
            {
                "series_id": "EBA.PJME-ALL.NG.H",
                "period": "2024-01-01T01:00:00",
                "period_start": datetime(2024, 1, 1, 1, 0, 0),
                "period_end": datetime(2024, 1, 1, 2, 0, 0),
                "frequency": "HOURLY",
                "value": 123.4,
                "raw_value": "123.4",
                "unit": "MW",
                "canonical_unit": "MW",
                "canonical_currency": None,
                "canonical_value": 123.4,
                "conversion_factor": None,
                "area": "PJM",
                "sector": "ALL",
                "seasonal_adjustment": None,
                "description": "Sample",
                "source": "EIA",
                "dataset": "EBA",
                "metadata": {"quality": "FINAL"},
                "ingest_ts": datetime(2024, 1, 2, 0, 0, 0),
            }
        ]
        return rows, 4.2

    monkeypatch.setattr(service, "query_eia_series", fake_query_series)

    client = TestClient(api_app.app)
    resp = client.get("/v1/ref/eia/series", params={"limit": 1, "format": "csv"})
    assert resp.status_code == 200
    assert resp.headers.get("content-type", "").startswith("text/csv")
    lines = [line for line in resp.text.splitlines() if line]
    assert lines[0].startswith("series_id")
    assert "EBA.PJME-ALL.NG.H" in lines[1]


def test_list_eia_series(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_series(*_args, **_kwargs):
        rows = [
            {
                "series_id": "EBA.PJME-ALL.NG.H",
                "period": "2024-01-01T01:00:00",
                "period_start": datetime(2024, 1, 1, 1, 0, 0),
                "period_end": datetime(2024, 1, 1, 2, 0, 0),
                "frequency": "HOURLY",
                "value": 123.4,
                "raw_value": "123.4",
                "unit": "MW",
                "area": "PJM",
                "sector": "ALL",
                "description": "Sample",
                "source": "EIA",
                "dataset": "EBA",
                "metadata": {"quality": "FINAL"},
                "ingest_ts": datetime(2024, 1, 2, 0, 0, 0),
            }
        ]
        return rows, 4.2

    monkeypatch.setattr(service, "query_eia_series", fake_query_series)

    client = TestClient(api_app.app)
    resp = client.get("/v1/ref/eia/series", params={"limit": 1})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["data"]
    record = payload["data"][0]
    assert record["series_id"] == "EBA.PJME-ALL.NG.H"
    assert record["metadata"] == {"quality": "FINAL"}


def test_list_eia_series_dimensions(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_dimensions(*_args, **_kwargs):
        values = {
            "dataset": ["EBA"],
            "area": ["PJM"],
            "sector": ["ALL"],
            "unit": ["MW"],
            "frequency": ["HOURLY"],
            "source": ["EIA"],
        }
        return values, 2.5

    monkeypatch.setattr(service, "query_eia_series_dimensions", fake_query_dimensions)

    client = TestClient(api_app.app)
    resp = client.get("/v1/ref/eia/series/dimensions")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["data"]["dataset"] == ["EBA"]
    assert payload["data"]["unit"] == ["MW"]


def test_list_scenario_outputs_csv(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    _ensure_opentelemetry(monkeypatch)
    from datetime import date
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    monkeypatch.setenv("AURUM_API_SCENARIO_OUTPUTS_ENABLED", "1")

    def fake_query(
        trino_cfg,
        cache_cfg,
        *,
        tenant_id,
        scenario_id,
        curve_key,
        tenor_type,
        metric,
        limit,
        offset,
        cursor_after,
        cursor_before,
        descending,
    ):
        rows = [
            {
                "tenant_id": tenant_id,
                "scenario_id": scenario_id,
                "run_id": "run-1",
                "asof_date": date(2024, 1, 1),
                "curve_key": "curve-a",
                "tenor_type": "MONTHLY",
                "contract_month": date(2024, 2, 1),
                "tenor_label": "2024-02",
                "metric": "mid",
                "value": 72.5,
                "band_lower": 70.0,
                "band_upper": 75.0,
                "attribution": {"policy": 1.2},
                "version_hash": "hash123",
            }
        ]
        return rows, 4.8

    monkeypatch.setattr(service, "query_scenario_outputs", fake_query)

    client = TestClient(api_app.app)
    client.headers.update({"X-Aurum-Tenant": "tenant-123"})
    resp = client.get(
        "/v1/scenarios/scn-1/outputs",
        params={"limit": 20, "format": "csv"},
    )
    assert resp.status_code == 200
    assert resp.headers.get("content-type", "").startswith("text/csv")
    lines = [line for line in resp.text.splitlines() if line]
    assert lines[0].startswith("tenant_id")
    assert "curve-a" in lines[1]
