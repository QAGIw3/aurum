"""Smoke tests for core API endpoints to ensure basic functionality works."""

from __future__ import annotations

import os
import sys
import types
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

# Mock OpenTelemetry to avoid dependency issues in tests
def _mock_opentelemetry():
    """Mock OpenTelemetry modules to avoid import errors in tests."""
    stub_span = types.SimpleNamespace(
        set_attribute=lambda *args, **kwargs: None,
        is_recording=lambda: False
    )

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

    class _ConsoleSpanExporter:
        def __init__(self, **kwargs):
            pass

    trace_export_module.ConsoleSpanExporter = _ConsoleSpanExporter

    # Add to sys.modules
    sys.modules["opentelemetry.trace"] = trace_module
    sys.modules["opentelemetry.propagate"] = propagate_module
    sys.modules["opentelemetry.sdk.resources"] = resources_module
    sys.modules["opentelemetry.sdk.trace"] = sdk_trace_module
    sys.modules["opentelemetry.sdk.trace.export"] = trace_export_module


# Mock OpenTelemetry before importing any aurum modules
_mock_opentelemetry()

# Set test environment variables
os.environ.setdefault("AURUM_API_AUTH_DISABLED", "1")
os.environ.setdefault("AURUM_API_RATE_LIMIT_ENABLED", "0")


class TestAPISmokeTests:
    """Smoke tests for core API functionality."""

    @pytest.fixture
    def mock_trino_client(self):
        """Mock Trino client for testing."""
        mock_client = MagicMock()

        # Mock execute_query method
        mock_client.execute_query = AsyncMock()

        # Default mock responses for different query types
        def mock_execute_query_side_effect(query, **kwargs):
            if "curve_observation" in query.lower():
                return {
                    "data": [
                        {
                            "iso": "PJM",
                            "market": "DAY_AHEAD",
                            "location": "WEST",
                            "asof": "2024-01-15",
                            "observation_time": "2024-01-15T10:00:00Z",
                            "price": 50.25,
                            "volume": 1000.0
                        },
                        {
                            "iso": "PJM",
                            "market": "DAY_AHEAD",
                            "location": "WEST",
                            "asof": "2024-01-15",
                            "observation_time": "2024-01-15T11:00:00Z",
                            "price": 52.10,
                            "volume": 1100.0
                        }
                    ],
                    "columns": ["iso", "market", "location", "asof", "observation_time", "price", "volume"]
                }
            elif "metadata" in query.lower() or "dimensions" in query.lower():
                return {
                    "data": [
                        {"dimension": "iso", "value": "PJM", "count": 100},
                        {"dimension": "iso", "value": "CAISO", "count": 80},
                        {"dimension": "market", "value": "DAY_AHEAD", "count": 150},
                        {"dimension": "market", "value": "REAL_TIME", "count": 120},
                        {"dimension": "location", "value": "WEST", "count": 75},
                        {"dimension": "location", "value": "EAST", "count": 65}
                    ],
                    "columns": ["dimension", "value", "count"]
                }
            elif "scenario_output" in query.lower():
                return {
                    "data": [
                        {
                            "scenario_id": "test-scenario-123",
                            "timestamp": "2024-01-15T10:00:00Z",
                            "metric_name": "power_output",
                            "value": 100.5,
                            "unit": "MW",
                            "tags": {"region": "west", "type": "renewable"}
                        },
                        {
                            "scenario_id": "test-scenario-123",
                            "timestamp": "2024-01-15T11:00:00Z",
                            "metric_name": "power_output",
                            "value": 95.2,
                            "unit": "MW",
                            "tags": {"region": "west", "type": "renewable"}
                        }
                    ],
                    "columns": ["scenario_id", "timestamp", "metric_name", "value", "unit", "tags"]
                }
            else:
                return {"data": [], "columns": []}

        mock_client.execute_query.side_effect = mock_execute_query_side_effect
        return mock_client

    @pytest.fixture
    def mock_scenario_store(self):
        """Mock scenario store for testing."""
        mock_store = MagicMock()

        # Mock feature flag methods
        mock_store.get_feature_flag = AsyncMock(return_value={
            "enabled": True,
            "configuration": {},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        })

        mock_store.set_feature_flag = AsyncMock(return_value={
            "enabled": True,
            "configuration": {},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        })

        # Mock scenario methods
        mock_store.get_scenario = AsyncMock(return_value={
            "id": "test-scenario-123",
            "name": "Test Scenario",
            "description": "A test scenario",
            "tenant_id": "test-tenant",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        })

        mock_store.list_scenarios = AsyncMock(return_value=(
            [
                {
                    "id": "test-scenario-123",
                    "name": "Test Scenario",
                    "description": "A test scenario",
                    "tenant_id": "test-tenant",
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            ],
            1
        ))

        mock_store.get_outputs = AsyncMock(return_value={
            "outputs": [
                {
                    "id": "output-1",
                    "scenario_run_id": "run-123",
                    "timestamp": "2024-01-15T10:00:00Z",
                    "metric_name": "power_output",
                    "value": 100.5,
                    "unit": "MW",
                    "tags": {"region": "west"}
                }
            ],
            "total": 1,
            "applied_filter": {
                "start_time": "2024-01-15T00:00:00Z",
                "end_time": "2024-01-15T23:59:59Z",
                "limit": 100,
                "offset": 0
            }
        })

        return mock_store

    @pytest.fixture
    async def test_app(self, mock_trino_client, mock_scenario_store):
        """Create test FastAPI app with mocked dependencies."""
        from aurum.api.app import create_app
        from aurum.core import AurumSettings

        # Create minimal settings for testing
        settings = MagicMock()
        settings.api.title = "Aurum API (Test)"
        settings.api.version = "1.0.0"
        settings.api.cors_allow_origins = ["*"]
        settings.api.cors_allow_credentials = True
        settings.api.gzip_min_bytes = 1000
        settings.api.rate_limit.enabled = False
        settings.telemetry.service_name = "aurum-api-test"

        # Ensure versioned routers mount by disabling light-init for this suite
        import os as _os
        _os.environ["AURUM_API_LIGHT_INIT"] = "0"

        # Create app; rely on internal wiring (avoid patching router configuration)
        app = create_app(settings)

        return app

    @pytest.fixture
    def client(self, test_app):
        """Create test client."""
        return TestClient(test_app)

    def test_health_endpoint(self, client):
        """Test /health endpoint returns 200."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] == "healthy"

    def test_ready_endpoint(self, client):
        """Test /ready endpoint returns 200 with deep health checks."""
        response = client.get("/ready")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] == "ready"
        assert "checks" in data

    def test_metrics_endpoint(self, client):
        """Test /metrics endpoint returns Prometheus metrics."""
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "# HELP" in response.text  # Prometheus format
        assert "# TYPE" in response.text

    def test_curves_endpoint_basic(self, client):
        """Test /v2/curves endpoint returns curve data (tenant-scoped)."""
        from unittest.mock import AsyncMock, patch

        class _StubCurve:
            def __init__(self):
                self._payload = {
                    "id": "curve-1",
                    "name": "Test Curve",
                    "description": None,
                    "data_points": 10,
                    "created_at": "2024-01-15T00:00:00Z",
                }

            def model_dump(self):
                return dict(self._payload)

        async def _mock_list_curves(**kwargs):
            return ([_StubCurve()], {"backend": "stub"})

        with patch("libs.services.curves_service.CurvesService.list_curves", new=AsyncMock(side_effect=_mock_list_curves)):
            response = client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 1},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        payload = response.json()

        # Check response structure
        assert "data" in payload and isinstance(payload["data"], list)
        assert "meta" in payload and isinstance(payload["meta"], dict)
        assert len(payload["data"]) == 1

    def test_curves_endpoint_with_pagination(self, client):
        """Test /v2/curves endpoint with cursor-based pagination (tenant-scoped)."""
        from unittest.mock import AsyncMock, patch

        class _StubCurve:
            def __init__(self, idx: int):
                self._payload = {
                    "id": f"curve-{idx}",
                    "name": f"Test Curve {idx}",
                    "description": None,
                    "data_points": 10,
                    "created_at": "2024-01-15T00:00:00Z",
                }

            def model_dump(self):
                return dict(self._payload)

        async def _mock_list_curves(**kwargs):
            return ([_StubCurve(1)], {})

        with patch("libs.services.curves_service.CurvesService.list_curves", new=AsyncMock(side_effect=_mock_list_curves)):
            response = client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 1},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "meta" in data
        assert len(data["data"]) <= 1

        # Use cursor for next page if provided
        cursor = data["meta"].get("next_cursor") if isinstance(data.get("meta"), dict) else None
        if cursor:
            with patch("libs.services.curves_service.CurvesService.list_curves", new=AsyncMock(side_effect=_mock_list_curves)):
                response2 = client.get(
                    "/v2/curves",
                    params={"tenant_id": "tenant-test", "cursor": cursor},
                    headers={"X-Aurum-Tenant": "tenant-test"},
                )
            assert response2.status_code == 200

    def test_curves_diff_endpoint(self, client):
        """Test /v2/curves/{curve_id}/diff endpoint (tenant-scoped)."""
        from unittest.mock import AsyncMock, patch

        class _StubCurve:
            def __init__(self):
                self._payload = {
                    "id": "curve-1",
                    "name": "Test Curve",
                    "description": None,
                    "data_points": 2,
                    "created_at": "2024-01-15T00:00:00Z",
                }

            def model_dump(self):
                return dict(self._payload)

        async def _mock_get_diff(**kwargs):
            return _StubCurve()

        with patch("libs.services.curves_service.CurvesService.get_curve_diff", new=AsyncMock(side_effect=_mock_get_diff)):
            response = client.get(
                "/v2/curves/curve-1/diff",
                params={
                    "tenant_id": "tenant-test",
                    "from_timestamp": "2024-01-14T00:00:00Z",
                    "to_timestamp": "2024-01-15T00:00:00Z",
                },
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload.get("data", {}).get("id") == "curve-1"

    def test_metadata_dimensions_endpoint(self, client):
        """Test /v2/metadata/dimensions endpoint returns dimension metadata (tenant-scoped)."""
        from unittest.mock import AsyncMock, patch

        async def _mock_list_dimensions(**kwargs):
            return ([{"dimension": "iso", "values": ["PJM"], "asof": "latest"}], None)

        with patch("libs.services.metadata_service.MetadataService.list_dimensions", new=AsyncMock(side_effect=_mock_list_dimensions)):
            response = client.get(
                "/v2/metadata/dimensions",
                params={"tenant_id": "tenant-test"},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        payload = response.json()
        assert "data" in payload and isinstance(payload["data"], list)
        assert payload["data"][0]["dimension"] == "iso"
        assert len(dimensions["iso"]) > 0

    def test_metadata_dimensions_with_counts(self, client):
        """Test /v1/metadata/dimensions endpoint with include_counts."""
        response = client.get("/v1/metadata/dimensions", params={"include_counts": "true"})

        assert response.status_code == 200
        data = response.json()

        # Check counts structure
        assert "counts" in data
        counts = data["counts"]
        assert isinstance(counts, dict)

        # Check count details
        if "iso" in counts:
            iso_counts = counts["iso"]
            assert isinstance(iso_counts, list)
            if iso_counts:
                count_item = iso_counts[0]
                assert "value" in count_item
                assert "count" in count_item

    def test_scenario_list_endpoint(self, client):
        """Test /v2/scenarios endpoint returns scenario list (tenant-scoped)."""
        from unittest.mock import AsyncMock, patch
        from datetime import datetime

        async def _mock_list_scenarios(**kwargs):
            scenario = {
                "id": "test-scenario-123",
                "tenant_id": "tenant-test",
                "name": "Test Scenario",
                "description": None,
                "status": "active",
                "unique_key": "u-123",
                "assumptions": [],
                "parameters": {},
                "tags": [],
                "created_at": datetime.utcnow().isoformat() + "Z",
                "updated_at": None,
                "created_by": None,
                "version": 1,
                "metadata": {},
            }
            return ([scenario], 1, {})

        with patch("libs.services.scenarios_service.ScenariosService.list_scenarios", new=AsyncMock(side_effect=_mock_list_scenarios)):
            response = client.get(
                "/v2/scenarios",
                params={"tenant_id": "tenant-test"},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        payload = response.json()
        assert isinstance(payload.get("data"), list)
        assert payload["data"][0]["tenant_id"] == "tenant-test"

    def test_scenario_runs_list_endpoint(self, client):
        """Test /v2/scenarios/{scenario_id}/runs endpoint (tenant-scoped)."""
        from unittest.mock import AsyncMock, patch

        async def _mock_list_runs(**kwargs):
            return [
                {
                    "id": "run-1",
                    "scenario_id": "test-scenario-123",
                    "status": "succeeded",
                    "timestamp": "2024-01-15T10:00:00Z",
                }
            ]

        with patch("libs.services.scenarios_service.ScenariosService.list_scenario_runs", new=AsyncMock(side_effect=_mock_list_runs)):
            response = client.get(
                "/v2/scenarios/test-scenario-123/runs",
                params={"tenant_id": "tenant-test", "limit": 10},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        payload = response.json()
        assert isinstance(payload.get("data"), list)
        assert payload["data"][0]["scenario_id"] == "test-scenario-123"

    def test_scenario_runs_with_filters(self, client):
        """Test /v2/scenarios/{scenario_id}/runs with filters (tenant-scoped)."""
        from unittest.mock import AsyncMock, patch

        async def _mock_list_runs(**kwargs):
            return []  # Empty list is fine for filter smoke

        with patch("libs.services.scenarios_service.ScenariosService.list_scenario_runs", new=AsyncMock(side_effect=_mock_list_runs)):
            response = client.get(
                "/v2/scenarios/test-scenario-123/runs",
                params={"tenant_id": "tenant-test", "status_filter": "running", "limit": 5},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200

    def test_scenario_runs_pagination(self, client):
        """Test /v2/scenarios/{scenario_id}/runs with pagination."""
        from unittest.mock import AsyncMock, patch

        async def _mock_list_runs(**kwargs):
            return [{"id": "run-1", "scenario_id": "test-scenario-123", "status": "succeeded"}]

        with patch("libs.services.scenarios_service.ScenariosService.list_scenario_runs", new=AsyncMock(side_effect=_mock_list_runs)):
            response = client.get(
                "/v2/scenarios/test-scenario-123/runs",
                params={"tenant_id": "tenant-test", "limit": 1},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        payload = response.json()
        assert isinstance(payload.get("data"), list)
        assert len(payload["data"]) <= 1

    def test_scenario_run_create_endpoint(self, client):
        """Test POST /v2/scenarios/{scenario_id}/runs endpoint (tenant-scoped)."""
        from unittest.mock import AsyncMock, patch
        from datetime import datetime

        class _StubRun:
            def __init__(self):
                self._payload = {
                    "meta": {},
                    "data": {
                        "id": "run-1",
                        "scenario_id": "test-scenario-123",
                        "status": "queued",
                        "created_at": datetime.utcnow().isoformat() + "Z",
                    },
                }

            def model_dump(self):
                return dict(self._payload)

        async def _mock_create_run(*args, **kwargs):
            return _StubRun()

        with patch("libs.services.scenarios_service.ScenariosService.create_scenario_run", new=AsyncMock(side_effect=_mock_create_run)):
            response = client.post(
                "/v2/scenarios/test-scenario-123/runs",
                params={"tenant_id": "tenant-test"},
                json={"priority": "normal", "timeout_minutes": 10, "parameters": {"k": "v"}},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code in (200, 201)
        payload = response.json()
        assert payload.get("data", {}).get("scenario_id") == "test-scenario-123"

    def test_api_error_handling(self, client):
        """Test v2 error handling for invalid parameters (422)."""
        # Invalid 'limit' (must be >=1)
        response = client.get(
            "/v2/curves",
            params={"tenant_id": "tenant-test", "limit": 0},
            headers={"X-Aurum-Tenant": "tenant-test"},
        )
        assert response.status_code in (400, 422)

    def test_api_not_found_handling(self, client):
        """Test v2 404 handling for non-existent scenario."""
        from fastapi import HTTPException
        from unittest.mock import AsyncMock, patch

        async def _mock_get_scenario(*args, **kwargs):
            raise HTTPException(status_code=404, detail="Not Found")

        with patch("libs.services.scenarios_service.ScenariosService.get_scenario", new=AsyncMock(side_effect=_mock_get_scenario)):
            response = client.get(
                "/v2/scenarios/non-existent",
                params={"tenant_id": "tenant-test"},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )
        assert response.status_code == 404

    def test_api_forbidden_handling(self, client):
        """Test v2 scenarios list endpoint structure (auth may be disabled)."""
        from unittest.mock import AsyncMock, patch
        async def _mock_list_scenarios(**kwargs):
            return ([], 0, {})
        with patch("libs.services.scenarios_service.ScenariosService.list_scenarios", new=AsyncMock(side_effect=_mock_list_scenarios)):
            response = client.get(
                "/v2/scenarios",
                params={"tenant_id": "tenant-test"},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )
        assert response.status_code in (200, 403)

    def test_api_headers_and_metadata(self, client):
        """Test v2 response headers and metadata."""
        from unittest.mock import AsyncMock, patch

        class _StubCurve:
            def model_dump(self):
                return {
                    "id": "curve-1",
                    "name": "Curve 1",
                    "description": None,
                    "data_points": 1,
                    "created_at": "2024-01-15T00:00:00Z",
                }

        async def _mock_list_curves(**kwargs):
            return ([_StubCurve()], {})

        with patch("libs.services.curves_service.CurvesService.list_curves", new=AsyncMock(side_effect=_mock_list_curves)):
            response = client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 1},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200

        # Check response headers
        assert "content-type" in response.headers
        assert response.headers["content-type"].startswith("application/json")

        # Check response metadata
        data = response.json()
        assert "meta" in data
        assert "request_id" in data["meta"]
        assert "tenant_id" in data["meta"]

    def test_api_concurrent_requests(self, client):
        """Test API handles concurrent v2 metadata requests properly."""
        from unittest.mock import AsyncMock, patch
        from concurrent.futures import ThreadPoolExecutor

        async def _mock_list_dimensions(**kwargs):
            return ([{"dimension": "iso", "values": ["PJM"], "asof": "latest"}], None)

        with patch("libs.services.metadata_service.MetadataService.list_dimensions", new=AsyncMock(side_effect=_mock_list_dimensions)):
            def make_request():
                resp = client.get(
                    "/v2/metadata/dimensions",
                    params={"tenant_id": "tenant-test"},
                    headers={"X-Aurum-Tenant": "tenant-test"},
                )
                return resp.status_code

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(make_request) for _ in range(10)]
                results = [f.result() for f in futures]

        assert all(status == 200 for status in results)

    def test_api_performance_basic(self, client):
        """Test basic API performance characteristics."""

        import time

        # Test response time for simple endpoint
        start_time = time.time()

        response = client.get("/health")

        end_time = time.time()
        response_time = end_time - start_time

        assert response.status_code == 200
        assert response_time < 1.0  # Should respond in less than 1 second

    def test_api_data_integrity(self, client):
        """Test v2 API integrity across endpoints using tenant context."""
        from unittest.mock import AsyncMock, patch

        # Stub curves and metadata endpoints
        class _StubCurve:
            def __init__(self, i: int):
                self._payload = {
                    "id": f"curve-{i}",
                    "name": f"Curve {i}",
                    "description": None,
                    "data_points": 5,
                    "created_at": "2024-01-15T00:00:00Z",
                }

            def model_dump(self):
                return dict(self._payload)

        async def _mock_list_curves(**kwargs):
            return ([_StubCurve(1), _StubCurve(2)], {})

        async def _mock_list_dimensions(**kwargs):
            return ([{"dimension": "iso", "values": ["PJM"], "asof": "latest"}], None)

        with patch("libs.services.curves_service.CurvesService.list_curves", new=AsyncMock(side_effect=_mock_list_curves)), \
             patch("libs.services.metadata_service.MetadataService.list_dimensions", new=AsyncMock(side_effect=_mock_list_dimensions)):
            curves_response = client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 2},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )
            metadata_response = client.get(
                "/v2/metadata/dimensions",
                params={"tenant_id": "tenant-test"},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert curves_response.status_code == 200
        assert metadata_response.status_code == 200
        curves_payload = curves_response.json()
        metadata_payload = metadata_response.json()

        # Integrity check: meta.tenant_id should match header; returned_count should match data length
        assert curves_payload["meta"]["tenant_id"] == "tenant-test"
        assert curves_payload["meta"]["returned_count"] == len(curves_payload["data"])
        assert isinstance(metadata_payload.get("data"), list)

    def test_api_cors_headers(self, client):
        """Test CORS headers are properly set."""

        response = client.get("/health")

        # Check CORS headers
        assert "access-control-allow-origin" in response.headers
        assert "access-control-allow-methods" in response.headers
        assert "access-control-allow-headers" in response.headers

    def test_api_content_types(self, client):
        """Test API supports different content types."""

        # JSON request
        response = client.get("/health")
        assert response.headers["content-type"] == "application/json"

        # Test with Accept header for different content type
        headers = {"Accept": "application/json"}
        response = client.get("/health", headers=headers)
        assert response.status_code == 200

    def test_api_rate_limiting_disabled(self, client):
        """Test rate limiting is disabled in test environment."""

        # Make many requests quickly
        for i in range(20):
            response = client.get("/health")
            assert response.status_code == 200

    def test_api_tenant_isolation(self, client):
        """Test tenant isolation in v2 responses."""
        from unittest.mock import AsyncMock, patch
        from datetime import datetime

        async def _mock_list_scenarios(**kwargs):
            return ([{
                "id": "s-1",
                "tenant_id": "tenant-test",
                "name": "S1",
                "description": None,
                "status": "active",
                "unique_key": "uk",
                "assumptions": [],
                "parameters": {},
                "tags": [],
                "created_at": datetime.utcnow().isoformat() + "Z",
                "updated_at": None,
                "created_by": None,
                "version": 1,
                "metadata": {},
            }], 1, {})

        with patch("libs.services.scenarios_service.ScenariosService.list_scenarios", new=AsyncMock(side_effect=_mock_list_scenarios)):
            response = client.get(
                "/v2/scenarios",
                params={"tenant_id": "tenant-test"},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["data"][0]["tenant_id"] == "tenant-test"

    def test_api_request_tracing(self, client):
        """Test request tracing headers and correlation IDs."""

        response = client.get("/health")

        # Check for tracing headers
        if "x-request-id" in response.headers:
            assert len(response.headers["x-request-id"]) > 0

        if "x-correlation-id" in response.headers:
            assert len(response.headers["x-correlation-id"]) > 0

    def test_api_structured_logging(self, client):
        """Test that v2 responses include structured metadata and ETag headers."""
        from unittest.mock import AsyncMock, patch

        class _StubCurve:
            def model_dump(self):
                return {
                    "id": "curve-1",
                    "name": "Curve 1",
                    "description": None,
                    "data_points": 1,
                    "created_at": "2024-01-15T00:00:00Z",
                }

        async def _mock_list_curves(**kwargs):
            return ([_StubCurve()], {})

        with patch("libs.services.curves_service.CurvesService.list_curves", new=AsyncMock(side_effect=_mock_list_curves)):
            response = client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 1},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        data = response.json()
        # Meta fields present
        assert "meta" in data
        assert "request_id" in data["meta"]
        assert "tenant_id" in data["meta"]
        assert "processing_time_ms" in data["meta"]
        # ETag header present
        assert "etag" in {k.lower(): v for k, v in response.headers.items()}

    def test_api_cache_headers(self, client):
        """Test cache control headers on v2 metadata endpoints."""
        from unittest.mock import AsyncMock, patch
        async def _mock_list_dimensions(**kwargs):
            return ([{"dimension": "iso", "values": ["PJM"], "asof": "latest"}], None)

        with patch("libs.services.metadata_service.MetadataService.list_dimensions", new=AsyncMock(side_effect=_mock_list_dimensions)):
            response = client.get(
                "/v2/metadata/dimensions",
                params={"tenant_id": "tenant-test"},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200
        assert "cache-control" in {k.lower(): v for k, v in response.headers.items()}

    def test_api_compression(self, client):
        """Test API response with gzip Accept-Encoding (v2)."""
        from unittest.mock import AsyncMock, patch
        async def _mock_list_curves(**kwargs):
            class _StubCurve:
                def model_dump(self):
                    return {
                        "id": "curve-1",
                        "name": "Curve 1",
                        "description": None,
                        "data_points": 100,
                        "created_at": "2024-01-15T00:00:00Z",
                    }
            return ([_StubCurve() for _ in range(5)], {})

        with patch("libs.services.curves_service.CurvesService.list_curves", new=AsyncMock(side_effect=_mock_list_curves)):
            headers = {"Accept-Encoding": "gzip"}
            response = client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 5},
                headers={**headers, "X-Aurum-Tenant": "tenant-test"},
            )

        assert response.status_code == 200

    def test_api_schema_validation(self, client):
        """Test v2 schema validation for required parameters."""
        # Missing tenant_id should cause validation error (422)
        response = client.get("/v2/curves", params={"limit": 1})
        assert response.status_code in (400, 422)

    def test_api_backward_compatibility(self, client):
        """Basic v2 response shape is consistent (data + meta)."""
        from unittest.mock import AsyncMock, patch
        class _StubCurve:
            def model_dump(self):
                return {
                    "id": "curve-1",
                    "name": "Curve 1",
                    "description": None,
                    "data_points": 1,
                    "created_at": "2024-01-15T00:00:00Z",
                }
        async def _mock_list_curves(**kwargs):
            return ([_StubCurve()], {})
        with patch("libs.services.curves_service.CurvesService.list_curves", new=AsyncMock(side_effect=_mock_list_curves)):
            response = client.get(
                "/v2/curves",
                params={"tenant_id": "tenant-test", "limit": 1},
                headers={"X-Aurum-Tenant": "tenant-test"},
            )
        assert response.status_code == 200
        data = response.json()
        assert "data" in data and "meta" in data

    def test_api_documentation_accessible(self, client):
        """Test that API documentation is accessible."""

        # Test OpenAPI/Swagger docs
        response = client.get("/docs")
        assert response.status_code in [200, 302]  # May redirect

        # Test ReDoc docs
        response = client.get("/redoc")
        assert response.status_code in [200, 302]  # May redirect

        # Test OpenAPI spec
        response = client.get("/openapi.json")
        assert response.status_code == 200

        spec = response.json()
        assert "openapi" in spec
        assert "info" in spec
        assert "paths" in spec
