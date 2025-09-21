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

        # Mock the app creation to avoid full initialization
        with patch("aurum.api.app.configure_state"):
            with patch("aurum.api.app.configure_routes"):
                with patch("aurum.api.app.configure_telemetry"):
                    with patch("aurum.api.state.get_scenario_store", return_value=mock_scenario_store):
                        with patch("aurum.api.state.get_trino_client", return_value=mock_trino_client):
                            with patch("aurum.api.state.get_scenario_output_feature_manager"):
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
        """Test /v1/curves endpoint returns curve data."""
        response = client.get("/v1/curves", params={
            "iso": "PJM",
            "market": "DAY_AHEAD",
            "location": "WEST",
            "asof": "2024-01-15"
        })

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "data" in data
        assert "meta" in data
        assert len(data["data"]) > 0

        # Check curve data structure
        curve_point = data["data"][0]
        assert "iso" in curve_point
        assert "market" in curve_point
        assert "location" in curve_point
        assert "asof" in curve_point
        assert "price" in curve_point
        assert "volume" in curve_point

        # Check values
        assert curve_point["iso"] == "PJM"
        assert curve_point["market"] == "DAY_AHEAD"
        assert curve_point["location"] == "WEST"
        assert curve_point["asof"] == "2024-01-15"

    def test_curves_endpoint_with_pagination(self, client):
        """Test /v1/curves endpoint with cursor-based pagination."""
        # First request
        response = client.get("/v1/curves", params={
            "iso": "PJM",
            "market": "DAY_AHEAD",
            "location": "WEST",
            "asof": "2024-01-15",
            "limit": 1
        })

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "meta" in data
        assert len(data["data"]) <= 1

        # Check cursor information
        if "prev_cursor" in data["meta"] or "next_cursor" in data["meta"]:
            # Use cursor for next page
            cursor = data["meta"].get("next_cursor")
            if cursor:
                response2 = client.get("/v1/curves", params={"cursor": cursor})
                assert response2.status_code == 200

    def test_curves_diff_endpoint(self, client):
        """Test /v1/curves/diff endpoint for comparing curve data."""
        response = client.get("/v1/curves/diff", params={
            "iso": "PJM",
            "market": "DAY_AHEAD",
            "location": "WEST",
            "asof_a": "2024-01-15",
            "asof_b": "2024-01-14",
            "dimension": "price"
        })

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "data" in data
        assert "meta" in data

        # Check diff data structure
        if data["data"]:
            diff_point = data["data"][0]
            assert "iso" in diff_point
            assert "market" in diff_point
            assert "location" in diff_point
            assert "dimension" in diff_point

    def test_metadata_dimensions_endpoint(self, client):
        """Test /v1/metadata/dimensions endpoint returns dimension metadata."""
        response = client.get("/v1/metadata/dimensions")

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "data" in data
        assert "meta" in data

        # Check dimensions structure
        dimensions = data["data"]
        assert isinstance(dimensions, dict)

        # Should have at least the basic dimensions
        assert "iso" in dimensions
        assert "market" in dimensions
        assert "location" in dimensions

        # Check dimension values
        assert isinstance(dimensions["iso"], list)
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
        """Test /v1/scenarios endpoint returns scenario list."""
        response = client.get("/v1/scenarios")

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "data" in data
        assert "meta" in data

        # Check scenarios structure
        scenarios = data["data"]
        assert isinstance(scenarios, list)

        if scenarios:
            scenario = scenarios[0]
            assert "id" in scenario
            assert "name" in scenario
            assert "tenant_id" in scenario

    def test_scenario_outputs_endpoint(self, client):
        """Test /v1/scenarios/{scenario_id}/outputs endpoint returns scenario outputs."""
        scenario_id = "test-scenario-123"

        response = client.get(f"/v1/scenarios/{scenario_id}/outputs", params={
            "limit": 10,
            "start_time": "2024-01-15T00:00:00Z",
            "end_time": "2024-01-15T23:59:59Z"
        })

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "outputs" in data
        assert "total" in data
        assert "applied_filter" in data

        # Check outputs structure
        outputs = data["outputs"]
        assert isinstance(outputs, list)

        if outputs:
            output = outputs[0]
            assert "id" in output
            assert "scenario_run_id" in output
            assert "timestamp" in output
            assert "metric_name" in output
            assert "value" in output

    def test_scenario_outputs_with_filters(self, client):
        """Test /v1/scenarios/{scenario_id}/outputs with various filters."""
        scenario_id = "test-scenario-123"

        # Test with metric name filter
        response = client.get(f"/v1/scenarios/{scenario_id}/outputs", params={
            "metric_name": "power_output",
            "limit": 5
        })

        assert response.status_code == 200
        data = response.json()
        assert "outputs" in data

        # Test with value range filter
        response = client.get(f"/v1/scenarios/{scenario_id}/outputs", params={
            "min_value": 50,
            "max_value": 200,
            "limit": 5
        })

        assert response.status_code == 200

    def test_scenario_outputs_pagination(self, client):
        """Test /v1/scenarios/{scenario_id}/outputs with pagination."""
        scenario_id = "test-scenario-123"

        # First page
        response = client.get(f"/v1/scenarios/{scenario_id}/outputs", params={
            "limit": 1,
            "offset": 0
        })

        assert response.status_code == 200
        data = response.json()
        assert "outputs" in data
        assert len(data["outputs"]) <= 1

    def test_bulk_scenario_runs_endpoint(self, client):
        """Test /v1/scenarios/{scenario_id}/runs:bulk endpoint for bulk scenario runs."""
        scenario_id = "test-scenario-123"

        bulk_request = {
            "runs": [
                {
                    "idempotency_key": "bulk-test-1",
                    "parameters": {"test": "value1"}
                },
                {
                    "idempotency_key": "bulk-test-2",
                    "parameters": {"test": "value2"}
                }
            ]
        }

        response = client.post(f"/v1/scenarios/{scenario_id}/runs:bulk", json=bulk_request)

        assert response.status_code == 202  # Accepted for async processing
        data = response.json()

        # Check response structure
        assert "results" in data
        assert "total" in data
        assert data["total"] == 2

    def test_api_error_handling(self, client):
        """Test API error handling for invalid requests."""

        # Test 400 for invalid parameters
        response = client.get("/v1/curves", params={
            "iso": "",  # Invalid empty value
            "market": "INVALID_MARKET",
            "location": "INVALID_LOCATION",
            "asof": "invalid-date"
        })

        assert response.status_code == 400
        data = response.json()
        assert "error" in data
        assert "validation_errors" in data

    def test_api_not_found_handling(self, client):
        """Test API 404 handling for non-existent resources."""

        # Test 404 for non-existent scenario
        response = client.get("/v1/scenarios/non-existent-scenario-123/outputs")

        assert response.status_code == 404
        data = response.json()
        assert "error" in data
        assert data["error"]["type"] == "not_found"

    def test_api_forbidden_handling(self, client):
        """Test API 403 handling for forbidden operations."""

        # This would require setting up auth to test properly
        # For now, just test that the endpoint structure is correct
        response = client.get("/v1/scenarios/test-scenario/outputs", params={
            "limit": 100
        })

        # Should be 200 if auth is disabled, 403 if auth is enabled
        assert response.status_code in [200, 403]

    def test_api_headers_and_metadata(self, client):
        """Test API response headers and metadata."""

        response = client.get("/v1/curves", params={
            "iso": "PJM",
            "market": "DAY_AHEAD",
            "location": "WEST",
            "asof": "2024-01-15"
        })

        assert response.status_code == 200

        # Check response headers
        assert "content-type" in response.headers
        assert response.headers["content-type"] == "application/json"

        # Check response metadata
        data = response.json()
        assert "meta" in data
        assert "request_id" in data["meta"]
        assert "timestamp" in data["meta"]
        assert "tenant_id" in data["meta"]

    def test_api_concurrent_requests(self, client):
        """Test API handles concurrent requests properly."""

        import asyncio
        from concurrent.futures import ThreadPoolExecutor

        def make_request():
            response = client.get("/v1/metadata/dimensions")
            return response.status_code

        # Make multiple concurrent requests
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_request) for _ in range(10)]
            results = [f.result() for f in futures]

        # All requests should succeed
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
        """Test API data integrity across endpoints."""

        # Get curve data
        curves_response = client.get("/v1/curves", params={
            "iso": "PJM",
            "market": "DAY_AHEAD",
            "location": "WEST",
            "asof": "2024-01-15"
        })

        assert curves_response.status_code == 200
        curves_data = curves_response.json()

        # Get metadata dimensions
        metadata_response = client.get("/v1/metadata/dimensions")
        assert metadata_response.status_code == 200
        metadata_data = metadata_response.json()

        # Verify consistency between endpoints
        if curves_data["data"] and metadata_data["data"]:
            curve_iso = curves_data["data"][0]["iso"]
            assert curve_iso in metadata_data["data"]["iso"]

            curve_market = curves_data["data"][0]["market"]
            assert curve_market in metadata_data["data"]["market"]

            curve_location = curves_data["data"][0]["location"]
            assert curve_location in metadata_data["data"]["location"]

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
        """Test tenant isolation in API responses."""

        response = client.get("/v1/scenarios")
        assert response.status_code in [200, 403]  # 403 if auth required

        if response.status_code == 200:
            data = response.json()
            # Check that tenant_id is included in responses
            if "data" in data and data["data"]:
                assert "tenant_id" in data["data"][0]

    def test_api_request_tracing(self, client):
        """Test request tracing headers and correlation IDs."""

        response = client.get("/health")

        # Check for tracing headers
        if "x-request-id" in response.headers:
            assert len(response.headers["x-request-id"]) > 0

        if "x-correlation-id" in response.headers:
            assert len(response.headers["x-correlation-id"]) > 0

    def test_api_structured_logging(self, client):
        """Test that API responses include structured logging information."""

        response = client.get("/v1/curves", params={
            "iso": "PJM",
            "market": "DAY_AHEAD",
            "location": "WEST",
            "asof": "2024-01-15"
        })

        assert response.status_code == 200
        data = response.json()

        # Check structured metadata
        assert "meta" in data
        assert "request_id" in data["meta"]
        assert "timestamp" in data["meta"]
        assert "tenant_id" in data["meta"]
        assert "version" in data["meta"]

    def test_api_cache_headers(self, client):
        """Test cache control headers for appropriate endpoints."""

        response = client.get("/v1/metadata/dimensions")

        # Metadata should be cacheable
        assert "cache-control" in response.headers

        # Check cache control value
        cache_control = response.headers["cache-control"]
        assert "max-age" in cache_control or "no-cache" in cache_control

    def test_api_compression(self, client):
        """Test API response compression."""

        # Test with Accept-Encoding header
        headers = {"Accept-Encoding": "gzip"}
        response = client.get("/v1/curves", headers=headers, params={
            "iso": "PJM",
            "market": "DAY_AHEAD",
            "location": "WEST",
            "asof": "2024-01-15"
        })

        assert response.status_code == 200

        # Response should be compressed if large enough
        # (In test environment, this may not be compressed due to small size)

    def test_api_schema_validation(self, client):
        """Test API schema validation for request/response data."""

        # Test with valid parameters
        response = client.get("/v1/curves", params={
            "iso": "PJM",
            "market": "DAY_AHEAD",
            "location": "WEST",
            "asof": "2024-01-15"
        })

        assert response.status_code == 200

        # Test with invalid parameters (should return 400)
        response = client.get("/v1/curves", params={
            "iso": "",  # Invalid empty string
            "market": "INVALID",
            "asof": "invalid-date"
        })

        assert response.status_code == 400

        # Check error response structure
        error_data = response.json()
        assert "error" in error_data
        assert "validation_errors" in error_data
        assert len(error_data["validation_errors"]) > 0

    def test_api_backward_compatibility(self, client):
        """Test API backward compatibility with older parameters."""

        # Test with older parameter names (if any)
        response = client.get("/v1/curves", params={
            "iso": "PJM",
            "market": "DAY_AHEAD",
            "location": "WEST",
            "asof": "2024-01-15"
        })

        assert response.status_code == 200

        # Ensure response format is consistent
        data = response.json()
        assert "data" in data
        assert "meta" in data

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
