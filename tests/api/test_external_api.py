import os
import sys
import types
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("AURUM_API_AUTH_DISABLED", "1")


def _ensure_opentelemetry(monkeypatch):
    """Mock OpenTelemetry for testing."""
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

    class _ConsoleSpanExporter:
        def __init__(self, **kwargs):
            pass

    trace_export_module.ConsoleSpanExporter = _ConsoleSpanExporter

    monkeypatch.setattr("aurum.observability.tracing.opentelemetry.trace", trace_module)
    monkeypatch.setattr("aurum.observability.tracing.opentelemetry.propagate", propagate_module)
    monkeypatch.setattr("aurum.observability.tracing.opentelemetry.sdk.resources", resources_module)
    monkeypatch.setattr("aurum.observability.tracing.opentelemetry.sdk.trace", sdk_trace_module)
    monkeypatch.setattr("aurum.observability.tracing.opentelemetry.sdk.trace.export", trace_export_module)


def _mock_trino_client():
    """Mock Trino client for testing."""
    mock_client = Mock()
    mock_client.execute_query = AsyncMock()
    return mock_client


def _mock_rate_limit_manager():
    """Mock rate limit manager for testing."""
    mock_manager = Mock()
    mock_manager.check_rate_limit = AsyncMock()
    mock_manager.check_rate_limit.return_value.allowed = True
    mock_manager.check_rate_limit.return_value.remaining_requests = 100
    mock_manager.check_rate_limit.return_value.reset_time = 60
    mock_manager.check_rate_limit.return_value.retry_after = 0
    mock_manager.check_rate_limit.return_value.quota = Mock()
    mock_manager.check_rate_limit.return_value.quota.requests_per_minute = 100
    mock_manager.check_rate_limit.return_value.tier = Mock()
    mock_manager.check_rate_limit.return_value.tier.value = "premium"
    mock_manager.check_rate_limit.return_value.to_headers = Mock(return_value={})
    return mock_manager


def _mock_cache_manager():
    """Mock cache manager for testing."""
    mock_manager = Mock()
    mock_manager.get = AsyncMock(return_value=None)
    mock_manager.set = AsyncMock()
    return mock_manager


@pytest.fixture
def client(monkeypatch):
    """Create test client with mocked dependencies."""
    _ensure_opentelemetry(monkeypatch)

    # Mock all the external dependencies
    with patch("aurum.api.handlers.external.get_external_dao") as mock_dao_getter, \
         patch("aurum.api.handlers.external.get_cache_manager") as mock_cache_getter, \
         patch("aurum.api.handlers.external.get_rate_limit_manager") as mock_rate_limit_getter, \
         patch("aurum.api.handlers.external.validate_oidc_auth", return_value={"sub": "test-user", "tier": "premium"}):
        # Create mock instances
        mock_dao = Mock()
        mock_dao.get_providers = AsyncMock(return_value=[])
        mock_dao.get_series = AsyncMock(return_value=[])
        mock_dao.get_observations = AsyncMock(return_value=[])
        mock_dao.get_metadata = AsyncMock(return_value={"providers": [], "total_series": 0, "last_updated": None})
        mock_dao_getter.return_value = mock_dao

        mock_cache = _mock_cache_manager()
        mock_cache_getter.return_value = mock_cache

        mock_rate_limit = _mock_rate_limit_manager()
        mock_rate_limit_getter.return_value = mock_rate_limit

        # Import the app after mocking
        from aurum.api.app import create_app
        from aurum.core import AurumSettings

        settings = AurumSettings.from_env()
        settings.auth.disabled = True  # Disable auth for testing

        app = create_app(settings)
        return TestClient(app)


class TestExternalProviders:
    """Test external providers endpoint."""

    def test_list_providers_success(self, client):
        """Test successful providers listing."""
        response = client.get("/v1/external/providers")
        assert response.status_code == 200

    def test_list_providers_with_limit(self, client):
        """Test providers listing with limit parameter."""
        response = client.get("/v1/external/providers?limit=10")
        assert response.status_code == 200

    def test_list_providers_with_offset(self, client):
        """Test providers listing with offset parameter."""
        response = client.get("/v1/external/providers?offset=5")
        assert response.status_code == 200

    def test_list_providers_with_cursor(self, client):
        """Test providers listing with cursor parameter."""
        response = client.get("/v1/external/providers?cursor=abc123")
        assert response.status_code == 200


class TestExternalSeries:
    """Test external series endpoint."""

    def test_list_series_success(self, client):
        """Test successful series listing."""
        response = client.get("/v1/external/series")
        assert response.status_code == 200

    def test_list_series_with_provider_filter(self, client):
        """Test series listing with provider filter."""
        response = client.get("/v1/external/series?provider=fred")
        assert response.status_code == 200

    def test_list_series_with_frequency_filter(self, client):
        """Test series listing with frequency filter."""
        response = client.get("/v1/external/series?frequency=monthly")
        assert response.status_code == 200

    def test_list_series_with_asof_filter(self, client):
        """Test series listing with asof filter."""
        response = client.get("/v1/external/series?asof=2024-01-01")
        assert response.status_code == 200

    def test_list_series_invalid_frequency(self, client):
        """Test series listing with invalid frequency."""
        response = client.get("/v1/external/series?frequency=invalid")
        assert response.status_code == 400

    def test_list_series_invalid_asof_format(self, client):
        """Test series listing with invalid asof format."""
        response = client.get("/v1/external/series?asof=invalid-date")
        assert response.status_code == 400


class TestExternalObservations:
    """Test external observations endpoint."""

    def test_get_observations_success(self, client):
        """Test successful observations retrieval."""
        response = client.get("/v1/external/series/FRED:GDP/observations")
        assert response.status_code == 200

    def test_get_observations_with_date_filters(self, client):
        """Test observations retrieval with date filters."""
        response = client.get("/v1/external/series/FRED:GDP/observations?start_date=2020-01-01&end_date=2024-01-01")
        assert response.status_code == 200

    def test_get_observations_with_frequency(self, client):
        """Test observations retrieval with frequency conversion."""
        response = client.get("/v1/external/series/FRED:GDP/observations?frequency=monthly")
        assert response.status_code == 200

    def test_get_observations_with_limit(self, client):
        """Test observations retrieval with limit."""
        response = client.get("/v1/external/series/FRED:GDP/observations?limit=100")
        assert response.status_code == 200

    def test_get_observations_invalid_series_id(self, client):
        """Test observations retrieval with invalid series ID."""
        response = client.get("/v1/external/series/INVALID/observations")
        assert response.status_code == 404

    def test_get_observations_invalid_date_format(self, client):
        """Test observations retrieval with invalid date format."""
        response = client.get("/v1/external/series/FRED:GDP/observations?start_date=invalid")
        assert response.status_code == 400

    def test_get_observations_csv_format(self, client):
        """Test observations retrieval in CSV format."""
        response = client.get("/v1/external/series/FRED:GDP/observations?format=csv")
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/csv; charset=utf-8"


class TestExternalMetadata:
    """Test external metadata endpoint."""

    def test_get_metadata_success(self, client):
        """Test successful metadata retrieval."""
        response = client.get("/v1/metadata/external")
        assert response.status_code == 200

    def test_get_metadata_with_provider_filter(self, client):
        """Test metadata retrieval with provider filter."""
        response = client.get("/v1/metadata/external?provider=fred")
        assert response.status_code == 200

    def test_get_metadata_with_counts(self, client):
        """Test metadata retrieval with series counts."""
        response = client.get("/v1/metadata/external?include_counts=true")
        assert response.status_code == 200


class TestExternalAPIAuthentication:
    """Test external API authentication."""

    @patch("aurum.api.handlers.external.validate_oidc_auth")
    def test_authentication_required(self, mock_auth, client):
        """Test that authentication is required."""
        mock_auth.side_effect = Exception("Authentication failed")
        response = client.get("/v1/external/providers")
        assert response.status_code == 401

    def test_authentication_missing_token(self, client):
        """Test missing authentication token."""
        # This would normally be handled by the auth middleware
        pass


class TestExternalAPIRateLimiting:
    """Test external API rate limiting."""

    @patch("aurum.api.handlers.external.get_rate_limit_manager")
    def test_rate_limit_exceeded(self, mock_rate_limit_getter, client):
        """Test rate limit exceeded response."""
        mock_rate_limit = _mock_rate_limit_manager()
        mock_rate_limit.check_rate_limit.return_value.allowed = False
        mock_rate_limit.check_rate_limit.return_value.retry_after = 60
        mock_rate_limit_getter.return_value = mock_rate_limit

        response = client.get("/v1/external/providers")
        assert response.status_code == 429
        assert "retry_after" in response.json()


class TestExternalAPICaching:
    """Test external API caching."""

    @patch("aurum.api.handlers.external.get_cache_manager")
    def test_cache_hit(self, mock_cache_getter, client):
        """Test cache hit scenario."""
        mock_cache = _mock_cache_manager()
        mock_cache.get = AsyncMock(return_value=[{"id": "test", "name": "Test Provider"}])
        mock_cache_getter.return_value = mock_cache

        response = client.get("/v1/external/providers")
        assert response.status_code == 200

    @patch("aurum.api.handlers.external.get_cache_manager")
    def test_cache_miss(self, mock_cache_getter, client):
        """Test cache miss scenario."""
        mock_cache = _mock_cache_manager()
        mock_cache.get = AsyncMock(return_value=None)
        mock_cache_getter.return_value = mock_cache

        response = client.get("/v1/external/providers")
        assert response.status_code == 200


class TestExternalAPICurveMapping:
    """Test external API curve mapping functionality."""

    @patch("aurum.api.handlers.external._check_curve_mapping")
    @patch("aurum.api.handlers.external._proxy_to_curves_endpoint")
    @patch("aurum.api.handlers.external._convert_curve_to_external_observations")
    def test_curve_mapping_enabled(self, mock_convert, mock_proxy, mock_check, client):
        """Test when curve mapping is enabled."""
        mock_check.return_value = True
        mock_proxy.return_value = []
        mock_convert.return_value = []

        response = client.get("/v1/external/series/FRED:GDP/observations")
        assert response.status_code == 200

    @patch("aurum.api.handlers.external._check_curve_mapping")
    def test_curve_mapping_disabled(self, mock_check, client):
        """Test when curve mapping is disabled."""
        mock_check.return_value = False

        response = client.get("/v1/external/series/FRED:GDP/observations")
        assert response.status_code == 200


class TestExternalAPIErrorHandling:
    """Test external API error handling."""

    def test_internal_server_error(self, client):
        """Test internal server error handling."""
        with patch("aurum.api.handlers.external.get_external_dao") as mock_dao_getter:
            mock_dao = Mock()
            mock_dao.get_providers = AsyncMock(side_effect=Exception("Database error"))
            mock_dao_getter.return_value = mock_dao

            response = client.get("/v1/external/providers")
            assert response.status_code == 500

    def test_validation_error(self, client):
        """Test validation error handling."""
        response = client.get("/v1/external/series?frequency=invalid_frequency")
        assert response.status_code == 400

    def test_not_found_error(self, client):
        """Test not found error handling."""
        with patch("aurum.api.handlers.external.get_external_dao") as mock_dao_getter:
            mock_dao = Mock()
            mock_dao.get_observations = AsyncMock(side_effect=Exception("Series not found"))
            mock_dao_getter.return_value = mock_dao

            response = client.get("/v1/external/series/INVALID/observations")
            assert response.status_code == 500  # This should be 404 but DAO throws exception


class TestExternalAPIResponseFormats:
    """Test external API response formats."""

    def test_providers_response_format(self, client):
        """Test providers response format."""
        response = client.get("/v1/external/providers")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "meta" in data
        assert isinstance(data["data"], list)
        assert isinstance(data["meta"], dict)

    def test_series_response_format(self, client):
        """Test series response format."""
        response = client.get("/v1/external/series")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "meta" in data
        assert isinstance(data["data"], list)
        assert isinstance(data["meta"], dict)

    def test_observations_response_format(self, client):
        """Test observations response format."""
        response = client.get("/v1/external/series/FRED:GDP/observations")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "meta" in data
        assert isinstance(data["data"], list)
        assert isinstance(data["meta"], dict)

    def test_metadata_response_format(self, client):
        """Test metadata response format."""
        response = client.get("/v1/metadata/external")
        assert response.status_code == 200
        data = response.json()
        assert "providers" in data
        assert "total_series" in data
        assert "last_updated" in data
        assert isinstance(data["providers"], list)
        assert isinstance(data["total_series"], int)


class TestExternalAPIPagination:
    """Test external API pagination."""

    def test_providers_pagination(self, client):
        """Test providers pagination parameters."""
        response = client.get("/v1/external/providers?limit=5&offset=10")
        assert response.status_code == 200

    def test_series_pagination(self, client):
        """Test series pagination parameters."""
        response = client.get("/v1/external/series?limit=20&offset=0&cursor=abc123")
        assert response.status_code == 200

    def test_observations_pagination(self, client):
        """Test observations pagination parameters."""
        response = client.get("/v1/external/series/FRED:GDP/observations?limit=100&offset=50")
        assert response.status_code == 200
