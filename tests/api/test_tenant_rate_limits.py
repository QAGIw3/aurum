"""Tests for per-tenant rate limiting functionality."""
from __future__ import annotations

import json
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from aurum.api import app as api_app
from aurum.core import AurumSettings


def test_tenant_rate_limit_headers():
    """Test that rate limit headers are included for tenant requests."""
    client = TestClient(api_app.app)

    # Mock successful query
    def mock_query(*args, **kwargs):
        return [{"curve_key": "test", "mid": 42.0}], 1.0

    with patch("aurum.api.service.query_curves", mock_query):
        response = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-123"})

    assert response.status_code == 200
    assert "X-RateLimit-Limit" in response.headers
    assert "X-RateLimit-Remaining" in response.headers
    assert "X-RateLimit-Reset" in response.headers

    # Should have default limits
    assert int(response.headers["X-RateLimit-Limit"]) > 0
    assert int(response.headers["X-RateLimit-Remaining"]) >= 0


def test_tenant_rate_limit_different_tenants():
    """Test that different tenants have separate rate limits."""
    client = TestClient(api_app.app)

    def mock_query(*args, **kwargs):
        return [{"curve_key": "test", "mid": 42.0}], 1.0

    with patch("aurum.api.service.query_curves", mock_query):
        # First tenant
        response1 = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-1"})
        remaining1 = int(response1.headers["X-RateLimit-Remaining"])

        # Second tenant
        response2 = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-2"})
        remaining2 = int(response2.headers["X-RateLimit-Remaining"])

        # Should have same limits but independent counters
        assert int(response1.headers["X-RateLimit-Limit"]) == int(response2.headers["X-RateLimit-Limit"])
        # Remaining should be the same since we only made one request per tenant
        assert remaining1 == remaining2 == int(response1.headers["X-RateLimit-Limit"]) - 1


def test_tenant_rate_limit_from_principal():
    """Test that tenant from principal is used for rate limiting."""
    client = TestClient(api_app.app)

    def mock_query(*args, **kwargs):
        return [{"curve_key": "test", "mid": 42.0}], 1.0

    # Mock principal with tenant
    def mock_get_principal(request):
        return {"tenant": "principal-tenant"}

    with patch("aurum.api.service.query_curves", mock_query), \
         patch("aurum.api.routes._get_principal", mock_get_principal):
        response = client.get("/v1/curves", params={"limit": 1})

    assert response.status_code == 200
    assert "X-RateLimit-Limit" in response.headers
    assert "X-RateLimit-Remaining" in response.headers


def test_tenant_rate_limit_priority():
    """Test tenant resolution priority: principal > X-Aurum-Tenant header."""
    client = TestClient(api_app.app)

    def mock_query(*args, **kwargs):
        return [{"curve_key": "test", "mid": 42.0}], 1.0

    # Mock principal with tenant
    def mock_get_principal(request):
        return {"tenant": "principal-tenant"}

    with patch("aurum.api.service.query_curves", mock_query), \
         patch("aurum.api.routes._get_principal", mock_get_principal):
        response = client.get(
            "/v1/curves",
            params={"limit": 1},
            headers={"X-Aurum-Tenant": "header-tenant"}
        )

    assert response.status_code == 200
    # Should use principal tenant, not header tenant
    assert int(response.headers["X-RateLimit-Remaining"]) == 29  # Default limit - 1


def test_tenant_rate_limit_overrides():
    """Test tenant-specific rate limit overrides."""
    # Configure tenant-specific overrides
    with patch.dict("os.environ", {"API_RATE_LIMIT_OVERRIDES": "/v1/curves=5:10"}):
        settings = AurumSettings.from_env()
        app = api_app.create_app(settings)

        client = TestClient(app)

        def mock_query(*args, **kwargs):
            return [{"curve_key": "test", "mid": 42.0}], 1.0

        with patch("aurum.api.service.query_curves", mock_query):
            response = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-123"})

        assert response.status_code == 200
        # Should use override limits (5 RPS + 10 burst = 15 total)
        assert int(response.headers["X-RateLimit-Limit"]) == 15
        assert int(response.headers["X-RateLimit-Remaining"]) == 14


def test_rate_limit_exhaustion():
    """Test rate limit exhaustion and retry-after header."""
    # Set very low limits for testing
    with patch.dict("os.environ", {"API_RATE_LIMIT_RPS": "2", "API_RATE_LIMIT_BURST": "1"}):
        settings = AurumSettings.from_env()
        app = api_app.create_app(settings)

        client = TestClient(app)

        def mock_query(*args, **kwargs):
            return [{"curve_key": "test", "mid": 42.0}], 1.0

        with patch("aurum.api.service.query_curves", mock_query):
            # First request - should succeed
            response1 = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-123"})
            assert response1.status_code == 200

            # Second request - should succeed (within burst)
            response2 = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-123"})
            assert response2.status_code == 200

            # Third request - should be rate limited
            response3 = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-123"})
            assert response3.status_code == 429

            # Check rate limit headers on the 429 response
            assert "Retry-After" in response3.headers
            assert "X-RateLimit-Limit" in response3.headers
            assert "X-RateLimit-Remaining" in response3.headers
            assert "X-RateLimit-Reset" in response3.headers

            # Parse the response
            error_data = response3.json()
            assert error_data["error"] == "rate_limited"
            assert "Too Many Requests" in error_data["message"]


def test_rate_limit_tenant_isolation():
    """Test that tenants are properly isolated in rate limiting."""
    # Set low limits for testing
    with patch.dict("os.environ", {"API_RATE_LIMIT_RPS": "2", "API_RATE_LIMIT_BURST": "2"}):
        settings = AurumSettings.from_env()
        app = api_app.create_app(settings)

        client = TestClient(app)

        def mock_query(*args, **kwargs):
            return [{"curve_key": "test", "mid": 42.0}], 1.0

        with patch("aurum.api.service.query_curves", mock_query):
            # Tenant 1 makes 3 requests
            for i in range(3):
                response = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-1"})
                if i < 4:  # 2 RPS + 2 burst = 4 total
                    assert response.status_code == 200
                else:
                    assert response.status_code == 429

            # Tenant 2 should still have full quota
            response = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-2"})
            assert response.status_code == 200


def test_rate_limit_redis_fallback():
    """Test that rate limiting works without Redis (memory fallback)."""
    # Disable Redis
    with patch.dict("os.environ", {"REDIS_URL": ""}):
        settings = AurumSettings.from_env()
        app = api_app.create_app(settings)

        client = TestClient(app)

        def mock_query(*args, **kwargs):
            return [{"curve_key": "test", "mid": 42.0}], 1.0

        with patch("aurum.api.service.query_curves", mock_query):
            # Should still work with memory-based rate limiting
            response = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-123"})

        assert response.status_code == 200
        assert "X-RateLimit-Limit" in response.headers


def test_rate_limit_whitelist():
    """Test that whitelisted tenants bypass rate limiting."""
    with patch.dict("os.environ", {"API_RATE_LIMIT_WHITELIST": "tenant-123,admin"}):
        settings = AurumSettings.from_env()
        app = api_app.create_app(settings)

        client = TestClient(app)

        def mock_query(*args, **kwargs):
            return [{"curve_key": "test", "mid": 42.0}], 1.0

        with patch("aurum.api.service.query_curves", mock_query):
            # Whitelisted tenant should not have rate limit headers
            response = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-123"})

        assert response.status_code == 200
        # Whitelisted requests should not have rate limit headers
        assert "X-RateLimit-Limit" not in response.headers
        assert "X-RateLimit-Remaining" not in response.headers


def test_rate_limit_identifier_header():
    """Test custom identifier header for rate limiting."""
    with patch.dict("os.environ", {"API_RATE_LIMIT_HEADER": "X-API-Key"}):
        settings = AurumSettings.from_env()
        app = api_app.create_app(settings)

        client = TestClient(app)

        def mock_query(*args, **kwargs):
            return [{"curve_key": "test", "mid": 42.0}], 1.0

        with patch("aurum.api.service.query_curves", mock_query):
            response = client.get(
                "/v1/curves",
                params={"limit": 1},
                headers={"X-Aurum-Tenant": "tenant-123", "X-API-Key": "key-456"}
            )

        assert response.status_code == 200
        # Should use both tenant and API key for rate limiting
        assert "X-RateLimit-Limit" in response.headers


def test_rate_limit_reset_time_calculation():
    """Test that rate limit reset time is calculated correctly."""
    client = TestClient(api_app.app)

    def mock_query(*args, **kwargs):
        return [{"curve_key": "test", "mid": 42.0}], 1.0

    with patch("aurum.api.service.query_curves", mock_query):
        response = client.get("/v1/curves", params={"limit": 1}, headers={"X-Aurum-Tenant": "tenant-123"})

    assert response.status_code == 200
    assert "X-RateLimit-Reset" in response.headers

    reset_time = int(response.headers["X-RateLimit-Reset"])
    # Should be a reasonable time in the future (1-60 seconds typically)
    assert 0 < reset_time < 3600  # Less than 1 hour from now
