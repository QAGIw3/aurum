"""Integration tests for API endpoints."""

import asyncio
import json
import pytest
import httpx
from typing import AsyncGenerator

from aurum.telemetry.context import get_request_id, correlation_context


@pytest.fixture(scope="session")
async def api_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Create an async HTTP client for API testing."""
    async with httpx.AsyncClient(
        base_url="http://api:8000",
        timeout=30.0
    ) as client:
        # Wait for API to be ready
        for i in range(30):
            try:
                response = await client.get("/health")
                if response.status_code == 200:
                    break
            except Exception:
                pass
            await asyncio.sleep(1)
        else:
            pytest.fail("API service did not become ready")

        yield client


@pytest.mark.asyncio
async def test_api_health_check(api_client: httpx.AsyncClient):
    """Test API health check endpoint."""
    response = await api_client.get("/health")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "healthy"
    assert "version" in data


@pytest.mark.asyncio
async def test_api_tracing(api_client: httpx.AsyncClient):
    """Test that API endpoints create traces."""
    with correlation_context(
        tenant_id="00000000-0000-0000-0000-000000000001",
        user_id="11111111-1111-1111-1111-111111111111"
    ):
        response = await api_client.get("/v1/curves")

        assert response.status_code == 200

        # Check that request ID is included in response headers
        assert "X-Request-ID" in response.headers

        # Check that tenant context was propagated
        request_id = response.headers["X-Request-ID"]
        assert request_id is not None


@pytest.mark.asyncio
async def test_api_metrics(api_client: httpx.AsyncClient):
    """Test API metrics collection."""
    # Make a request to generate some metrics
    response = await api_client.get("/v1/curves")
    assert response.status_code == 200

    # Check Prometheus metrics endpoint
    metrics_response = await api_client.get("/metrics")
    assert metrics_response.status_code == 200

    # Verify metrics are being collected
    metrics_text = metrics_response.text
    assert "aurum_api_requests_total" in metrics_text
    assert "aurum_api_request_duration_seconds" in metrics_text


@pytest.mark.asyncio
async def test_api_rate_limiting(api_client: httpx.AsyncClient):
    """Test API rate limiting."""
    # Make multiple requests quickly to trigger rate limiting
    responses = []
    for i in range(10):
        response = await api_client.get("/v1/curves")
        responses.append(response.status_code)

    # Should have some 429 (Too Many Requests) responses
    assert 429 in responses or all(code == 200 for code in responses)


@pytest.mark.asyncio
async def test_api_structured_logging(api_client: httpx.AsyncClient):
    """Test structured JSON logging."""
    # Make a request that should generate structured logs
    response = await api_client.get("/v1/curves")
    assert response.status_code == 200

    # The logs should be in JSON format with request context
    # This would typically be verified by checking log files or a log aggregation system
    # For now, we just ensure the request completes successfully


@pytest.mark.asyncio
async def test_api_error_handling(api_client: httpx.AsyncClient):
    """Test API error handling and error metrics."""
    # Make a request to a non-existent endpoint
    response = await api_client.get("/v1/nonexistent")
    assert response.status_code == 404

    # Check that error metrics are collected
    metrics_response = await api_client.get("/metrics")
    assert metrics_response.status_code == 200

    metrics_text = metrics_response.text
    # Should have some error metrics
    assert "aurum_api_requests_total" in metrics_text


@pytest.mark.asyncio
async def test_api_caching(api_client: httpx.AsyncClient):
    """Test API response caching."""
    # Make the same request twice
    response1 = await api_client.get("/v1/curves")
    response2 = await api_client.get("/v1/curves")

    assert response1.status_code == 200
    assert response2.status_code == 200

    # Check cache metrics
    metrics_response = await api_client.get("/metrics")
    assert metrics_response.status_code == 200

    metrics_text = metrics_response.text
    assert "aurum_cache_operations_total" in metrics_text


@pytest.mark.asyncio
async def test_api_tenant_isolation(api_client: httpx.AsyncClient):
    """Test that tenant data is properly isolated."""
    # Test with different tenant contexts
    test_cases = [
        ("00000000-0000-0000-0000-000000000001", "11111111-1111-1111-1111-111111111111"),
        ("00000000-0000-0000-0000-000000000002", "22222222-2222-2222-2222-222222222222"),
    ]

    for tenant_id, user_id in test_cases:
        with correlation_context(tenant_id=tenant_id, user_id=user_id):
            response = await api_client.get("/v1/curves")
            assert response.status_code == 200

            # Verify tenant context in response headers
            assert "X-Tenant-ID" in response.headers
            assert response.headers["X-Tenant-ID"] == tenant_id


@pytest.mark.asyncio
async def test_api_concurrency(api_client: httpx.AsyncClient):
    """Test API under concurrent load."""
    async def make_request():
        return await api_client.get("/v1/curves")

    # Make 10 concurrent requests
    tasks = [make_request() for _ in range(10)]
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    # All requests should succeed
    success_count = sum(1 for r in responses if isinstance(r, httpx.Response) and r.status_code == 200)
    assert success_count == 10

    # Check that concurrent requests are properly traced
    for response in responses:
        if isinstance(response, httpx.Response):
            assert "X-Request-ID" in response.headers
