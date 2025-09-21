"""Tests for gzip compression and content negotiation."""
from __future__ import annotations

import gzip
import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from aurum.api import app as api_app
from aurum.core import AurumSettings


def test_gzip_threshold_environment_variable():
    """Test that gzip threshold can be configured via environment variable."""
    # Test default value
    settings = AurumSettings.from_env()
    assert settings.api.gzip_min_bytes == 500

    # Test custom value via environment variable
    with patch.dict(os.environ, {"API_GZIP_MIN_BYTES": "1024"}):
        settings = AurumSettings.from_env()
        assert settings.api.gzip_min_bytes == 1024

    # Test zero value (disable gzip)
    with patch.dict(os.environ, {"API_GZIP_MIN_BYTES": "0"}):
        settings = AurumSettings.from_env()
        assert settings.api.gzip_min_bytes == 0


def test_gzip_middleware_configuration():
    """Test that GZipMiddleware is configured with correct threshold."""
    # Test with default threshold
    settings = AurumSettings.from_env()
    app = api_app.create_app(settings)

    # Check that GZipMiddleware is added with correct minimum_size
    gzip_middleware = None
    for middleware in app.user_middleware:
        if middleware.cls.__name__ == "GZipMiddleware":
            gzip_middleware = middleware
            break

    assert gzip_middleware is not None
    assert gzip_middleware.options["minimum_size"] == settings.api.gzip_min_bytes

    # Test with custom threshold
    with patch.dict(os.environ, {"API_GZIP_MIN_BYTES": "2048"}):
        settings = AurumSettings.from_env()
        app = api_app.create_app(settings)

        gzip_middleware = None
        for middleware in app.user_middleware:
            if middleware.cls.__name__ == "GZipMiddleware":
                gzip_middleware = middleware
                break

        assert gzip_middleware is not None
        assert gzip_middleware.options["minimum_size"] == 2048


def test_gzip_compression_small_response():
    """Test that small responses are not gzipped."""
    client = TestClient(api_app.app)

    # Mock a small response
    def mock_query(*args, **kwargs):
        # Small response that should not be gzipped
        return [{"curve_key": "test", "mid": 42.0}], 1.0

    with patch("aurum.api.service.query_curves", mock_query):
        response = client.get("/v1/curves", params={"limit": 1})

    assert response.status_code == 200
    assert response.headers.get("Content-Encoding") is None
    assert "Vary" in response.headers
    assert "Accept-Encoding" in response.headers["Vary"]


def test_gzip_compression_large_response():
    """Test that large responses are gzipped."""
    client = TestClient(api_app.app)

    # Mock a large response that should be gzipped
    def mock_query(*args, **kwargs):
        # Large response that should be gzipped
        return [{"curve_key": f"test_{i}", "mid": float(i)} for i in range(1000)], 1.0

    with patch("aurum.api.service.query_curves", mock_query):
        response = client.get("/v1/curves", params={"limit": 1000})

    assert response.status_code == 200
    assert response.headers.get("Content-Encoding") == "gzip"
    assert "Vary" in response.headers
    assert "Accept-Encoding" in response.headers["Vary"]

    # Verify the content is actually gzipped
    compressed_content = response.content
    decompressed = gzip.decompress(compressed_content)
    assert len(decompressed) > len(compressed_content)  # Should be smaller when compressed


def test_gzip_disabled():
    """Test behavior when gzip is disabled."""
    # Disable gzip
    with patch.dict(os.environ, {"API_GZIP_MIN_BYTES": "0"}):
        from aurum.api import app as api_app_module
        settings = AurumSettings.from_env()
        app = api_app_module.create_app(settings)

        client = TestClient(app)

        def mock_query(*args, **kwargs):
            return [{"curve_key": f"test_{i}", "mid": float(i)} for i in range(1000)], 1.0

        with patch("aurum.api.service.query_curves", mock_query):
            response = client.get("/v1/curves", params={"limit": 1000})

        assert response.status_code == 200
        assert response.headers.get("Content-Encoding") is None
        assert "Vary" in response.headers
        assert "Accept-Encoding" in response.headers["Vary"]


def test_vary_header_always_present():
    """Test that Vary header is always present for content negotiation."""
    client = TestClient(api_app.app)

    # Test with various endpoints
    endpoints = [
        "/v1/curves",
        "/v1/curves/diff",
        "/v1/metadata/dimensions",
        "/health",
        "/ready"
    ]

    for endpoint in endpoints:
        response = client.get(endpoint)
        assert "Vary" in response.headers, f"Vary header missing for {endpoint}"
        assert "Accept-Encoding" in response.headers["Vary"], f"Accept-Encoding missing from Vary header for {endpoint}"


def test_content_type_vary_header():
    """Test that Vary header includes Accept for content-type negotiation."""
    client = TestClient(api_app.app)

    # Request CSV format
    response = client.get("/v1/curves", params={"format": "csv", "limit": 1})

    assert response.status_code == 200
    assert "Vary" in response.headers
    assert "Accept" in response.headers["Vary"]
    assert "Accept-Encoding" in response.headers["Vary"]


def test_gzip_accept_encoding_wildcard():
    """Test gzip compression with Accept-Encoding: *."""
    client = TestClient(api_app.app)

    def mock_query(*args, **kwargs):
        return [{"curve_key": f"test_{i}", "mid": float(i)} for i in range(1000)], 1.0

    with patch("aurum.api.service.query_curves", mock_query):
        response = client.get(
            "/v1/curves",
            params={"limit": 1000},
            headers={"Accept-Encoding": "*"}
        )

    assert response.status_code == 200
    assert response.headers.get("Content-Encoding") == "gzip"


def test_gzip_accept_encoding_gzip():
    """Test gzip compression with Accept-Encoding: gzip."""
    client = TestClient(api_app.app)

    def mock_query(*args, **kwargs):
        return [{"curve_key": f"test_{i}", "mid": float(i)} for i in range(1000)], 1.0

    with patch("aurum.api.service.query_curves", mock_query):
        response = client.get(
            "/v1/curves",
            params={"limit": 1000},
            headers={"Accept-Encoding": "gzip"}
        )

    assert response.status_code == 200
    assert response.headers.get("Content-Encoding") == "gzip"


def test_no_gzip_when_not_accepted():
    """Test no gzip compression when not accepted."""
    client = TestClient(api_app.app)

    def mock_query(*args, **kwargs):
        return [{"curve_key": f"test_{i}", "mid": float(i)} for i in range(1000)], 1.0

    with patch("aurum.api.service.query_curves", mock_query):
        response = client.get(
            "/v1/curves",
            params={"limit": 1000},
            headers={"Accept-Encoding": "identity"}
        )

    assert response.status_code == 200
    assert response.headers.get("Content-Encoding") is None


def test_gzip_threshold_edge_cases():
    """Test gzip threshold edge cases."""
    # Test threshold of 1 byte
    with patch.dict(os.environ, {"API_GZIP_MIN_BYTES": "1"}):
        settings = AurumSettings.from_env()
        app = api_app.create_app(settings)

        client = TestClient(app)

        # Even smallest response should be gzipped
        def mock_query(*args, **kwargs):
            return [{"curve_key": "test", "mid": 42.0}], 1.0

        with patch("aurum.api.service.query_curves", mock_query):
            response = client.get("/v1/curves", params={"limit": 1})

        assert response.status_code == 200
        assert response.headers.get("Content-Encoding") == "gzip"


def test_gzip_error_handling():
    """Test gzip error handling for malformed requests."""
    client = TestClient(api_app.app)

    # Test with invalid Accept-Encoding header
    response = client.get(
        "/v1/curves",
        params={"limit": 1},
        headers={"Accept-Encoding": "invalid-encoding"}
    )

    # Should still work, just not compress
    assert response.status_code == 200
    assert response.headers.get("Content-Encoding") is None


def test_gzip_content_length_header():
    """Test that Content-Length header is correct for gzipped responses."""
    client = TestClient(api_app.app)

    def mock_query(*args, **kwargs):
        # Large response that will be gzipped
        data = [{"curve_key": f"test_{i}", "mid": float(i)} for i in range(500)]
        return data, 1.0

    with patch("aurum.api.service.query_curves", mock_query):
        response = client.get("/v1/curves", params={"limit": 500})

    assert response.status_code == 200
    if response.headers.get("Content-Encoding") == "gzip":
        # Content-Length should be the compressed size
        assert "Content-Length" in response.headers
        content_length = int(response.headers["Content-Length"])
        assert content_length > 0
        assert content_length < len(str(response.json()).encode())  # Should be smaller than uncompressed
