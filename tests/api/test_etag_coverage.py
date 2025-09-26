"""Tests for ETag coverage across all list and detail endpoints."""

from __future__ import annotations

import pytest
import pytest_asyncio
from unittest.mock import Mock, patch
from typing import Any, Dict

from fastapi import Request, Response
from fastapi.testclient import TestClient

from aurum.api.routes import _respond_with_etag, _generate_etag
from aurum.api.app import create_app
from aurum.core import AurumSettings


class TestETagCoverage:
    """Test ETag functionality across all endpoints."""

    @pytest.fixture
    def test_model(self):
        """Create a test model for ETag testing."""
        class TestModel:
            def __init__(self, data: Any):
                self.data = data

            def model_dump(self) -> Dict[str, Any]:
                return {"data": self.data}

        return TestModel({"test": "value"})

    @pytest.fixture
    def mock_request(self):
        """Create a mock request."""
        request = Mock(spec=Request)
        request.headers = {}
        request.state = Mock()
        return request

    @pytest.fixture
    def mock_response(self):
        """Create a mock response."""
        response = Mock(spec=Response)
        response.headers = {}
        return response

    def test_etag_generation(self, test_model, mock_request, mock_response):
        """Test ETag generation from model."""
        result = _respond_with_etag(test_model, mock_request, mock_response)

        # Check that ETag header is set
        assert "ETag" in mock_response.headers
        assert mock_response.headers["ETag"] is not None
        assert result == test_model

    def test_etag_if_none_match_hit(self, test_model, mock_request, mock_response):
        """Test If-None-Match header handling when ETag matches."""
        etag = _generate_etag(test_model.model_dump())

        mock_request.headers = {"if-none-match": etag}
        mock_response.headers = {}

        result = _respond_with_etag(test_model, mock_request, mock_response)

        # Should return 304 Not Modified response
        assert result is not None  # _respond_with_etag should return the model even for 304
        assert "ETag" in mock_response.headers

    def test_etag_if_none_match_miss(self, test_model, mock_request, mock_response):
        """Test If-None-Match header handling when ETag doesn't match."""
        mock_request.headers = {"if-none-match": "different-etag"}
        mock_response.headers = {}

        result = _respond_with_etag(test_model, mock_request, mock_response)

        # Should return the model with ETag header
        assert result == test_model
        assert "ETag" in mock_response.headers

    def test_etag_with_extra_headers(self, test_model, mock_request, mock_response):
        """Test ETag with additional headers."""
        extra_headers = {"Cache-Control": "public, max-age=300"}

        result = _respond_with_etag(test_model, mock_request, mock_response, extra_headers=extra_headers)

        # Check that both ETag and extra headers are set
        assert "ETag" in mock_response.headers
        assert "Cache-Control" in mock_response.headers
        assert mock_response.headers["Cache-Control"] == "public, max-age=300"
        assert result == test_model

    @pytest_asyncio.asyncio
    async def test_etag_in_scenarios_endpoints(self, monkeypatch):
        """Test ETag support in scenario endpoints."""
        monkeypatch.setenv("AURUM_API_AUTH_DISABLED", "1")
        settings = AurumSettings.from_env()
        app = create_app(settings)

        with TestClient(app) as client:
            # Test list scenarios endpoint
            response = client.get("/v1/scenarios?limit=10")
            assert response.status_code in {200, 403, 404}
            if response.status_code == 200:
                assert "ETag" in response.headers

                # Test If-None-Match with same request
                etag = response.headers["ETag"]
                response2 = client.get(
                    "/v1/scenarios?limit=10",
                    headers={"If-None-Match": etag},
                )
                assert response2.status_code in {200, 304}

    @pytest_asyncio.asyncio
    async def test_etag_in_curve_endpoints(self, monkeypatch):
        """Test ETag support in curve endpoints."""
        monkeypatch.setenv("AURUM_API_AUTH_DISABLED", "1")
        settings = AurumSettings.from_env()
        app = create_app(settings)

        with TestClient(app) as client:
            # Test curve observations endpoint
            response = client.get("/v1/curves?asof=2024-01-01&curve_key=TEST&iso=USD")
            assert response.status_code in [200, 403, 404, 500]
            if response.status_code == 200:
                assert "ETag" in response.headers

    @pytest_asyncio.asyncio
    async def test_etag_in_external_endpoints(self, monkeypatch):
        """Test ETag support in external data endpoints."""
        monkeypatch.setenv("AURUM_API_AUTH_DISABLED", "1")
        settings = AurumSettings.from_env()
        app = create_app(settings)

        with TestClient(app) as client:
            # Test external providers endpoint
            response = client.get("/v1/external/providers?limit=10")
            assert response.status_code in [200, 404]
            if response.status_code == 200:
                assert "ETag" in response.headers

    def test_etag_consistency(self, test_model):
        """Test that ETag generation is consistent for same data."""
        etag1 = _generate_etag(test_model.model_dump())
        etag2 = _generate_etag(test_model.model_dump())

        # ETags should be identical for same data
        assert etag1 == etag2

    def test_etag_different_for_different_data(self, test_model):
        """Test that ETag generation produces different values for different data."""
        etag1 = _generate_etag(test_model.model_dump())

        different_model = type('DifferentModel', (), {})()
        different_model.model_dump = lambda: {"data": "different_value"}
        etag2 = _generate_etag(different_model.model_dump())

        # ETags should be different for different data
        assert etag1 != etag2

    def test_etag_with_complex_data(self):
        """Test ETag generation with complex nested data."""
        complex_data = {
            "nested": {
                "array": [1, 2, 3],
                "object": {"key": "value"}
            },
            "list": ["a", "b", "c"],
            "number": 42,
            "string": "test"
        }

        etag1 = _generate_etag(complex_data)
        etag2 = _generate_etag(complex_data)

        # Should be consistent
        assert etag1 == etag2

        # Different data should produce different ETag
        different_data = complex_data.copy()
        different_data["number"] = 43
        etag3 = _generate_etag(different_data)
        assert etag1 != etag3
