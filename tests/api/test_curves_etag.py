"""Tests for curves.py ETag functionality and pagination."""

import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient

from aurum.api.v1.curves import router
from aurum.api.models import CurveResponse, CurvePoint
from aurum.api.http import respond_with_etag


class TestCurvesETag:
    """Test ETag functionality in curves endpoints."""

    def setup_method(self):
        """Set up test client."""
        self.client = TestClient(router)

    @patch('aurum.api.v1.curves.get_service')
    async def test_curves_etag_generation(self, mock_get_service):
        """Test that ETags are generated correctly for curve responses."""
        # Mock the service
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        # Mock curve data
        curve_points = [
            CurvePoint(timestamp="2024-01-01T00:00:00Z", value=100.0, unit="USD/MWh")
        ]

        # Mock the service response
        mock_service.fetch_curves.return_value = (
            curve_points,
            type('Meta', (), {'next_cursor': None, 'prev_cursor': None})()
        )

        # Make request
        response = self.client.get("/v1/curves?iso=PJM&market=DAY_AHEAD")

        # Check response
        assert response.status_code == 200
        assert "ETag" in response.headers

        # Verify ETag format (should be a hash)
        etag = response.headers["ETag"]
        assert len(etag) == 64  # SHA256 hex digest length
        assert all(c in "0123456789abcdef" for c in etag)

    @patch('aurum.api.v1.curves.get_service')
    async def test_curves_etag_304_response(self, mock_get_service):
        """Test that 304 responses are returned for matching ETags."""
        # Mock the service
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        # Mock curve data
        curve_points = [
            CurvePoint(timestamp="2024-01-01T00:00:00Z", value=100.0, unit="USD/MWh")
        ]

        # Mock the service response
        mock_service.fetch_curves.return_value = (
            curve_points,
            type('Meta', (), {'next_cursor': None, 'prev_cursor': None})()
        )

        # First request to get ETag
        response1 = self.client.get("/v1/curves?iso=PJM&market=DAY_AHEAD")
        etag = response1.headers["ETag"]

        # Second request with If-None-Match header
        response2 = self.client.get(
            "/v1/curves?iso=PJM&market=DAY_AHEAD",
            headers={"If-None-Match": etag}
        )

        # Should return 304
        assert response2.status_code == 304

    @patch('aurum.api.v1.curves.get_service')
    async def test_curves_offset_deprecation_headers(self, mock_get_service):
        """Test that deprecation headers are added when using offset parameter."""
        # Mock the service
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        # Mock curve data
        curve_points = [
            CurvePoint(timestamp="2024-01-01T00:00:00Z", value=100.0, unit="USD/MWh")
        ]

        # Mock the service response
        mock_service.fetch_curves.return_value = (
            curve_points,
            type('Meta', (), {'next_cursor': None, 'prev_cursor': None})()
        )

        # Make request with offset parameter
        response = self.client.get("/v1/curves?iso=PJM&market=DAY_AHEAD&offset=10")

        # Check deprecation headers
        assert "Deprecation" in response.headers
        assert "X-Deprecation-Info" in response.headers
        assert "X-Sunset" in response.headers

        assert response.headers["Deprecation"] == "true"
        assert "offset-based pagination" in response.headers["X-Deprecation-Info"]
        assert response.headers["X-Sunset"] == "Removed in version v2.0.0"
