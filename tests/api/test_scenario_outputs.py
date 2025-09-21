"""Tests for scenario outputs API endpoint with time-based filtering."""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from aurum.api.scenarios import router
from aurum.api.container import get_service
from aurum.api.async_service import AsyncScenarioService
from aurum.api.models import ScenarioOutputListResponse, ScenarioOutputFilter


class TestScenarioOutputsEndpoint:
    """Test scenario outputs listing with time-based filtering."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi import FastAPI
        app = FastAPI()
        app.include_router(router)
        return TestClient(app)

    @pytest.fixture
    def mock_service(self):
        """Mock the AsyncScenarioService."""
        mock_service = MagicMock(spec=AsyncScenarioService)
        with patch('aurum.api.scenarios.get_service') as mock_get_service:
            mock_get_service.return_value = mock_service
            yield mock_service

    @pytest.fixture
    def sample_outputs(self):
        """Sample scenario output data."""
        return [
            {
                "timestamp": "2024-01-01T10:00:00Z",
                "metric_name": "power_output",
                "value": 100.5,
                "unit": "MW",
                "tags": {"region": "west", "type": "renewable"}
            },
            {
                "timestamp": "2024-01-01T11:00:00Z",
                "metric_name": "power_output",
                "value": 95.2,
                "unit": "MW",
                "tags": {"region": "west", "type": "renewable"}
            },
            {
                "timestamp": "2024-01-01T12:00:00Z",
                "metric_name": "cost",
                "value": 50.0,
                "unit": "$/MWh",
                "tags": {"region": "west", "type": "operating"}
            }
        ]

    def test_get_scenario_outputs_basic(self, client, mock_service, sample_outputs):
        """Test basic scenario outputs retrieval."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.return_value = (sample_outputs, 3, {"total": 3})

        response = client.get(f"/v1/scenarios/{scenario_id}/outputs")

        assert response.status_code == 200
        result = response.json()

        assert "meta" in result
        assert "data" in result
        assert "filter" in result
        assert len(result["data"]) == 3
        assert result["meta"]["count"] == 3
        assert result["meta"]["total"] == 3

    def test_get_scenario_outputs_with_time_filters(self, client, mock_service, sample_outputs):
        """Test scenario outputs with time-based filtering."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.return_value = (sample_outputs[:1], 1, {"total": 1})

        start_time = "2024-01-01T09:00:00Z"
        end_time = "2024-01-01T10:30:00Z"

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"start_time": start_time, "end_time": end_time}
        )

        assert response.status_code == 200
        result = response.json()

        # Verify filters are returned
        assert result["filter"]["start_time"] == start_time
        assert result["filter"]["end_time"] == end_time
        assert len(result["data"]) == 1
        assert result["data"][0]["timestamp"] == "2024-01-01T10:00:00Z"

    def test_get_scenario_outputs_with_metric_filter(self, client, mock_service, sample_outputs):
        """Test scenario outputs with metric name filtering."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.return_value = (sample_outputs[2:], 1, {"total": 1})

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"metric_name": "cost"}
        )

        assert response.status_code == 200
        result = response.json()

        assert result["filter"]["metric_name"] == "cost"
        assert len(result["data"]) == 1
        assert result["data"][0]["metric_name"] == "cost"

    def test_get_scenario_outputs_with_value_filters(self, client, mock_service, sample_outputs):
        """Test scenario outputs with min/max value filtering."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.return_value = (sample_outputs[2:], 1, {"total": 1})

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"min_value": 90, "max_value": 110}
        )

        assert response.status_code == 200
        result = response.json()

        assert result["filter"]["min_value"] == 90
        assert result["filter"]["max_value"] == 110
        assert len(result["data"]) == 1

    def test_get_scenario_outputs_with_tags_filter(self, client, mock_service, sample_outputs):
        """Test scenario outputs with tags filtering."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.return_value = (sample_outputs[:2], 2, {"total": 2})

        tags_filter = '{"region": "west", "type": "renewable"}'
        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"tags": tags_filter}
        )

        assert response.status_code == 200
        result = response.json()

        assert result["filter"]["tags"] == {"region": "west", "type": "renewable"}
        assert len(result["data"]) == 2
        for output in result["data"]:
            assert output["tags"]["region"] == "west"
            assert output["tags"]["type"] == "renewable"

    def test_get_scenario_outputs_with_pagination(self, client, mock_service, sample_outputs):
        """Test scenario outputs with pagination."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.return_value = (sample_outputs[:1], 3, {"total": 3})

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"limit": 1, "offset": 1}
        )

        assert response.status_code == 200
        result = response.json()

        assert result["meta"]["count"] == 1
        assert result["meta"]["total"] == 3
        assert result["meta"]["offset"] == 1
        assert result["meta"]["limit"] == 1
        assert result["meta"]["has_more"] is True

    def test_get_scenario_outputs_invalid_scenario_id(self, client, mock_service):
        """Test scenario outputs with invalid scenario ID."""
        scenario_id = "invalid-uuid"

        response = client.get(f"/v1/scenarios/{scenario_id}/outputs")

        assert response.status_code == 400
        assert "Invalid scenario ID format" in response.json()["detail"]

    def test_get_scenario_outputs_invalid_time_format(self, client, mock_service):
        """Test scenario outputs with invalid time format."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"start_time": "invalid-time"}
        )

        assert response.status_code == 400
        assert "Invalid ISO 8601 datetime format" in response.json()["detail"]

    def test_get_scenario_outputs_invalid_tags_format(self, client, mock_service):
        """Test scenario outputs with invalid tags format."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"tags": "invalid-json"}
        )

        assert response.status_code == 400
        assert "Invalid tags format" in response.json()["detail"]

    def test_get_scenario_outputs_invalid_value_range(self, client, mock_service):
        """Test scenario outputs with invalid min/max value range."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"min_value": 100, "max_value": 50}
        )

        assert response.status_code == 400
        assert "min_value cannot be greater than max_value" in response.json()["detail"]

    def test_get_scenario_outputs_service_error(self, client, mock_service):
        """Test scenario outputs when service raises an error."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.side_effect = Exception("Database connection failed")

        response = client.get(f"/v1/scenarios/{scenario_id}/outputs")

        assert response.status_code == 500
        assert "Failed to get scenario outputs" in response.json()["detail"]

    def test_get_scenario_outputs_csv_format(self, client, mock_service, sample_outputs):
        """Test scenario outputs in CSV format."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.return_value = (sample_outputs, 3, {"total": 3})

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"format": "csv"}
        )

        # CSV format would return a streaming response
        # This is a placeholder test - actual implementation would need CSV streaming
        assert response.status_code == 200

    def test_get_scenario_outputs_combined_filters(self, client, mock_service, sample_outputs):
        """Test scenario outputs with multiple filters combined."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.return_value = (sample_outputs[:1], 1, {"total": 1})

        start_time = "2024-01-01T09:00:00Z"
        end_time = "2024-01-01T11:00:00Z"
        metric_name = "power_output"
        min_value = 90
        max_value = 110

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={
                "start_time": start_time,
                "end_time": end_time,
                "metric_name": metric_name,
                "min_value": min_value,
                "max_value": max_value
            }
        )

        assert response.status_code == 200
        result = response.json()

        # Verify all filters are applied
        filter_obj = result["filter"]
        assert filter_obj["start_time"] == start_time
        assert filter_obj["end_time"] == end_time
        assert filter_obj["metric_name"] == metric_name
        assert filter_obj["min_value"] == min_value
        assert filter_obj["max_value"] == max_value

        assert len(result["data"]) == 1
        assert result["data"][0]["metric_name"] == "power_output"
        assert 90 <= result["data"][0]["value"] <= 110

    def test_get_scenario_outputs_empty_results(self, client, mock_service):
        """Test scenario outputs when no results match filters."""
        scenario_id = "123e4567-e89b-12d3-a456-426614174000"
        mock_service.get_scenario_outputs.return_value = ([], 0, {"total": 0})

        response = client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            params={"metric_name": "nonexistent_metric"}
        )

        assert response.status_code == 200
        result = response.json()

        assert result["meta"]["count"] == 0
        assert result["meta"]["total"] == 0
        assert len(result["data"]) == 0
        assert result["meta"]["has_more"] is False


class TestScenarioOutputFilterModel:
    """Test ScenarioOutputFilter model."""

    def test_scenario_output_filter_creation(self):
        """Test creating ScenarioOutputFilter with all fields."""
        filter_obj = ScenarioOutputFilter(
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-02T00:00:00Z",
            metric_name="power_output",
            min_value=0,
            max_value=100,
            tags={"region": "west"}
        )

        assert filter_obj.start_time == "2024-01-01T00:00:00Z"
        assert filter_obj.end_time == "2024-01-02T00:00:00Z"
        assert filter_obj.metric_name == "power_output"
        assert filter_obj.min_value == 0
        assert filter_obj.max_value == 100
        assert filter_obj.tags == {"region": "west"}

    def test_scenario_output_filter_partial(self):
        """Test creating ScenarioOutputFilter with partial fields."""
        filter_obj = ScenarioOutputFilter(
            metric_name="power_output"
        )

        assert filter_obj.metric_name == "power_output"
        assert filter_obj.start_time is None
        assert filter_obj.end_time is None
        assert filter_obj.min_value is None
        assert filter_obj.max_value is None
        assert filter_obj.tags is None

    def test_scenario_output_filter_validation(self):
        """Test that ScenarioOutputFilter validates constraints."""
        # Test min_value >= 0 constraint
        with pytest.raises(Exception):
            ScenarioOutputFilter(min_value=-1)

        # Test max_value >= 0 constraint
        with pytest.raises(Exception):
            ScenarioOutputFilter(max_value=-1)


class TestScenarioOutputListResponseModel:
    """Test ScenarioOutputListResponse model."""

    def test_scenario_output_list_response_creation(self):
        """Test creating ScenarioOutputListResponse."""
        sample_outputs = [
            {
                "timestamp": "2024-01-01T10:00:00Z",
                "metric_name": "power_output",
                "value": 100.5,
                "unit": "MW",
                "tags": {"region": "west"}
            }
        ]

        filter_obj = ScenarioOutputFilter(metric_name="power_output")

        response = ScenarioOutputListResponse(
            meta={
                "request_id": "test-request-id",
                "query_time_ms": 123.45,
                "total": 1,
                "count": 1,
                "offset": 0,
                "limit": 10,
                "has_more": False
            },
            data=sample_outputs,
            filter=filter_obj
        )

        assert response.meta["total"] == 1
        assert response.meta["count"] == 1
        assert len(response.data) == 1
        assert response.filter.metric_name == "power_output"

    def test_scenario_output_list_response_without_filter(self):
        """Test creating ScenarioOutputListResponse without filter."""
        sample_outputs = []

        response = ScenarioOutputListResponse(
            meta={
                "request_id": "test-request-id",
                "query_time_ms": 50.0,
                "total": 0,
                "count": 0,
                "offset": 0,
                "limit": 10,
                "has_more": False
            },
            data=sample_outputs,
            filter=None
        )

        assert response.meta["total"] == 0
        assert len(response.data) == 0
        assert response.filter is None
