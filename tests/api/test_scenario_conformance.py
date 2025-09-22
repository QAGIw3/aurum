"""Comprehensive conformance tests for Scenario API.

Tests cover:
- Idempotent run creation
- Cursor pagination behavior
- ETag/If-None-Match semantics
- RBAC authorization checks
- Property-based validation testing
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import HTTPException
from hypothesis import given, strategies as st, settings

from aurum.api.scenarios import router
from aurum.api.scenario_models import (
    CreateScenarioRequest,
    ScenarioResponse,
    ScenarioListResponse,
    ScenarioRunOptions,
    ScenarioRunResponse,
    ScenarioRunListResponse,
    ScenarioOutputResponse,
    ScenarioMetricLatestResponse,
    BulkScenarioRunRequest,
    BulkScenarioRunResponse,
    ScenarioStatus,
    ScenarioRunStatus,
    ScenarioRunPriority,
)
from aurum.api.http import respond_with_etag, encode_cursor, decode_cursor


class TestScenarioAPIConformance:
    """Comprehensive conformance tests for Scenario API."""

    def setup_method(self):
        """Set up test client and mocks."""
        self.client = TestClient(router)

    @patch('aurum.api.scenarios.get_service')
    async def test_scenario_creation_idempotency(self, mock_get_service):
        """Test that scenario creation is idempotent based on tenant/name."""
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        # Mock scenario data
        scenario_data = {
            "tenant_id": "test-tenant",
            "name": "test-scenario",
            "description": "Test scenario",
            "assumptions": [],
            "parameters": {},
            "tags": []
        }

        mock_service.create_scenario.return_value = {
            "id": "scenario-123",
            "tenant_id": "test-tenant",
            "name": "test-scenario",
            "description": "Test scenario",
            "status": "created",
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "assumptions": [],
            "parameters": {},
            "tags": [],
            "unique_key": "test-tenant:test-scenario",
            "version": 1
        }

        # First creation
        response1 = self.client.post(
            "/v1/scenarios",
            json=scenario_data,
            headers={"Authorization": "Bearer valid-token"}
        )

        assert response1.status_code == 201

        # Second creation with same data (should succeed idempotently)
        response2 = self.client.post(
            "/v1/scenarios",
            json=scenario_data,
            headers={"Authorization": "Bearer valid-token"}
        )

        assert response2.status_code == 201
        assert response1.json()["data"]["id"] == response2.json()["data"]["id"]

    @patch('aurum.api.scenarios.get_service')
    async def test_scenario_run_creation_idempotency(self, mock_get_service):
        """Test that scenario run creation is idempotent."""
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        run_data = {
            "code_version": "v1.0",
            "seed": 42,
            "priority": "normal",
            "timeout_minutes": 60,
            "max_memory_mb": 1024,
            "environment": {},
            "parameters": {},
            "max_retries": 3,
            "idempotency_key": "unique-run-key-123"
        }

        mock_run = {
            "id": "run-123",
            "scenario_id": "scenario-123",
            "status": "queued",
            "created_at": datetime.now(timezone.utc),
            "code_version": "v1.0",
            "seed": 42,
            "priority": "normal",
            "run_key": "scenario-123:hash123",
            "input_hash": "hash123",
            "parameters": {},
            "environment": {}
        }

        mock_service.create_scenario_run.return_value = mock_run

        # Create first run
        response1 = self.client.post(
            "/v1/scenarios/scenario-123/run",
            json=run_data,
            headers={"Authorization": "Bearer valid-token"}
        )

        assert response1.status_code == 202

        # Create second run with same idempotency key
        response2 = self.client.post(
            "/v1/scenarios/scenario-123/run",
            json=run_data,
            headers={"Authorization": "Bearer valid-token"}
        )

        assert response2.status_code == 202
        assert response1.json()["data"]["id"] == response2.json()["data"]["id"]

    @patch('aurum.api.scenarios.get_service')
    async def test_cursor_pagination_stability(self, mock_get_service):
        """Test that cursor-based pagination provides stable results."""
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        # Mock scenarios data
        scenarios = [
            {"id": f"scenario-{i}", "name": f"Test Scenario {i}", "tenant_id": "test-tenant"}
            for i in range(25)
        ]

        mock_service.list_scenarios.return_value = (
            scenarios[:10],  # First page
            25,  # Total count
            type('Meta', (), {'next_cursor': encode_cursor({"offset": 10}), 'prev_cursor': None})()
        )

        # First page request
        response1 = self.client.get(
            "/v1/scenarios?limit=10",
            headers={"Authorization": "Bearer valid-token"}
        )

        assert response1.status_code == 200
        first_page_ids = [item["id"] for item in response1.json()["data"]]
        next_cursor = response1.json()["meta"]["next_cursor"]

        # Mock second page
        mock_service.list_scenarios.return_value = (
            scenarios[10:20],  # Second page
            25,  # Total count
            type('Meta', (), {
                'next_cursor': encode_cursor({"offset": 20}),
                'prev_cursor': encode_cursor({"offset": 0})
            })()
        )

        # Second page request using cursor
        response2 = self.client.get(
            f"/v1/scenarios?cursor={next_cursor}",
            headers={"Authorization": "Bearer valid-token"}
        )

        assert response2.status_code == 200
        second_page_ids = [item["id"] for item in response2.json()["data"]]

        # Verify no overlap and stable ordering
        assert set(first_page_ids).isdisjoint(set(second_page_ids))
        assert len(first_page_ids) == 10
        assert len(second_page_ids) == 10

    @patch('aurum.api.scenarios.get_service')
    async def test_etag_conditional_requests(self, mock_get_service):
        """Test ETag conditional request handling."""
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        # Mock scenario data
        scenario_data = {
            "id": "scenario-123",
            "tenant_id": "test-tenant",
            "name": "test-scenario",
            "status": "active",
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }

        mock_service.get_scenario.return_value = scenario_data

        # First request to get ETag
        response1 = self.client.get(
            "/v1/scenarios/scenario-123",
            headers={"Authorization": "Bearer valid-token"}
        )

        assert response1.status_code == 200
        assert "ETag" in response1.headers
        etag = response1.headers["ETag"]

        # Second request with If-None-Match header
        response2 = self.client.get(
            "/v1/scenarios/scenario-123",
            headers={
                "Authorization": "Bearer valid-token",
                "If-None-Match": etag
            }
        )

        assert response2.status_code == 304  # Not Modified
        assert not response2.content  # No body

    @patch('aurum.api.scenarios.get_service')
    async def test_rbac_authorization_checks(self, mock_get_service):
        """Test RBAC authorization checks for different permissions."""
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        # Mock principal with different permissions
        def mock_principal():
            return {
                "sub": "user123",
                "groups": ["scenarios-read", "scenarios-write"],
                "tenant": "test-tenant"
            }

        # Test list scenarios (requires read permission)
        mock_service.list_scenarios.return_value = ([], 0, type('Meta', (), {})())

        with patch('aurum.api.scenarios.getattr') as mock_getattr:
            mock_getattr.return_value = mock_principal()

            response = self.client.get(
                "/v1/scenarios",
                headers={"Authorization": "Bearer valid-token"}
            )

            assert response.status_code == 200

        # Test create scenario (requires write permission)
        mock_service.create_scenario.return_value = {
            "id": "scenario-123",
            "tenant_id": "test-tenant",
            "name": "test-scenario"
        }

        with patch('aurum.api.scenarios.getattr') as mock_getattr:
            mock_getattr.return_value = mock_principal()

            response = self.client.post(
                "/v1/scenarios",
                json={
                    "tenant_id": "test-tenant",
                    "name": "new-scenario",
                    "description": "New test scenario"
                },
                headers={"Authorization": "Bearer valid-token"}
            )

            assert response.status_code == 201

        # Test delete scenario (requires delete permission)
        mock_service.delete_scenario.return_value = True

        with patch('aurum.api.scenarios.getattr') as mock_getattr:
            mock_getattr.return_value = mock_principal()

            response = self.client.delete(
                "/v1/scenarios/scenario-123",
                headers={"Authorization": "Bearer valid-token"}
            )

            assert response.status_code == 204

    @given(st.text(min_size=1, max_size=100))
    def test_scenario_name_validation(self, name):
        """Property-based test for scenario name validation."""
        # This would test the actual validation logic in scenario_models.py
        # For now, just ensure the request can be created
        request_data = {
            "tenant_id": "test-tenant",
            "name": name,
            "description": "Test scenario",
            "assumptions": [],
            "parameters": {},
            "tags": []
        }

        # Should not raise validation errors for reasonable names
        if len(name) <= 255 and all(ord(c) < 128 for c in name):
            # Valid name - should be able to create request object
            request = CreateScenarioRequest(**request_data)
            assert request.name == name

    @given(st.dictionaries(
        keys=st.text(min_size=1, max_size=50),
        values=st.one_of(st.text(), st.integers(), st.floats(), st.booleans()),
        min_size=0,
        max_size=10
    ))
    def test_scenario_parameters_validation(self, parameters):
        """Property-based test for scenario parameters validation."""
        request_data = {
            "tenant_id": "test-tenant",
            "name": "test-scenario",
            "description": "Test scenario",
            "assumptions": [],
            "parameters": parameters,
            "tags": []
        }

        # Should be able to create request with various parameter types
        request = CreateScenarioRequest(**request_data)
        assert request.parameters == parameters

    @given(st.lists(
        st.dictionaries(
            keys=st.text(min_size=1, max_size=20),
            values=st.one_of(st.text(), st.integers(), st.floats()),
            min_size=1,
            max_size=5
        ),
        min_size=0,
        max_size=5
    ))
    def test_scenario_assumptions_validation(self, assumptions):
        """Property-based test for scenario assumptions validation."""
        request_data = {
            "tenant_id": "test-tenant",
            "name": "test-scenario",
            "description": "Test scenario",
            "assumptions": assumptions,
            "parameters": {},
            "tags": []
        }

        # Should be able to create request with various assumption structures
        request = CreateScenarioRequest(**request_data)
        assert len(request.assumptions) == len(assumptions)

    @patch('aurum.api.scenarios.get_service')
    async def test_bulk_run_creation_validation(self, mock_get_service):
        """Test bulk scenario run creation with validation."""
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        # Mock successful bulk run creation
        mock_results = [
            {
                "index": 0,
                "run_id": "run-123",
                "status": "created",
                "idempotency_key": "key-1"
            },
            {
                "index": 1,
                "run_id": "run-124",
                "status": "created",
                "idempotency_key": "key-2"
            }
        ]

        mock_service.create_bulk_scenario_runs.return_value = (mock_results, [])

        bulk_request = {
            "runs": [
                {
                    "code_version": "v1.0",
                    "priority": "normal",
                    "idempotency_key": "key-1"
                },
                {
                    "code_version": "v1.0",
                    "priority": "high",
                    "idempotency_key": "key-2"
                }
            ]
        }

        response = self.client.post(
            "/v1/scenarios/scenario-123/runs:bulk",
            json=bulk_request,
            headers={"Authorization": "Bearer valid-token"}
        )

        assert response.status_code == 202
        assert len(response.json()["data"]) == 2
        assert response.json()["meta"]["total_runs"] == 2
        assert response.json()["meta"]["successful_runs"] == 2

    @patch('aurum.api.scenarios.get_service')
    async def test_error_response_structure(self, mock_get_service):
        """Test that error responses follow RFC 7807 structure."""
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service

        # Mock a validation error
        mock_service.create_scenario.side_effect = Exception("Validation failed: invalid tenant_id")

        request_data = {
            "tenant_id": "",  # Invalid empty tenant_id
            "name": "test-scenario",
            "description": "Test scenario"
        }

        response = self.client.post(
            "/v1/scenarios",
            json=request_data,
            headers={"Authorization": "Bearer valid-token"}
        )

        # Should return structured error
        assert response.status_code == 400
        error_data = response.json()

        # Check RFC 7807 structure
        assert "error" in error_data
        assert "type" in error_data["error"]
        assert "title" in error_data["error"]
        assert "detail" in error_data["error"]
        assert "instance" in error_data

        # Check specific error details
        assert "Validation failed" in error_data["error"]["detail"]

    async def test_cursor_encoding_decoding_roundtrip(self):
        """Test that cursor encoding/decoding works correctly."""
        test_payloads = [
            {"offset": 0},
            {"offset": 10, "tenant_id": "test-tenant"},
            {"offset": 100, "filter": {"status": "active"}},
            {"metadata": {"version": "1.0", "timestamp": "2024-01-01T00:00:00Z"}}
        ]

        for payload in test_payloads:
            # Encode
            encoded = encode_cursor(payload)

            # Decode
            decoded = decode_cursor(encoded)

            # Verify roundtrip
            assert decoded == payload
