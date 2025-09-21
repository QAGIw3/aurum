"""Tests for bulk scenario run submission API with idempotency keys."""

from __future__ import annotations

import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import uuid4

from fastapi import HTTPException
from fastapi.testclient import TestClient

from aurum.api.models import (
    BulkScenarioRunRequest,
    BulkScenarioRunItem,
    BulkScenarioRunResponse,
    BulkScenarioRunResult,
    BulkScenarioRunDuplicate,
    ScenarioRunPriority,
)
from aurum.api.scenarios import router as scenarios_router
from aurum.api.app import create_app


class TestBulkScenarioRunModels:
    """Test bulk scenario run Pydantic models."""

    def test_bulk_scenario_run_request_valid(self):
        """Test valid bulk scenario run request."""
        request = BulkScenarioRunRequest(
            scenario_id="test-scenario-id",
            runs=[
                BulkScenarioRunItem(
                    idempotency_key="run-1",
                    priority=ScenarioRunPriority.NORMAL,
                    timeout_minutes=60,
                    max_memory_mb=1024,
                ),
                BulkScenarioRunItem(
                    idempotency_key="run-2",
                    priority=ScenarioRunPriority.HIGH,
                    timeout_minutes=120,
                    max_memory_mb=2048,
                )
            ],
            idempotency_key="bulk-key"
        )

        assert request.scenario_id == "test-scenario-id"
        assert len(request.runs) == 2
        assert request.idempotency_key == "bulk-key"

    def test_bulk_scenario_run_request_empty_runs(self):
        """Test bulk request with empty runs."""
        with pytest.raises(Exception, match="min_items"):
            BulkScenarioRunRequest(
                scenario_id="test-scenario-id",
                runs=[]
            )

    def test_bulk_scenario_run_request_too_many_runs(self):
        """Test bulk request with too many runs."""
        runs = [
            BulkScenarioRunItem(idempotency_key=f"run-{i}")
            for i in range(101)
        ]

        with pytest.raises(Exception, match="max_items"):
            BulkScenarioRunRequest(
                scenario_id="test-scenario-id",
                runs=runs
            )

    def test_bulk_scenario_run_request_duplicate_idempotency_keys(self):
        """Test bulk request with duplicate idempotency keys in request."""
        runs = [
            BulkScenarioRunItem(idempotency_key="duplicate-key"),
            BulkScenarioRunItem(idempotency_key="duplicate-key"),
        ]

        with pytest.raises(Exception, match="Duplicate idempotency key"):
            BulkScenarioRunRequest(
                scenario_id="test-scenario-id",
                runs=runs
            )

    def test_bulk_scenario_run_item_valid(self):
        """Test valid bulk scenario run item."""
        item = BulkScenarioRunItem(
            idempotency_key="test-run",
            priority=ScenarioRunPriority.CRITICAL,
            timeout_minutes=30,
            max_memory_mb=512,
            environment={"TEST": "value"},
            parameters={"param": "value"}
        )

        assert item.idempotency_key == "test-run"
        assert item.priority == ScenarioRunPriority.CRITICAL
        assert item.timeout_minutes == 30
        assert item.max_memory_mb == 512
        assert item.environment == {"TEST": "value"}
        assert item.parameters == {"param": "value"}

    def test_bulk_scenario_run_item_invalid_idempotency_key(self):
        """Test bulk scenario run item with invalid idempotency key."""
        with pytest.raises(ValueError, match="idempotency_key cannot be empty"):
            BulkScenarioRunItem(idempotency_key="")

    def test_bulk_scenario_run_item_invalid_timeout(self):
        """Test bulk scenario run item with invalid timeout."""
        with pytest.raises(ValueError):
            BulkScenarioRunItem(timeout_minutes=0)  # Must be >= 1

        with pytest.raises(ValueError):
            BulkScenarioRunItem(timeout_minutes=1500)  # Must be <= 1440

    def test_bulk_scenario_run_item_invalid_memory(self):
        """Test bulk scenario run item with invalid memory."""
        with pytest.raises(ValueError):
            BulkScenarioRunItem(max_memory_mb=100)  # Must be >= 256

        with pytest.raises(ValueError):
            BulkScenarioRunItem(max_memory_mb=20000)  # Must be <= 16384

    def test_bulk_scenario_run_response_creation(self):
        """Test bulk scenario run response creation."""
        results = [
            BulkScenarioRunResult(
                index=0,
                idempotency_key="run-1",
                run_id="run-id-1",
                status="created",
                error=None
            ),
            BulkScenarioRunResult(
                index=1,
                idempotency_key="run-2",
                run_id="run-id-2",
                status="created",
                error=None
            )
        ]

        duplicates = [
            BulkScenarioRunDuplicate(
                index=2,
                idempotency_key="run-3",
                existing_run_id="existing-run-id",
                existing_status="running",
                created_at=datetime.utcnow()
            )
        ]

        response = BulkScenarioRunResponse(
            meta={
                "request_id": "test-req-id",
                "total_runs": 3,
                "successful_runs": 2,
                "duplicate_runs": 1,
                "failed_runs": 0,
            },
            data=results,
            duplicates=duplicates
        )

        assert len(response.data) == 2
        assert len(response.duplicates) == 1
        assert response.meta["total_runs"] == 3

    def test_bulk_scenario_run_result_creation(self):
        """Test bulk scenario run result creation."""
        result = BulkScenarioRunResult(
            index=0,
            idempotency_key="test-key",
            run_id="run-123",
            status="created",
            error=None
        )

        assert result.index == 0
        assert result.idempotency_key == "test-key"
        assert result.run_id == "run-123"
        assert result.status == "created"
        assert result.error is None

    def test_bulk_scenario_run_duplicate_creation(self):
        """Test bulk scenario run duplicate creation."""
        duplicate = BulkScenarioRunDuplicate(
            index=1,
            idempotency_key="duplicate-key",
            existing_run_id="existing-run-456",
            existing_status="running",
            created_at=datetime.utcnow()
        )

        assert duplicate.index == 1
        assert duplicate.idempotency_key == "duplicate-key"
        assert duplicate.existing_run_id == "existing-run-456"
        assert duplicate.existing_status == "running"

    def test_model_serialization(self):
        """Test model JSON serialization/deserialization."""
        request = BulkScenarioRunRequest(
            scenario_id="scenario-123",
            runs=[
                BulkScenarioRunItem(
                    idempotency_key="run-1",
                    priority=ScenarioRunPriority.NORMAL
                )
            ]
        )

        # Should serialize to JSON
        json_str = request.json()
        assert isinstance(json_str, str)

        # Should deserialize from JSON
        parsed = BulkScenarioRunRequest.parse_raw(json_str)
        assert parsed.scenario_id == request.scenario_id
        assert len(parsed.runs) == len(request.runs)


class TestBulkScenarioRunAPI:
    """Test bulk scenario run API endpoints."""

    @pytest.fixture
    def app(self):
        """Create test app."""
        app = create_app()
        app.include_router(scenarios_router)
        return app

    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return TestClient(app)

    @pytest.fixture
    def mock_service(self):
        """Mock AsyncScenarioService."""
        service = AsyncMock()
        service.create_bulk_scenario_runs.return_value = (
            [
                {
                    "index": 0,
                    "idempotency_key": "run-1",
                    "run_id": "run-id-1",
                    "status": "created",
                    "error": None
                },
                {
                    "index": 1,
                    "idempotency_key": "run-2",
                    "run_id": "run-id-2",
                    "status": "created",
                    "error": None
                }
            ],
            []
        )
        return service

    @pytest.fixture
    def mock_auth(self):
        """Mock authentication."""
        with patch("aurum.api.scenarios.require_permission"):
            yield

    def test_bulk_scenario_runs_success(self, client, mock_service, mock_auth):
        """Test successful bulk scenario run creation."""
        with patch("aurum.api.container.get_service", return_value=mock_service):
            response = client.post(
                "/v1/scenarios/test-scenario-id/runs:bulk",
                json={
                    "scenario_id": "test-scenario-id",
                    "runs": [
                        {
                            "idempotency_key": "run-1",
                            "priority": "normal",
                            "timeout_minutes": 60,
                            "max_memory_mb": 1024
                        },
                        {
                            "idempotency_key": "run-2",
                            "priority": "high",
                            "timeout_minutes": 120,
                            "max_memory_mb": 2048
                        }
                    ]
                },
                headers={"X-Aurum-Tenant": "test-tenant"}
            )

        assert response.status_code == 202

        data = response.json()
        assert "meta" in data
        assert "data" in data
        assert "duplicates" in data
        assert len(data["data"]) == 2
        assert data["meta"]["total_runs"] == 2
        assert data["meta"]["successful_runs"] == 2
        assert data["meta"]["duplicate_runs"] == 0
        assert data["meta"]["failed_runs"] == 0

    def test_bulk_scenario_runs_with_duplicates(self, client, mock_service, mock_auth):
        """Test bulk scenario run creation with duplicates."""
        mock_service.create_bulk_scenario_runs.return_value = (
            [
                {
                    "index": 0,
                    "idempotency_key": "run-1",
                    "run_id": "run-id-1",
                    "status": "created",
                    "error": None
                }
            ],
            [
                {
                    "index": 1,
                    "idempotency_key": "run-2",
                    "existing_run_id": "existing-run-id",
                    "existing_status": "running",
                    "created_at": datetime.utcnow().isoformat()
                }
            ]
        )

        with patch("aurum.api.container.get_service", return_value=mock_service):
            response = client.post(
                "/v1/scenarios/test-scenario-id/runs:bulk",
                json={
                    "scenario_id": "test-scenario-id",
                    "runs": [
                        {"idempotency_key": "run-1"},
                        {"idempotency_key": "run-2"}
                    ]
                },
                headers={"X-Aurum-Tenant": "test-tenant"}
            )

        assert response.status_code == 202

        data = response.json()
        assert len(data["data"]) == 1
        assert len(data["duplicates"]) == 1
        assert data["meta"]["total_runs"] == 2
        assert data["meta"]["successful_runs"] == 1
        assert data["meta"]["duplicate_runs"] == 1

    def test_bulk_scenario_runs_validation_error(self, client, mock_service, mock_auth):
        """Test bulk scenario run validation errors."""
        with patch("aurum.api.container.get_service", return_value=mock_service):
            # Empty runs
            response = client.post(
                "/v1/scenarios/test-scenario-id/runs:bulk",
                json={
                    "scenario_id": "test-scenario-id",
                    "runs": []
                }
            )

            assert response.status_code == 400

    def test_bulk_scenario_runs_invalid_scenario_id(self, client, mock_service, mock_auth):
        """Test bulk scenario run with invalid scenario ID."""
        with patch("aurum.api.container.get_service", return_value=mock_service):
            response = client.post(
                "/v1/scenarios/invalid-uuid/runs:bulk",
                json={
                    "scenario_id": "invalid-uuid",
                    "runs": [{"idempotency_key": "run-1"}]
                }
            )

            assert response.status_code == 400
            data = response.json()
            assert "Invalid scenario ID format" in str(data)

    def test_bulk_scenario_runs_service_error(self, client, mock_service, mock_auth):
        """Test bulk scenario run service errors."""
        mock_service.create_bulk_scenario_runs.side_effect = Exception("Database error")

        with patch("aurum.api.container.get_service", return_value=mock_service):
            response = client.post(
                "/v1/scenarios/test-scenario-id/runs:bulk",
                json={
                    "scenario_id": "test-scenario-id",
                    "runs": [{"idempotency_key": "run-1"}]
                }
            )

            assert response.status_code == 500
            data = response.json()
            assert "Failed to create bulk scenario runs" in str(data)

    def test_bulk_scenario_runs_too_many_runs(self, client, mock_service, mock_auth):
        """Test bulk scenario run with too many runs."""
        with patch("aurum.api.container.get_service", return_value=mock_service):
            # Create 101 runs
            runs = [{"idempotency_key": f"run-{i}"} for i in range(101)]

            response = client.post(
                "/v1/scenarios/test-scenario-id/runs:bulk",
                json={
                    "scenario_id": "test-scenario-id",
                    "runs": runs
                }
            )

            assert response.status_code == 400
            data = response.json()
            assert "Cannot create more than 100 runs" in str(data)

    def test_bulk_scenario_runs_duplicate_keys_in_request(self, client, mock_service, mock_auth):
        """Test bulk scenario run with duplicate idempotency keys in request."""
        with patch("aurum.api.container.get_service", return_value=mock_service):
            response = client.post(
                "/v1/scenarios/test-scenario-id/runs:bulk",
                json={
                    "scenario_id": "test-scenario-id",
                    "runs": [
                        {"idempotency_key": "duplicate"},
                        {"idempotency_key": "duplicate"}
                    ]
                }
            )

            assert response.status_code == 400
            data = response.json()
            assert "Duplicate idempotency key" in str(data)


class TestBulkScenarioRunService:
    """Test bulk scenario run service implementation."""

    @pytest.fixture
    def service(self):
        """Create AsyncScenarioService instance."""
        from aurum.api.async_service import AsyncScenarioService
        return AsyncScenarioService(settings=None)

    async def test_create_bulk_scenario_runs_success(self, service):
        """Test successful bulk scenario run creation."""
        runs = [
            {"idempotency_key": "run-1", "priority": "normal"},
            {"idempotency_key": "run-2", "priority": "high"}
        ]

        results, duplicates = await service.create_bulk_scenario_runs(
            scenario_id="test-scenario-id",
            runs=runs,
            bulk_idempotency_key="bulk-key"
        )

        assert len(results) == 2
        assert len(duplicates) == 0

        for result in results:
            assert result["status"] == "created"
            assert result["run_id"] is not None
            assert result["error"] is None

    async def test_create_bulk_scenario_runs_with_duplicates(self, service):
        """Test bulk scenario run creation with existing duplicates."""
        runs = [
            {"idempotency_key": "run-1", "priority": "normal"},
            {"idempotency_key": "existing-run", "priority": "normal"}
        ]

        # Mock database lookup to return existing run
        with patch.object(service, '_find_existing_run', return_value={
            "id": "existing-run-id",
            "status": "running",
            "created_at": datetime.utcnow()
        }):
            results, duplicates = await service.create_bulk_scenario_runs(
                scenario_id="test-scenario-id",
                runs=runs
            )

        assert len(results) == 1  # Only the non-duplicate run
        assert len(duplicates) == 1  # The duplicate run

        # Check results
        assert results[0]["status"] == "created"
        assert results[0]["idempotency_key"] == "run-1"

        # Check duplicates
        assert duplicates[0]["idempotency_key"] == "existing-run"
        assert duplicates[0]["existing_run_id"] == "existing-run-id"
        assert duplicates[0]["existing_status"] == "running"

    async def test_create_bulk_scenario_runs_with_errors(self, service):
        """Test bulk scenario run creation with errors."""
        runs = [
            {"idempotency_key": "run-1", "priority": "normal"},
            {"idempotency_key": "invalid-run", "priority": "invalid-priority"}  # Invalid priority
        ]

        results, duplicates = await service.create_bulk_scenario_runs(
            scenario_id="test-scenario-id",
            runs=runs
        )

        # Should have mixed results
        successful = [r for r in results if r["status"] == "created"]
        failed = [r for r in results if r["status"] == "failed"]

        assert len(successful) == 1
        assert len(failed) == 1
        assert len(duplicates) == 0

    async def test_create_bulk_scenario_runs_with_bulk_idempotency(self, service):
        """Test bulk scenario run creation with bulk-level idempotency."""
        runs = [{"idempotency_key": "run-1", "priority": "normal"}]

        # First call
        results1, duplicates1 = await service.create_bulk_scenario_runs(
            scenario_id="test-scenario-id",
            runs=runs,
            bulk_idempotency_key="bulk-key-123"
        )

        # Second call with same bulk key should be handled appropriately
        results2, duplicates2 = await service.create_bulk_scenario_runs(
            scenario_id="test-scenario-id",
            runs=runs,
            bulk_idempotency_key="bulk-key-123"
        )

        assert len(results1) == 1
        assert len(results2) == 1


class TestBulkScenarioRunIntegration:
    """Test bulk scenario run integration scenarios."""

    def test_bulk_request_validation_flow(self):
        """Test complete validation flow for bulk requests."""
        # Valid request
        request = BulkScenarioRunRequest(
            scenario_id="valid-scenario-id",
            runs=[
                BulkScenarioRunItem(
                    idempotency_key="valid-run-1",
                    priority=ScenarioRunPriority.NORMAL
                ),
                BulkScenarioRunItem(
                    idempotency_key="valid-run-2",
                    priority=ScenarioRunPriority.HIGH
                )
            ]
        )

        assert request.scenario_id == "valid-scenario-id"
        assert len(request.runs) == 2

        # Convert to dict for API call
        request_dict = request.dict()
        assert "scenario_id" in request_dict
        assert "runs" in request_dict

    def test_bulk_response_processing(self):
        """Test processing of bulk response data."""
        results = [
            BulkScenarioRunResult(
                index=0,
                idempotency_key="run-1",
                run_id="run-id-1",
                status="created",
                error=None
            ),
            BulkScenarioRunResult(
                index=1,
                idempotency_key="run-2",
                run_id="run-id-2",
                status="failed",
                error="Invalid configuration"
            )
        ]

        duplicates = [
            BulkScenarioRunDuplicate(
                index=2,
                idempotency_key="run-3",
                existing_run_id="existing-run-id",
                existing_status="running",
                created_at=datetime.utcnow()
            )
        ]

        # Calculate summary statistics
        successful = len([r for r in results if r.status == "created"])
        failed = len([r for r in results if r.status == "failed"])
        total_duplicates = len(duplicates)

        assert successful == 1
        assert failed == 1
        assert total_duplicates == 1

    def test_idempotency_key_uniqueness_enforcement(self):
        """Test that idempotency keys must be unique within a request."""
        # Create request with duplicate keys in the list
        runs = [
            BulkScenarioRunItem(idempotency_key="duplicate"),
            BulkScenarioRunItem(idempotency_key="duplicate"),
        ]

        # Should raise validation error
        with pytest.raises(Exception, match="Duplicate idempotency key"):
            BulkScenarioRunRequest(
                scenario_id="test-scenario-id",
                runs=runs
            )

    def test_bulk_request_size_limits(self):
        """Test bulk request size limits."""
        # Test minimum size
        with pytest.raises(Exception):
            BulkScenarioRunRequest(
                scenario_id="test-scenario-id",
                runs=[]
            )

        # Test maximum size (create 101 runs)
        runs = [BulkScenarioRunItem(idempotency_key=f"run-{i}") for i in range(101)]

        with pytest.raises(Exception):
            BulkScenarioRunRequest(
                scenario_id="test-scenario-id",
                runs=runs
            )

    def test_response_metadata_calculation(self):
        """Test calculation of response metadata."""
        results = [
            BulkScenarioRunResult(index=0, run_id="run-1", status="created", error=None),
            BulkScenarioRunResult(index=1, run_id="run-2", status="created", error=None),
            BulkScenarioRunResult(index=2, run_id="run-3", status="failed", error="Error"),
            BulkScenarioRunResult(index=3, run_id="run-4", status="created", error=None),
        ]

        duplicates = [
            BulkScenarioRunDuplicate(
                index=4,
                existing_run_id="existing-1",
                existing_status="running",
                created_at=datetime.utcnow()
            )
        ]

        # Calculate metadata
        total_runs = len(results) + len(duplicates)
        successful_runs = len([r for r in results if r.status == "created"])
        failed_runs = len([r for r in results if r.status == "failed"])
        duplicate_runs = len(duplicates)

        assert total_runs == 5
        assert successful_runs == 3
        assert failed_runs == 1
        assert duplicate_runs == 1

    def test_error_handling_and_reporting(self):
        """Test error handling and reporting in bulk operations."""
        results = [
            BulkScenarioRunResult(
                index=0,
                run_id="run-1",
                status="created",
                error=None
            ),
            BulkScenarioRunResult(
                index=1,
                run_id=None,
                status="failed",
                error="Database connection failed"
            ),
            BulkScenarioRunResult(
                index=2,
                run_id="run-3",
                status="created",
                error=None
            ),
        ]

        # Check that errors are properly reported
        for result in results:
            if result.status == "failed":
                assert result.error is not None
                assert result.run_id is None
            else:
                assert result.error is None
                assert result.run_id is not None

    def test_empty_results_handling(self):
        """Test handling of empty results."""
        results = []
        duplicates = []

        # Should handle empty lists gracefully
        assert len(results) == 0
        assert len(duplicates) == 0

        # Metadata should reflect empty state
        total_runs = len(results) + len(duplicates)
        assert total_runs == 0

    def test_mixed_success_and_failure_scenarios(self):
        """Test scenarios with mixed success and failure."""
        results = [
            BulkScenarioRunResult(index=0, run_id="success-1", status="created", error=None),
            BulkScenarioRunResult(index=1, run_id=None, status="failed", error="Validation error"),
            BulkScenarioRunResult(index=2, run_id="success-2", status="created", error=None),
            BulkScenarioRunResult(index=3, run_id=None, status="failed", error="Permission denied"),
        ]

        duplicates = [
            BulkScenarioRunDuplicate(
                index=4,
                existing_run_id="existing-1",
                existing_status="running",
                created_at=datetime.utcnow()
            )
        ]

        # Verify mixed state
        created_runs = [r for r in results if r.status == "created"]
        failed_runs = [r for r in results if r.status == "failed"]

        assert len(created_runs) == 2
        assert len(failed_runs) == 2
        assert len(duplicates) == 1

        # All failed runs should have error messages
        for run in failed_runs:
            assert run.error is not None

        # All successful runs should have run IDs
        for run in created_runs:
            assert run.run_id is not None
