"""Tests for scenario database persistence layer."""

from __future__ import annotations

import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import uuid4

from fastapi import HTTPException

from aurum.api.scenario_models import (
    ScenarioData,
    ScenarioRunData,
    ScenarioStatus,
    ScenarioRunStatus,
    ScenarioRunPriority,
    ScenarioOutputPoint,
)
from aurum.scenarios.storage import ScenarioStore


class TestScenarioStore:
    """Test ScenarioStore database operations."""

    @pytest.fixture
    def mock_pool(self):
        """Mock asyncpg pool."""
        pool = AsyncMock()
        conn = AsyncMock()

        # Mock connection context manager
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

        return pool

    @pytest.fixture
    def store(self, mock_pool):
        """Create ScenarioStore instance with mocked pool."""
        store = ScenarioStore("postgresql://test")
        store.pool = mock_pool
        return store

    @pytest.fixture
    def sample_scenario_data(self):
        """Sample scenario data for testing."""
        return {
            "name": "Test Scenario",
            "description": "Test description",
            "status": "created",
            "assumptions": [{"type": "policy", "value": "test"}],
            "parameters": {"param1": "value1"},
            "tags": ["tag1", "tag2"],
            "version": 1
        }

    @pytest.fixture
    def sample_scenario_run_data(self):
        """Sample scenario run data for testing."""
        return {
            "status": "queued",
            "priority": "normal",
            "parameters": {"run_param": "value"},
            "environment": {"ENV": "test"}
        }

    async def test_create_scenario_success(self, store, mock_pool, sample_scenario_data):
        """Test successful scenario creation."""
        # Mock database response
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = None

        # Mock get_scenario to return the created scenario
        store.get_scenario = AsyncMock(return_value=ScenarioData(
            id="test-scenario-id",
            tenant_id="test-tenant",
            name=sample_scenario_data["name"],
            description=sample_scenario_data["description"],
            status=ScenarioStatus.CREATED,
            assumptions=sample_scenario_data["assumptions"],
            parameters=sample_scenario_data["parameters"],
            tags=sample_scenario_data["tags"],
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            created_by="test-user",
            version=1
        ))

        result = await store.create_scenario(sample_scenario_data)

        assert result is not None
        assert result.name == sample_scenario_data["name"]
        assert result.status == ScenarioStatus.CREATED
        mock_conn.execute.assert_called_once()

    async def test_get_scenario_found(self, store, mock_pool):
        """Test getting existing scenario."""
        scenario_id = "test-scenario-id"
        tenant_id = "test-tenant"

        # Mock database response
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {
            "id": scenario_id,
            "tenant_id": tenant_id,
            "name": "Test Scenario",
            "description": "Test description",
            "status": "created",
            "assumptions": '[{"type": "policy", "value": "test"}]',
            "parameters": '{"param1": "value1"}',
            "tags": '["tag1", "tag2"]',
            "version": 1,
            "created_by": "test-user",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        result = await store.get_scenario(scenario_id)

        assert result is not None
        assert result.id == scenario_id
        assert result.tenant_id == tenant_id
        assert result.status == ScenarioStatus.CREATED
        mock_conn.fetchrow.assert_called_once()

    async def test_get_scenario_not_found(self, store, mock_pool):
        """Test getting non-existent scenario."""
        scenario_id = "non-existent-id"

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = None

        result = await store.get_scenario(scenario_id)

        assert result is None

    async def test_list_scenarios_with_filters(self, store, mock_pool):
        """Test listing scenarios with status filter."""
        tenant_id = "test-tenant"
        status = "active"

        # Mock database responses
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {"total": 5}
        mock_conn.fetch.return_value = [
            {
                "id": "scenario-1",
                "tenant_id": tenant_id,
                "name": "Scenario 1",
                "description": "Description 1",
                "status": "active",
                "assumptions": "[]",
                "parameters": "{}",
                "tags": "[]",
                "version": 1,
                "created_by": "user1",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            },
            {
                "id": "scenario-2",
                "tenant_id": tenant_id,
                "name": "Scenario 2",
                "description": "Description 2",
                "status": "active",
                "assumptions": "[]",
                "parameters": "{}",
                "tags": "[]",
                "version": 1,
                "created_by": "user2",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
        ]

        scenarios, total = await store.list_scenarios(
            tenant_id=tenant_id,
            status=status,
            limit=10,
            offset=0
        )

        assert len(scenarios) == 2
        assert total == 5
        assert all(s.status == ScenarioStatus.ACTIVE for s in scenarios)

    async def test_update_scenario(self, store, mock_pool):
        """Test updating scenario fields."""
        scenario_id = "test-scenario-id"
        updates = {
            "name": "Updated Scenario",
            "description": "Updated description",
            "status": "active"
        }

        # Mock database response
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = None

        # Mock get_scenario to return updated scenario
        store.get_scenario = AsyncMock(return_value=ScenarioData(
            id=scenario_id,
            tenant_id="test-tenant",
            name=updates["name"],
            description=updates["description"],
            status=ScenarioStatus.ACTIVE,
            assumptions=[],
            parameters={},
            tags=[],
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            created_by="test-user",
            version=1
        ))

        result = await store.update_scenario(scenario_id, updates)

        assert result is not None
        assert result.name == updates["name"]
        assert result.description == updates["description"]
        assert result.status == ScenarioStatus.ACTIVE
        mock_conn.execute.assert_called_once()

    async def test_delete_scenario_success(self, store, mock_pool):
        """Test successful scenario deletion."""
        scenario_id = "test-scenario-id"

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = "DELETE 1"

        result = await store.delete_scenario(scenario_id)

        assert result is True
        mock_conn.execute.assert_called_once()

    async def test_delete_scenario_not_found(self, store, mock_pool):
        """Test deleting non-existent scenario."""
        scenario_id = "non-existent-id"

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = "DELETE 0"

        result = await store.delete_scenario(scenario_id)

        assert result is False

    async def test_create_run_success(self, store, mock_pool, sample_scenario_run_data):
        """Test successful scenario run creation."""
        scenario_id = "test-scenario-id"

        # Mock database response
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = None

        # Mock get_run to return the created run
        store.get_run = AsyncMock(return_value=ScenarioRunData(
            id="test-run-id",
            scenario_id=scenario_id,
            status=ScenarioRunStatus.QUEUED,
            priority=ScenarioRunPriority.NORMAL,
            parameters=sample_scenario_run_data["parameters"],
            environment=sample_scenario_run_data["environment"],
            created_at=datetime.utcnow(),
            queued_at=datetime.utcnow(),
            retry_count=0,
            max_retries=3
        ))

        result = await store.create_run(scenario_id, sample_scenario_run_data)

        assert result is not None
        assert result.scenario_id == scenario_id
        assert result.status == ScenarioRunStatus.QUEUED
        mock_conn.execute.assert_called_once()

    async def test_get_run_found(self, store, mock_pool):
        """Test getting existing scenario run."""
        scenario_id = "test-scenario-id"
        run_id = "test-run-id"

        # Mock database response
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {
            "id": run_id,
            "scenario_id": scenario_id,
            "tenant_id": "test-tenant",
            "status": "running",
            "priority": "normal",
            "started_at": datetime.utcnow(),
            "completed_at": None,
            "duration_seconds": None,
            "error_message": None,
            "retry_count": 0,
            "max_retries": 3,
            "progress_percent": 50.0,
            "parameters": '{"run_param": "value"}',
            "environment": '{"ENV": "test"}',
            "created_at": datetime.utcnow(),
            "queued_at": datetime.utcnow(),
            "cancelled_at": None
        }

        result = await store.get_run(scenario_id, run_id)

        assert result is not None
        assert result.id == run_id
        assert result.scenario_id == scenario_id
        assert result.status == ScenarioRunStatus.RUNNING
        assert result.progress_percent == 50.0
        mock_conn.fetchrow.assert_called_once()

    async def test_get_run_not_found(self, store, mock_pool):
        """Test getting non-existent scenario run."""
        scenario_id = "test-scenario-id"
        run_id = "non-existent-run-id"

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = None

        result = await store.get_run(scenario_id, run_id)

        assert result is None

    async def test_list_runs(self, store, mock_pool):
        """Test listing scenario runs."""
        scenario_id = "test-scenario-id"

        # Mock database responses
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {"total": 3}
        mock_conn.fetch.return_value = [
            {
                "id": "run-1",
                "scenario_id": scenario_id,
                "tenant_id": "test-tenant",
                "status": "completed",
                "priority": "normal",
                "started_at": datetime.utcnow() - timedelta(hours=1),
                "completed_at": datetime.utcnow(),
                "duration_seconds": 3600.0,
                "error_message": None,
                "retry_count": 0,
                "max_retries": 3,
                "progress_percent": 100.0,
                "parameters": "{}",
                "environment": "{}",
                "created_at": datetime.utcnow() - timedelta(hours=2),
                "queued_at": datetime.utcnow() - timedelta(hours=2),
                "cancelled_at": None
            },
            {
                "id": "run-2",
                "scenario_id": scenario_id,
                "tenant_id": "test-tenant",
                "status": "running",
                "priority": "high",
                "started_at": datetime.utcnow() - timedelta(minutes=30),
                "completed_at": None,
                "duration_seconds": None,
                "error_message": None,
                "retry_count": 0,
                "max_retries": 3,
                "progress_percent": 75.0,
                "parameters": "{}",
                "environment": "{}",
                "created_at": datetime.utcnow() - timedelta(hours=1),
                "queued_at": datetime.utcnow() - timedelta(hours=1),
                "cancelled_at": None
            }
        ]

        runs, total = await store.list_runs(scenario_id, limit=10, offset=0)

        assert len(runs) == 2
        assert total == 3
        assert runs[0].status == ScenarioRunStatus.COMPLETED
        assert runs[1].status == ScenarioRunStatus.RUNNING

    async def test_update_run_state(self, store, mock_pool):
        """Test updating scenario run state."""
        run_id = "test-run-id"
        state_update = {
            "status": "running",
            "started_at": datetime.utcnow(),
            "progress_percent": 25.0
        }

        # Mock database response
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = None

        # Mock get_run to return updated run
        store.get_run = AsyncMock(return_value=ScenarioRunData(
            id=run_id,
            scenario_id="test-scenario-id",
            status=ScenarioRunStatus.RUNNING,
            priority=ScenarioRunPriority.NORMAL,
            started_at=state_update["started_at"],
            progress_percent=state_update["progress_percent"],
            created_at=datetime.utcnow(),
            queued_at=datetime.utcnow(),
        ))

        result = await store.update_run_state(run_id, state_update)

        assert result is not None
        assert result.status == ScenarioRunStatus.RUNNING
        mock_conn.execute.assert_called_once()

    async def test_cancel_run(self, store, mock_pool):
        """Test cancelling a scenario run."""
        run_id = "test-run-id"

        # Mock update_run_state
        store.update_run_state = AsyncMock(return_value=ScenarioRunData(
            id=run_id,
            scenario_id="test-scenario-id",
            status=ScenarioRunStatus.CANCELLED,
            priority=ScenarioRunPriority.NORMAL,
            cancelled_at=datetime.utcnow(),
            created_at=datetime.utcnow(),
            queued_at=datetime.utcnow(),
        ))

        result = await store.cancel_run(run_id)

        assert result is not None
        assert result.status == ScenarioRunStatus.CANCELLED
        store.update_run_state.assert_called_once_with(run_id, {
            "status": "cancelled",
            "cancelled_at": datetime.utcnow(),
        })

    async def test_get_outputs_with_filters(self, store, mock_pool):
        """Test getting scenario outputs with filtering."""
        scenario_run_id = "test-run-id"
        start_time = "2024-01-01T00:00:00Z"
        end_time = "2024-01-02T00:00:00Z"
        metric_name = "test_metric"

        # Mock database responses
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {"total": 5}
        mock_conn.fetch.return_value = [
            {
                "id": "output-1",
                "scenario_run_id": scenario_run_id,
                "tenant_id": "test-tenant",
                "timestamp": datetime.utcnow(),
                "metric_name": metric_name,
                "value": 100.0,
                "unit": "MW",
                "tags": '{"region": "CA"}',
                "created_at": datetime.utcnow()
            },
            {
                "id": "output-2",
                "scenario_run_id": scenario_run_id,
                "tenant_id": "test-tenant",
                "timestamp": datetime.utcnow() + timedelta(hours=1),
                "metric_name": metric_name,
                "value": 150.0,
                "unit": "MW",
                "tags": '{"region": "TX"}',
                "created_at": datetime.utcnow()
            }
        ]

        outputs, total = await store.get_outputs(
            scenario_run_id=scenario_run_id,
            limit=10,
            offset=0,
            start_time=start_time,
            end_time=end_time,
            metric_name=metric_name
        )

        assert len(outputs) == 2
        assert total == 5
        assert all(o.metric_name == metric_name for o in outputs)
        assert all(o.unit == "MW" for o in outputs)

    async def test_get_feature_flag_exists(self, store, mock_pool):
        """Test getting existing feature flag."""
        feature_name = "scenario_outputs"

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {
            "feature_name": feature_name,
            "enabled": True,
            "configuration": '{"max_outputs": 1000}',
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        result = await store.get_feature_flag(feature_name)

        assert result is not None
        assert result["feature_name"] == feature_name
        assert result["enabled"] is True
        assert result["configuration"]["max_outputs"] == 1000

    async def test_get_feature_flag_not_found(self, store, mock_pool):
        """Test getting non-existent feature flag."""
        feature_name = "non_existent_feature"

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = None

        result = await store.get_feature_flag(feature_name)

        assert result is None

    async def test_set_feature_flag(self, store, mock_pool):
        """Test setting feature flag."""
        feature_name = "new_feature"
        enabled = True
        configuration = {"timeout": 300}

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = None

        # Mock get_feature_flag to return the set flag
        store.get_feature_flag = AsyncMock(return_value={
            "feature_name": feature_name,
            "enabled": enabled,
            "configuration": configuration,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        })

        result = await store.set_feature_flag(feature_name, enabled, configuration)

        assert result is not None
        assert result["feature_name"] == feature_name
        assert result["enabled"] is True
        mock_conn.execute.assert_called_once()

    async def test_store_not_initialized_error(self):
        """Test error when store is not initialized."""
        store = ScenarioStore("postgresql://test")

        with pytest.raises(RuntimeError, match="ScenarioStore not initialized"):
            async with store.get_connection():
                pass

    async def test_pool_initialization(self, mock_pool):
        """Test pool initialization."""
        store = ScenarioStore("postgresql://test")

        with patch("aurum.scenarios.storage.asyncpg.create_pool", return_value=mock_pool):
            await store.initialize()

        assert store.pool == mock_pool
        mock_pool.assert_called_once()

    async def test_pool_close(self, mock_pool):
        """Test pool closing."""
        store = ScenarioStore("postgresql://test")
        store.pool = mock_pool

        await store.close()

        mock_pool.close.assert_called_once()
        assert store.pool is None


class TestScenarioStoreIntegration:
    """Integration tests for ScenarioStore."""

    def test_scenario_lifecycle_integration(self):
        """Test complete scenario lifecycle operations."""
        # This would test the full flow of:
        # 1. Create scenario
        # 2. Create run
        # 3. Update run state
        # 4. Cancel run
        # 5. List scenarios and runs
        # 6. Delete scenario
        pass  # Placeholder for integration tests

    def test_feature_flag_integration(self):
        """Test feature flag operations."""
        # This would test:
        # 1. Set feature flag
        # 2. Get feature flag
        # 3. Update feature flag
        # 4. Feature flag inheritance
        pass  # Placeholder for integration tests

    def test_tenant_isolation(self):
        """Test tenant data isolation."""
        # This would test that data is properly isolated between tenants
        pass  # Placeholder for integration tests

    def test_error_handling(self):
        """Test error handling in database operations."""
        # This would test:
        # 1. Database connection errors
        # 2. Constraint violations
        # 3. Invalid data handling
        # 4. Rollback scenarios
        pass  # Placeholder for integration tests

    def test_performance_characteristics(self):
        """Test performance characteristics."""
        # This would test:
        # 1. Query performance with large datasets
        # 2. Connection pool efficiency
        # 3. Index effectiveness
        # 4. Bulk operation performance
        pass  # Placeholder for integration tests

    def test_data_consistency(self):
        """Test data consistency across operations."""
        # This would test:
        # 1. Transaction consistency
        # 2. Cascade operations
        # 3. Referential integrity
        # 4. Audit trail completeness
        pass  # Placeholder for integration tests
