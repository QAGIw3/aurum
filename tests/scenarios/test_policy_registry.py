"""Tests for policy driver registry with versioning and parameter validation."""

from __future__ import annotations

import json
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import uuid4

from fastapi import HTTPException

from aurum.api.scenario_models import DriverType
from aurum.scenarios.policy_registry import (
    PolicyDriver,
    PolicyDriverParameter,
    PolicyDriverVersion,
    PolicyDriverTestCase,
    PolicyDriverRegistry,
)


class TestPolicyDriverModels:
    """Test policy driver Pydantic models."""

    def test_policy_driver_parameter_valid(self):
        """Test valid policy driver parameter."""
        param = PolicyDriverParameter(
            name="policy_name",
            parameter_type="string",
            required=True,
            default_value="rps_50_percent",
            validation_rules={"min_length": 3, "max_length": 100},
            description="Name of the policy driver",
            order_index=1
        )

        assert param.name == "policy_name"
        assert param.parameter_type == "string"
        assert param.required is True
        assert param.default_value == "rps_50_percent"
        assert param.order_index == 1

    def test_policy_driver_parameter_invalid_type(self):
        """Test policy driver parameter with invalid type."""
        with pytest.raises(ValueError, match="Parameter type must be one of"):
            PolicyDriverParameter(
                name="test_param",
                parameter_type="invalid_type"
            )

    def test_policy_driver_parameter_invalid_name(self):
        """Test policy driver parameter with invalid name."""
        with pytest.raises(ValueError, match="Parameter name must be a valid identifier"):
            PolicyDriverParameter(
                name="123invalid",
                parameter_type="string"
            )

    def test_policy_driver_version_valid(self):
        """Test valid policy driver version."""
        version = PolicyDriverVersion(
            version="1.2.3",
            parameter_schema={"policy_name": {"type": "string"}},
            validation_rules={"policy_name": {"required": True}},
            implementation_path="/drivers/rps_v1.py",
            metadata={"author": "test"}
        )

        assert version.version == "1.2.3"
        assert version.implementation_path == "/drivers/rps_v1.py"

    def test_policy_driver_version_invalid_semver(self):
        """Test policy driver version with invalid semantic version."""
        with pytest.raises(ValueError, match="Version must be in semantic version format"):
            PolicyDriverVersion(
                version="invalid.version",
                implementation_path="/test"
            )

    def test_policy_driver_version_schema_mismatch(self):
        """Test policy driver version with mismatched schema and validation rules."""
        with pytest.raises(ValueError, match="Parameter schema and validation rules must have matching parameters"):
            PolicyDriverVersion(
                version="1.0.0",
                parameter_schema={"param1": {"type": "string"}},
                validation_rules={"param1": {"required": True}, "param2": {"required": False}},
                implementation_path="/test"
            )

    def test_policy_driver_test_case_valid(self):
        """Test valid policy driver test case."""
        test_case = PolicyDriverTestCase(
            name="test_basic_policy",
            description="Basic policy validation test",
            input_payload={"policy_name": "rps_50_percent", "start_year": 2025},
            expected_output={"valid": True},
            is_valid=True
        )

        assert test_case.name == "test_basic_policy"
        assert test_case.is_valid is True
        assert test_case.input_payload["policy_name"] == "rps_50_percent"

    def test_policy_driver_test_case_invalid_name(self):
        """Test policy driver test case with invalid name."""
        with pytest.raises(ValueError, match="Test case name cannot be empty"):
            PolicyDriverTestCase(
                name="",
                input_payload={},
                is_valid=True
            )

    def test_policy_driver_valid(self):
        """Test valid policy driver."""
        driver = PolicyDriver(
            name="rps_california",
            driver_type=DriverType.POLICY,
            description="California RPS policy driver",
            version="2.1.0",
            status="active",
            parameter_schema={
                "policy_name": {"type": "string", "description": "Policy name"},
                "start_year": {"type": "number", "description": "Start year"},
                "target_percentage": {"type": "number", "description": "Target percentage"}
            },
            validation_rules={
                "policy_name": {"required": True, "min_length": 3},
                "start_year": {"required": True, "min": 2020, "max": 2050},
                "target_percentage": {"required": True, "min": 0, "max": 100}
            },
            implementation_path="/drivers/rps_california_v2.py",
            metadata={"region": "CA", "policy_type": "renewable_portfolio_standard"},
            parameters=[
                PolicyDriverParameter(
                    name="policy_name",
                    parameter_type="string",
                    required=True,
                    description="Name of the RPS policy"
                ),
                PolicyDriverParameter(
                    name="start_year",
                    parameter_type="number",
                    required=True,
                    description="Year when policy takes effect"
                ),
                PolicyDriverParameter(
                    name="target_percentage",
                    parameter_type="number",
                    required=False,
                    default_value=50.0,
                    description="Target renewable percentage"
                )
            ],
            versions=[
                PolicyDriverVersion(
                    version="1.0.0",
                    parameter_schema={"policy_name": {"type": "string"}},
                    implementation_path="/drivers/rps_california_v1.py"
                )
            ],
            test_cases=[
                PolicyDriverTestCase(
                    name="basic_rps_test",
                    description="Test basic RPS policy",
                    input_payload={"policy_name": "rps_50_percent", "start_year": 2025},
                    is_valid=True
                )
            ]
        )

        assert driver.name == "rps_california"
        assert driver.driver_type == DriverType.POLICY
        assert len(driver.parameters) == 3
        assert len(driver.versions) == 1
        assert len(driver.test_cases) == 1

    def test_policy_driver_invalid_type(self):
        """Test policy driver with invalid driver type."""
        with pytest.raises(ValueError, match="Only policy drivers are supported"):
            PolicyDriver(
                name="test_driver",
                driver_type=DriverType.LOAD_GROWTH  # Not policy type
            )

    def test_policy_driver_schema_mismatch(self):
        """Test policy driver with mismatched parameter schema."""
        with pytest.raises(ValueError, match="Parameter schema must match parameter definitions"):
            PolicyDriver(
                name="test_driver",
                driver_type=DriverType.POLICY,
                parameter_schema={"param1": {"type": "string"}},
                parameters=[
                    PolicyDriverParameter(name="param2", parameter_type="string")  # Different param
                ]
            )

    def test_policy_driver_version_mismatch(self):
        """Test policy driver with mismatched current version schema."""
        with pytest.raises(ValueError, match="Current version schema must match main parameter schema"):
            PolicyDriver(
                name="test_driver",
                driver_type=DriverType.POLICY,
                version="1.0.0",
                parameter_schema={"param1": {"type": "string"}},
                versions=[
                    PolicyDriverVersion(
                        version="1.0.0",  # Current version
                        parameter_schema={"param2": {"type": "string"}}  # Different schema
                    )
                ]
            )

    def test_model_serialization(self):
        """Test model JSON serialization/deserialization."""
        param = PolicyDriverParameter(
            name="test_param",
            parameter_type="string",
            required=True
        )

        # Should serialize to JSON
        json_str = param.json()
        assert isinstance(json_str, str)

        # Should deserialize from JSON
        parsed = PolicyDriverParameter.parse_raw(json_str)
        assert parsed.name == param.name
        assert parsed.parameter_type == param.parameter_type


class TestPolicyDriverRegistry:
    """Test policy driver registry operations."""

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
    def registry(self, mock_pool):
        """Create PolicyDriverRegistry instance with mocked pool."""
        registry = PolicyDriverRegistry("postgresql://test")
        registry.pool = mock_pool
        return registry

    @pytest.fixture
    def sample_policy_driver(self):
        """Sample policy driver for testing."""
        return PolicyDriver(
            name="rps_california",
            driver_type=DriverType.POLICY,
            description="California RPS policy driver",
            version="2.1.0",
            status="active",
            parameter_schema={
                "policy_name": {"type": "string"},
                "start_year": {"type": "number"},
                "target_percentage": {"type": "number"}
            },
            validation_rules={
                "policy_name": {"required": True},
                "start_year": {"required": True},
                "target_percentage": {"required": True}
            },
            implementation_path="/drivers/rps_california_v2.py",
            parameters=[
                PolicyDriverParameter(
                    name="policy_name",
                    parameter_type="string",
                    required=True
                ),
                PolicyDriverParameter(
                    name="start_year",
                    parameter_type="number",
                    required=True
                ),
                PolicyDriverParameter(
                    name="target_percentage",
                    parameter_type="number",
                    required=True,
                    default_value=50.0
                )
            ],
            test_cases=[
                PolicyDriverTestCase(
                    name="basic_test",
                    input_payload={"policy_name": "rps_50", "start_year": 2025, "target_percentage": 50.0},
                    is_valid=True
                )
            ]
        )

    async def test_register_driver_success(self, registry, mock_pool, sample_policy_driver):
        """Test successful policy driver registration."""
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value

        # Mock database responses
        mock_conn.execute.return_value = None

        result = await registry.register_driver(sample_policy_driver, "test-user")

        assert result is not None
        assert result.id is not None
        assert result.name == sample_policy_driver.name
        assert mock_conn.execute.call_count >= 3  # Main record + parameters + test cases

    async def test_get_driver_found(self, registry, mock_pool):
        """Test getting existing policy driver."""
        driver_id = "test-driver-id"

        # Mock database responses
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {
            "id": driver_id,
            "name": "test_driver",
            "type": "policy",
            "description": "Test policy driver",
            "version": "1.0.0",
            "status": "active",
            "parameter_schema": '{"policy_name": {"type": "string"}}',
            "validation_rules": '{"policy_name": {"required": True}}',
            "implementation_path": "/drivers/test.py",
            "metadata": '{"region": "CA"}',
            "created_by": "test-user",
            "created_at": datetime.utcnow(),
            "updated_by": "test-user",
            "updated_at": datetime.utcnow()
        }

        mock_conn.fetch.return_value = [
            {
                "parameter_name": "policy_name",
                "parameter_type": "string",
                "required": True,
                "default_value": None,
                "validation_rules": "{}",
                "description": "Policy name",
                "order_index": 1
            }
        ]

        mock_conn.fetch.side_effect = [
            [{"parameter_name": "policy_name", "parameter_type": "string", "required": True, "default_value": None, "validation_rules": "{}", "description": "Policy name", "order_index": 1}],
            [],  # No versions
            []   # No test cases
        ]

        result = await registry.get_driver(driver_id)

        assert result is not None
        assert result.id == driver_id
        assert result.name == "test_driver"
        assert len(result.parameters) == 1
        assert result.parameters[0].name == "policy_name"

    async def test_get_driver_not_found(self, registry, mock_pool):
        """Test getting non-existent policy driver."""
        driver_id = "non-existent-id"

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = None

        result = await registry.get_driver(driver_id)

        assert result is None

    async def test_list_drivers_with_filters(self, registry, mock_pool):
        """Test listing policy drivers with status filter."""
        # Mock database responses
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {"total": 2}
        mock_conn.fetch.return_value = [
            {
                "id": "driver-1",
                "name": "driver1",
                "description": "Driver 1",
                "version": "1.0.0",
                "status": "active",
                "parameter_schema": "{}",
                "validation_rules": "{}",
                "implementation_path": "/drivers/driver1.py",
                "metadata": "{}",
                "created_by": "user1",
                "created_at": datetime.utcnow(),
                "updated_by": "user1",
                "updated_at": datetime.utcnow()
            },
            {
                "id": "driver-2",
                "name": "driver2",
                "description": "Driver 2",
                "version": "2.0.0",
                "status": "active",
                "parameter_schema": "{}",
                "validation_rules": "{}",
                "implementation_path": "/drivers/driver2.py",
                "metadata": "{}",
                "created_by": "user2",
                "created_at": datetime.utcnow(),
                "updated_by": "user2",
                "updated_at": datetime.utcnow()
            }
        ]

        drivers, total = await registry.list_drivers(status="active", limit=10, offset=0)

        assert len(drivers) == 2
        assert total == 2
        assert all(d.status == "active" for d in drivers)

    async def test_update_driver(self, registry, mock_pool):
        """Test updating policy driver."""
        driver_id = "test-driver-id"
        updates = {
            "name": "updated_driver",
            "status": "inactive",
            "parameter_schema": {"new_param": {"type": "string"}}
        }

        # Mock database responses
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = None

        # Mock get_driver to return updated driver
        registry.get_driver = AsyncMock(return_value=PolicyDriver(
            id=driver_id,
            name=updates["name"],
            driver_type=DriverType.POLICY,
            status=updates["status"],
            parameter_schema=updates["parameter_schema"]
        ))

        result = await registry.update_driver(driver_id, updates, "test-user")

        assert result is not None
        assert result.name == updates["name"]
        assert result.status == updates["status"]
        mock_conn.execute.assert_called_once()

    async def test_create_driver_version(self, registry, mock_pool):
        """Test creating new driver version."""
        driver_id = "test-driver-id"
        version = PolicyDriverVersion(
            version="1.1.0",
            parameter_schema={"policy_name": {"type": "string"}},
            validation_rules={"policy_name": {"required": True}},
            implementation_path="/drivers/test_v1.1.py",
            metadata={"notes": "Updated version"}
        )

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.execute.return_value = None

        result = await registry.create_driver_version(driver_id, version, "test-user")

        assert result is not None
        assert result.version == version.version
        mock_conn.execute.assert_called_once()

    async def test_validate_driver_parameters_valid(self, registry, mock_pool):
        """Test validating valid driver parameters."""
        driver_id = "test-driver-id"
        valid_payload = {"policy_name": "rps_50", "start_year": 2025}

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchval.return_value = True

        is_valid, error = await registry.validate_driver_parameters(driver_id, valid_payload)

        assert is_valid is True
        assert error is None

    async def test_validate_driver_parameters_invalid(self, registry, mock_pool):
        """Test validating invalid driver parameters."""
        driver_id = "test-driver-id"
        invalid_payload = {"invalid_param": "value"}

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchval.return_value = False

        is_valid, error = await registry.validate_driver_parameters(driver_id, invalid_payload)

        assert is_valid is False
        assert error is not None

    async def test_run_driver_test_cases(self, registry, mock_pool):
        """Test running driver test cases."""
        driver_id = "test-driver-id"

        # Mock database responses
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch.return_value = [
            {
                "name": "test1",
                "description": "Test 1",
                "input_payload": '{"policy_name": "rps_50"}',
                "expected_output": '{"valid": True}',
                "is_valid": True
            }
        ]

        # Mock validation to return True
        registry.validate_driver_parameters = AsyncMock(return_value=(True, None))

        results = await registry.run_driver_test_cases(driver_id)

        assert results["total_tests"] == 1
        assert results["passed_tests"] == 1
        assert results["failed_tests"] == 0
        assert len(results["test_results"]) == 1

    async def test_get_driver_version_history(self, registry, mock_pool):
        """Test getting driver version history."""
        driver_id = "test-driver-id"

        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetch.return_value = [
            {
                "version": "1.1.0",
                "parameter_schema": '{"policy_name": {"type": "string"}}',
                "validation_rules": '{"policy_name": {"required": True}}',
                "implementation_path": "/drivers/v1.1.py",
                "metadata": '{"notes": "Updated"}',
                "created_by": "test-user",
                "created_at": datetime.utcnow()
            }
        ]

        history = await registry.get_driver_version_history(driver_id)

        assert len(history) == 1
        assert history[0]["version"] == "1.1.0"
        assert history[0]["created_by"] == "test-user"

    async def test_search_drivers(self, registry, mock_pool):
        """Test searching policy drivers."""
        query = "california"

        # Mock database responses
        mock_conn = mock_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.return_value = {"total": 1}
        mock_conn.fetch.return_value = [
            {
                "id": "driver-1",
                "name": "rps_california",
                "description": "California RPS policy",
                "version": "2.0.0",
                "status": "active",
                "parameter_schema": "{}",
                "validation_rules": "{}",
                "implementation_path": "/drivers/rps_ca.py",
                "metadata": "{}",
                "created_by": "test-user",
                "created_at": datetime.utcnow(),
                "updated_by": "test-user",
                "updated_at": datetime.utcnow()
            }
        ]

        drivers, total = await registry.search_drivers(query, limit=10, offset=0)

        assert len(drivers) == 1
        assert total == 1
        assert "california" in drivers[0].name.lower()

    async def test_get_popular_drivers(self, registry, mock_pool):
        """Test getting popular policy drivers."""
        # Mock list_drivers
        registry.list_drivers = AsyncMock(return_value=(
            [PolicyDriver(name="driver1", driver_type=DriverType.POLICY)],
            1
        ))

        drivers = await registry.get_popular_drivers(limit=5)

        assert len(drivers) == 1
        assert drivers[0].name == "driver1"

    async def test_registry_not_initialized_error(self):
        """Test error when registry is not initialized."""
        registry = PolicyDriverRegistry("postgresql://test")

        with pytest.raises(RuntimeError, match="PolicyDriverRegistry not initialized"):
            async with registry.get_connection():
                pass

    async def test_pool_initialization(self, mock_pool):
        """Test pool initialization."""
        registry = PolicyDriverRegistry("postgresql://test")

        with patch("aurum.scenarios.policy_registry.asyncpg.create_pool", return_value=mock_pool):
            await registry.initialize()

        assert registry.pool == mock_pool
        mock_pool.assert_called_once()

    async def test_pool_close(self, mock_pool):
        """Test pool closing."""
        registry = PolicyDriverRegistry("postgresql://test")
        registry.pool = mock_pool

        await registry.close()

        mock_pool.close.assert_called_once()
        assert registry.pool is None


class TestPolicyDriverRegistryIntegration:
    """Integration tests for PolicyDriverRegistry."""

    def test_driver_lifecycle_integration(self):
        """Test complete policy driver lifecycle."""
        # This would test the full flow of:
        # 1. Register driver with parameters and test cases
        # 2. Create driver version
        # 3. Update driver
        # 4. Validate parameters
        # 5. Run test cases
        # 6. Search and list drivers
        pass  # Placeholder for integration tests

    def test_parameter_validation_integration(self):
        """Test parameter validation across different driver types."""
        # This would test:
        # 1. Schema validation for different parameter types
        # 2. Custom validation rules
        # 3. Required vs optional parameters
        # 4. Default value handling
        pass  # Placeholder for integration tests

    def test_version_management_integration(self):
        """Test version management and compatibility."""
        # This would test:
        # 1. Version creation and validation
        # 2. Version history tracking
        # 3. Backward compatibility checks
        # 4. Version promotion workflow
        pass  # Placeholder for integration tests

    def test_test_case_execution_integration(self):
        """Test test case execution and validation."""
        # This would test:
        # 1. Test case creation and management
        # 2. Test case execution against drivers
        # 3. Test result reporting
        # 4. Test case validation
        pass  # Placeholder for integration tests

    def test_search_and_discovery_integration(self):
        """Test driver search and discovery features."""
        # This would test:
        # 1. Full-text search across drivers
        # 2. Tag-based filtering
        # 3. Metadata-based discovery
        # 4. Usage statistics and popularity
        pass  # Placeholder for integration tests

    def test_error_handling_integration(self):
        """Test error handling in registry operations."""
        # This would test:
        # 1. Database connection failures
        # 2. Schema validation errors
        # 3. Constraint violations
        # 4. Rollback scenarios
        pass  # Placeholder for integration tests

    def test_performance_characteristics(self):
        """Test performance characteristics of registry."""
        # This would test:
        # 1. Driver registration performance
        # 2. Parameter validation speed
        # 3. Search query performance
        # 4. Version history loading
        pass  # Placeholder for integration tests

    def test_concurrent_operations(self):
        """Test concurrent registry operations."""
        # This would test:
        # 1. Concurrent driver registration
        # 2. Concurrent version creation
        # 3. Concurrent parameter validation
        # 4. Transaction isolation
        pass  # Placeholder for integration tests

    def test_data_integrity(self):
        """Test data integrity across registry operations."""
        # This would test:
        # 1. Schema consistency checks
        # 2. Version compatibility validation
        # 3. Parameter definition integrity
        # 4. Test case consistency
        pass  # Placeholder for integration tests
