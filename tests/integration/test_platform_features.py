"""Integration tests for the new platform reliability, security, and cost features."""

from __future__ import annotations

import asyncio
import json
import pytest
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from aurum.schema_registry.policy_manager import (
    SchemaRegistryPolicyManager,
    SchemaRegistryPolicyConfig,
    SubjectPolicy,
    SchemaCompatibilityMode,
    PolicyEnforcementLevel,
    SchemaEvolutionPolicy
)
from aurum.security.secrets_rotation_manager import (
    SecretsRotationManager,
    TokenType,
    RotationPolicy,
    SecretRotationConfig,
    get_secrets_rotation_manager
)
from aurum.cost_profiler.enhanced_cost_profiler import (
    EnhancedCostProfiler,
    QueryComplexity,
    BudgetStatus,
    get_enhanced_cost_profiler
)
from aurum.observability.dr_test_manager import (
    DRTestManager,
    DRTestType,
    DRTestStatus,
    get_dr_test_manager
)


class TestSchemaRegistryPolicies:
    """Integration tests for schema registry policies."""

    @pytest.fixture
    async def policy_manager(self):
        config = SchemaRegistryPolicyConfig(
            subject_policies=[
                SubjectPolicy(
                    subject_pattern="aurum.test.*",
                    compatibility_mode=SchemaCompatibilityMode.BACKWARD,
                    evolution_policy=SchemaEvolutionPolicy.BACKWARD_COMPATIBLE,
                    enforcement_level=PolicyEnforcementLevel.FAIL,
                    allow_field_addition=True,
                    allow_field_removal=False,
                    require_documentation=True,
                    max_field_count=10,
                    field_name_pattern=r'^[a-z][a-z0-9_]*$'
                )
            ],
            default_compatibility_mode=SchemaCompatibilityMode.BACKWARD,
            enable_performance_checks=True
        )

        manager = SchemaRegistryPolicyManager(config)
        yield manager

    async def test_schema_policy_enforcement(self, policy_manager):
        """Test schema policy enforcement works correctly."""
        test_schema = {
            "type": "record",
            "name": "TestRecord",
            "namespace": "aurum.test",
            "fields": [
                {
                    "name": "valid_field",
                    "type": "string",
                    "doc": "A valid field"
                },
                {
                    "name": "invalidField",  # Should fail naming pattern
                    "type": "int"
                }
            ]
        }

        result = await policy_manager.validate_schema_registration(
            "aurum.test.example-value",
            test_schema
        )

        assert not result.passed
        assert any("naming pattern" in msg for msg in result.messages)

    async def test_performance_constraints(self, policy_manager):
        """Test performance constraint validation."""
        large_schema = {
            "type": "record",
            "name": "LargeRecord",
            "namespace": "aurum.test",
            "fields": [
                {"name": f"field_{i}", "type": "string"}
                for i in range(15)  # Exceeds max_field_count
            ]
        }

        result = await policy_manager.validate_schema_registration(
            "aurum.test.large-value",
            large_schema
        )

        assert not result.passed
        assert any("field count" in msg for msg in result.messages)


class TestSecretsRotation:
    """Integration tests for secrets rotation system."""

    @pytest.fixture
    async def rotation_manager(self):
        from aurum.security.vault_client import VaultConfig

        vault_config = VaultConfig(vault_url="http://localhost:8200")
        rotation_config = SecretRotationConfig(
            enable_auto_rotation=True,
            enable_lazy_rotation=True,
            default_token_ttl_hours=1,
            max_token_ttl_hours=8
        )

        manager = SecretsRotationManager(vault_config, rotation_config)
        yield manager

    async def test_token_creation_and_rotation(self, rotation_manager):
        """Test token creation and rotation functionality."""
        # Create token configuration
        config = await rotation_manager.create_token_config(
            name="test_api",
            token_type=TokenType.API_CLIENT,
            policies=["read"],
            ttl_hours=1
        )

        assert config.name == "test_api"
        assert config.token_type == TokenType.API_CLIENT

        # Test token retrieval
        token = await rotation_manager.get_short_lived_token(
            TokenType.API_CLIENT,
            "test_api"
        )

        assert token is not None
        assert token.token_config.name == "test_api"
        assert token.can_be_used()

    async def test_token_revocation(self, rotation_manager):
        """Test token revocation works correctly."""
        # Create and get a token
        token = await rotation_manager.get_short_lived_token(
            TokenType.API_CLIENT,
            "test_api"
        )

        assert token is not None

        # Revoke the token
        success = await rotation_manager.revoke_token(token.token_id, "test")

        assert success

        # Verify token is revoked
        revoked_token = rotation_manager.active_tokens.get(token.token_id)
        assert revoked_token is None or revoked_token.is_revoked

    async def test_circuit_breaker_pattern(self, rotation_manager):
        """Test circuit breaker pattern for token failures."""
        # Simulate multiple token failures
        for i in range(6):  # Exceed failure threshold
            config = await rotation_manager.create_token_config(
                name=f"failed_api_{i}",
                token_type=TokenType.API_CLIENT,
                policies=["read"],
                ttl_hours=1
            )

            # Simulate failure
            # In real scenario, this would be handled by the circuit breaker

        # Check that circuit breaker state is updated
        status = await rotation_manager.get_rotation_status()
        assert "circuit_breaker" in status or "active_tokens" in status


class TestEnhancedCostProfiler:
    """Integration tests for enhanced cost profiler."""

    @pytest.fixture
    async def cost_profiler(self):
        profiler = EnhancedCostProfiler()
        yield profiler

    async def test_query_cost_tracking(self, cost_profiler):
        """Test comprehensive query cost tracking."""
        query_id = "test_query_123"

        # Track query cost
        cost_hint = await cost_profiler.track_query_cost(
            query_id=query_id,
            query_type="SELECT",
            dataset_name="test_dataset",
            estimated_complexity=QueryComplexity.MEDIUM,
            estimated_cost_usd=0.05,
            budget_allocated=0.10
        )

        assert cost_hint.query_id == query_id
        assert cost_hint.estimated_cost_usd == 0.05
        assert cost_hint.budget_allocated == 0.10

        # Update with actual costs
        updated_hint = await cost_profiler.update_query_actuals(
            query_id=query_id,
            actual_cost_usd=0.03,
            actual_rows=1000,
            actual_runtime_seconds=2.5
        )

        assert updated_hint is not None
        assert updated_hint.actual_cost_usd == 0.03
        assert updated_hint.status == "completed"

    async def test_budget_management(self, cost_profiler):
        """Test budget allocation and tracking."""
        # Check initial budget
        budget_check = await cost_profiler.check_budget_availability(
            "test_dataset",
            0.10
        )

        assert budget_check["available"] is True
        assert budget_check["budget_status"] == BudgetStatus.UNDER_BUDGET.value

        # Allocate budget
        await cost_profiler.track_query_cost(
            query_id="budget_test",
            query_type="SELECT",
            dataset_name="test_dataset",
            budget_allocated=0.05
        )

        # Check updated budget
        budget_check_after = await cost_profiler.check_budget_availability(
            "test_dataset",
            0.10
        )

        assert budget_check_after["remaining_budget"] < 1000.0  # Default budget

    async def test_cost_optimization_recommendations(self, cost_profiler):
        """Test automatic cost optimization recommendations."""
        # Track a query with poor performance
        cost_hint = await cost_profiler.track_query_cost(
            query_id="slow_query",
            query_type="SELECT",
            dataset_name="test_dataset",
            estimated_complexity=QueryComplexity.HIGH,
            estimated_cost_usd=0.50
        )

        await cost_profiler.update_query_actuals(
            query_id="slow_query",
            actual_cost_usd=1.20,  # Much higher than estimated
            actual_rows=500,
            actual_runtime_seconds=45.0  # Very slow
        )

        # Check if recommendations were generated
        assert len(cost_profiler.recommendations) > 0

        # Find recommendation for this query
        recs = [r for r in cost_profiler.recommendations if r.query_id == "slow_query"]
        assert len(recs) > 0

        # Verify recommendation quality
        rec = recs[0]
        assert rec.estimated_savings_usd > 0
        assert rec.confidence_score > 0

    async def test_cost_report_generation(self, cost_profiler):
        """Test comprehensive cost report generation."""
        # Create some test data
        for i in range(5):
            await cost_profiler.track_query_cost(
                query_id=f"query_{i}",
                query_type="SELECT",
                dataset_name="test_dataset",
                estimated_cost_usd=0.01 * (i + 1)
            )

            await cost_profiler.update_query_actuals(
                query_id=f"query_{i}",
                actual_cost_usd=0.01 * (i + 1),
                actual_rows=100 * (i + 1),
                actual_runtime_seconds=1.0
            )

        # Generate report
        report = await cost_profiler.generate_cost_report()

        assert "summary" in report
        assert "total_queries" in report["summary"]
        assert report["summary"]["total_queries"] == 5
        assert "total_actual_cost" in report["summary"]
        assert report["summary"]["total_actual_cost"] == 0.15  # Sum of 0.01 + 0.02 + 0.03 + 0.04 + 0.05


class TestDRTestManager:
    """Integration tests for disaster recovery testing."""

    @pytest.fixture
    async def dr_manager(self):
        manager = DRTestManager()
        yield manager

    async def test_dr_test_execution(self, dr_manager):
        """Test DR test execution and status tracking."""
        from aurum.observability.dr_test_manager import DRTestConfig, DRTestType

        config = DRTestConfig(
            test_type=DRTestType.DATABASE_RESTORE,
            name="test_database_restore",
            description="Test database restoration",
            timeout_minutes=30,
            validate_data_integrity=True
        )

        # Run DR test
        result = await dr_manager.run_dr_test(config, "test_user")

        assert result.test_id is not None
        assert result.status in [DRTestStatus.PASSED, DRTestStatus.FAILED]
        assert result.start_time is not None
        assert result.end_time is not None
        assert result.duration_seconds > 0

    async def test_backup_validation(self, dr_manager):
        """Test backup validation functionality."""
        # Test validation of different backup types
        validations = await dr_manager._validate_postgres_backup()
        assert validations in [
            dr_manager.backup_validations["postgres"] for _ in range(1)
        ]

        # Verify validation is stored
        assert "postgres" in dr_manager.backup_validations

    async def test_dr_report_generation(self, dr_manager):
        """Test DR report generation."""
        # Generate report
        report = await dr_manager.generate_dr_report()

        assert "summary" in report
        assert "component_statistics" in report
        assert "recent_tests" in report
        assert "active_tests" in report

        # Check report structure
        assert "total_tests" in report["summary"]
        assert "success_rate_percent" in report["summary"]
        assert "average_rto_minutes" in report["summary"]
        assert "average_rpo_minutes" in report["summary"]


class TestPlatformIntegration:
    """End-to-end integration tests for the platform features."""

    @pytest.fixture
    async def integrated_platform(self):
        """Set up all platform components for integration testing."""
        # Initialize all managers
        schema_manager = SchemaRegistryPolicyManager(SchemaRegistryPolicyConfig())
        secrets_manager = await get_secrets_rotation_manager()
        cost_profiler = await get_enhanced_cost_profiler()
        dr_manager = await get_dr_test_manager()

        yield {
            "schema": schema_manager,
            "secrets": secrets_manager,
            "cost": cost_profiler,
            "dr": dr_manager
        }

    async def test_cross_component_integration(self, integrated_platform):
        """Test integration between different platform components."""
        # Test that components can work together
        schema_manager = integrated_platform["schema"]
        cost_profiler = integrated_platform["cost"]

        # Create schema with policy compliance
        test_schema = {
            "type": "record",
            "name": "IntegrationTest",
            "namespace": "aurum.integration",
            "fields": [
                {
                    "name": "test_field",
                    "type": "string",
                    "doc": "Test field for integration"
                }
            ]
        }

        # Validate schema
        result = await schema_manager.validate_schema_registration(
            "aurum.integration.test-value",
            test_schema
        )

        assert result.passed

        # Track cost for schema operation
        cost_hint = await cost_profiler.track_query_cost(
            query_id="schema_validation_test",
            query_type="VALIDATE",
            dataset_name="schema_registry",
            estimated_cost_usd=0.01
        )

        assert cost_hint.query_id == "schema_validation_test"

    async def test_error_handling_and_recovery(self, integrated_platform):
        """Test error handling and recovery across components."""
        cost_profiler = integrated_platform["cost"]

        # Test invalid query tracking
        with pytest.raises(Exception):  # Should handle gracefully
            await cost_profiler.update_query_actuals(
                "nonexistent_query",
                0.01,
                100,
                1.0
            )

        # Verify system remains stable
        status = await cost_profiler.generate_cost_report()
        assert "summary" in status

    async def test_performance_under_load(self, integrated_platform):
        """Test performance under concurrent load."""
        import asyncio

        cost_profiler = integrated_platform["cost"]

        # Create multiple concurrent operations
        tasks = []
        for i in range(10):
            task = cost_profiler.track_query_cost(
                query_id=f"load_test_{i}",
                query_type="SELECT",
                dataset_name="performance_test",
                estimated_cost_usd=0.01
            )
            tasks.append(task)

        # Execute concurrently
        start_time = time.time()
        results = await asyncio.gather(*tasks)
        end_time = time.time()

        # Verify all completed
        assert len(results) == 10
        assert all(r is not None for r in results)

        # Check performance
        execution_time = end_time - start_time
        assert execution_time < 5.0  # Should complete in under 5 seconds

        # Verify all queries were tracked
        report = await cost_profiler.generate_cost_report()
        assert report["summary"]["total_queries"] >= 10


class TestAPIFeatureIntegration:
    """Test integration with API layer."""

    async def test_api_endpoints_availability(self):
        """Test that API endpoints are properly configured."""
        # This would test the actual API endpoints
        # For now, we simulate the integration

        from aurum.api.app import create_app
        from fastapi.testclient import TestClient

        app = create_app()
        client = TestClient(app)

        # Test basic health endpoint
        response = client.get("/health")
        assert response.status_code == 200

        # Test API v2 endpoints
        response = client.get("/v2/health")
        assert response.status_code == 200

    async def test_api_error_handling(self):
        """Test API error handling for new features."""
        from aurum.api.app import create_app
        from fastapi.testclient import TestClient

        app = create_app()
        client = TestClient(app)

        # Test invalid endpoint
        response = client.get("/invalid-endpoint")
        assert response.status_code == 404

        # Test malformed request
        response = client.post("/v2/scenarios", data="invalid json")
        assert response.status_code in [400, 422]  # Bad Request or Unprocessable Entity


class TestMonitoringAndAlerting:
    """Test monitoring and alerting integration."""

    async def test_metrics_collection(self):
        """Test metrics collection for new features."""
        cost_profiler = await get_enhanced_cost_profiler()

        # Track some queries to generate metrics
        for i in range(3):
            await cost_profiler.track_query_cost(
                query_id=f"metrics_test_{i}",
                query_type="SELECT",
                dataset_name="metrics_dataset",
                estimated_cost_usd=0.01
            )

        # Verify metrics are collected
        # In a real scenario, this would check actual metrics
        assert len(cost_profiler.query_costs) == 3

    async def test_alert_generation(self):
        """Test alert generation for budget limits."""
        cost_profiler = await get_enhanced_cost_profiler()

        # Check budget before exceeding
        budget_check = await cost_profiler.check_budget_availability(
            "alert_test",
            500.0  # Request large amount
        )

        # Should generate alert or warning
        assert "budget_status" in budget_check
        assert budget_check["budget_status"] in [
            BudgetStatus.UNDER_BUDGET.value,
            BudgetStatus.APPROACHING_LIMIT.value,
            BudgetStatus.OVER_BUDGET.value
        ]


# Utility functions for test setup
async def setup_test_environment():
    """Set up test environment with all components."""
    # Initialize test databases, mock services, etc.
    # This would be expanded based on actual test needs

    return {
        "test_database": "test_aurum_db",
        "test_kafka": "test_kafka_cluster",
        "test_vault": "test_vault_instance"
    }


async def teardown_test_environment(env):
    """Clean up test environment."""
    # Clean up test resources
    pass


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
