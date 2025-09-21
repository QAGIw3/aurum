"""Tests for scenario output feature flags per tenant."""

from __future__ import annotations

import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import uuid4

from fastapi import HTTPException

from aurum.api.scenario_models import DriverType
from aurum.scenarios.feature_flags import (
    ScenarioOutputFeature,
    ScenarioOutputLimits,
    ScenarioOutputConfiguration,
    ScenarioOutputFeatureManager,
    require_scenario_output_feature,
    validate_scenario_output_request,
    check_scenario_output_feature,
    enforce_scenario_output_limits,
    get_scenario_output_configuration,
    initialize_scenario_output_feature_manager,
)


class TestScenarioOutputFeature:
    """Test ScenarioOutputFeature enum."""

    def test_feature_enum_values(self):
        """Test that all feature enum values are valid."""
        expected_features = {
            "scenario_outputs_enabled",
            "bulk_scenario_runs",
            "scenario_outputs_pagination",
            "scenario_outputs_filtering",
            "scenario_outputs_csv_export",
            "scenario_outputs_streaming",
            "scenario_outputs_real_time",
            "scenario_outputs_analytics",
            "scenario_outputs_dashboard",
            "scenario_outputs_api",
            "scenario_outputs_compression",
            "scenario_outputs_caching",
            "scenario_outputs_optimization",
            "scenario_outputs_audit_log",
            "scenario_outputs_data_retention",
            "scenario_outputs_pii_filtering",
            "scenario_outputs_webhooks",
            "scenario_outputs_integrations",
            "scenario_outputs_notifications"
        }

        actual_features = set(f.value for f in ScenarioOutputFeature)
        assert actual_features == expected_features

    def test_feature_enum_string_conversion(self):
        """Test feature enum string conversion."""
        feature = ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED
        assert str(feature) == "scenario_outputs_enabled"
        assert feature.value == "scenario_outputs_enabled"


class TestScenarioOutputLimits:
    """Test ScenarioOutputLimits model."""

    def test_valid_limits(self):
        """Test valid limits configuration."""
        limits = ScenarioOutputLimits(
            max_outputs_per_run=10000,
            max_outputs_per_request=1000,
            max_concurrent_requests=10,
            max_retention_days=90,
            max_file_size_mb=100
        )

        assert limits.max_outputs_per_run == 10000
        assert limits.max_outputs_per_request == 1000
        assert limits.max_concurrent_requests == 10
        assert limits.max_retention_days == 90
        assert limits.max_file_size_mb == 100

    def test_invalid_positive_int(self):
        """Test validation of positive integer fields."""
        with pytest.raises(ValueError, match="must be positive"):
            ScenarioOutputLimits(max_outputs_per_run=0)

        with pytest.raises(ValueError, match="must be positive"):
            ScenarioOutputLimits(max_outputs_per_request=-1)

    def test_invalid_retention_days(self):
        """Test validation of retention days."""
        with pytest.raises(ValueError, match="max_retention_days must be between 1 and 3650"):
            ScenarioOutputLimits(max_retention_days=0)

        with pytest.raises(ValueError, match="max_retention_days must be between 1 and 3650"):
            ScenarioOutputLimits(max_retention_days=4000)

    def test_invalid_file_size(self):
        """Test validation of file size."""
        with pytest.raises(ValueError, match="max_file_size_mb must be between 1 and 1000"):
            ScenarioOutputLimits(max_file_size_mb=0)

        with pytest.raises(ValueError, match="max_file_size_mb must be between 1 and 1000"):
            ScenarioOutputLimits(max_file_size_mb=1500)

    def test_model_serialization(self):
        """Test limits model JSON serialization."""
        limits = ScenarioOutputLimits()

        # Should serialize to JSON
        json_str = limits.json()
        assert isinstance(json_str, str)

        # Should deserialize from JSON
        parsed = ScenarioOutputLimits.parse_raw(json_str)
        assert parsed.max_outputs_per_run == limits.max_outputs_per_run


class TestScenarioOutputConfiguration:
    """Test ScenarioOutputConfiguration model."""

    def test_valid_configuration(self):
        """Test valid configuration."""
        config = ScenarioOutputConfiguration(
            limits=ScenarioOutputLimits(max_outputs_per_run=5000),
            enabled_features=["scenario_outputs_enabled", "scenario_outputs_pagination"],
            disabled_features=["scenario_outputs_real_time"],
            custom_settings={"cache_ttl": 300}
        )

        assert len(config.enabled_features) == 2
        assert len(config.disabled_features) == 1
        assert config.custom_settings["cache_ttl"] == 300

    def test_overlapping_features_validation(self):
        """Test validation of overlapping enabled/disabled features."""
        with pytest.raises(ValueError, match="Features cannot be both enabled and disabled"):
            ScenarioOutputConfiguration(
                enabled_features=["scenario_outputs_enabled"],
                disabled_features=["scenario_outputs_enabled"]
            )

    def test_model_serialization(self):
        """Test configuration model JSON serialization."""
        config = ScenarioOutputConfiguration()

        # Should serialize to JSON
        json_str = config.json()
        assert isinstance(json_str, str)

        # Should deserialize from JSON
        parsed = ScenarioOutputConfiguration.parse_raw(json_str)
        assert isinstance(parsed.limits, ScenarioOutputLimits)


class TestScenarioOutputFeatureManager:
    """Test ScenarioOutputFeatureManager operations."""

    @pytest.fixture
    def mock_store(self):
        """Mock scenario store."""
        store = AsyncMock()
        store.get_feature_flag.return_value = {
            "enabled": True,
            "configuration": {"limits": {"max_outputs_per_request": 100}},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        return store

    @pytest.fixture
    def manager(self, mock_store):
        """Create feature manager with mocked store."""
        return ScenarioOutputFeatureManager(mock_store)

    async def test_is_feature_enabled_found(self, manager, mock_store):
        """Test checking if feature is enabled when flag exists."""
        result = await manager.is_feature_enabled(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)

        assert result is True
        mock_store.get_feature_flag.assert_called_once()

    async def test_is_feature_enabled_not_found(self, manager, mock_store):
        """Test checking if feature is enabled when flag doesn't exist."""
        mock_store.get_feature_flag.return_value = None

        result = await manager.is_feature_enabled(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)

        assert result is True  # Default for core features

    async def test_is_feature_enabled_disabled(self, manager, mock_store):
        """Test checking if feature is enabled when flag is disabled."""
        mock_store.get_feature_flag.return_value = {
            "enabled": False,
            "configuration": {},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        result = await manager.is_feature_enabled(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)

        assert result is False

    async def test_set_feature_enabled(self, manager, mock_store):
        """Test setting feature enabled state."""
        mock_store.set_feature_flag.return_value = {
            "enabled": True,
            "configuration": {},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        result = await manager.set_feature_enabled(
            ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED,
            enabled=True
        )

        assert result is not None
        assert result["enabled"] is True
        mock_store.set_feature_flag.assert_called_once_with(
            feature_name="scenario_outputs_enabled",
            enabled=True,
            configuration=None
        )

    async def test_get_feature_configuration(self, manager, mock_store):
        """Test getting feature configuration."""
        config = {"limits": {"max_outputs_per_request": 200}}
        mock_store.get_feature_flag.return_value = {
            "enabled": True,
            "configuration": config,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        result = await manager.get_feature_configuration(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)

        assert result == config

    async def test_update_feature_configuration(self, manager, mock_store):
        """Test updating feature configuration."""
        new_config = {"limits": {"max_outputs_per_request": 300}}
        mock_store.set_feature_flag.return_value = {
            "enabled": True,
            "configuration": new_config,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        result = await manager.update_feature_configuration(
            ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED,
            new_config
        )

        assert result is not None
        assert result["configuration"] == new_config
        mock_store.set_feature_flag.assert_called_once_with(
            feature_name="scenario_outputs_enabled",
            enabled=True,
            configuration=new_config
        )

    async def test_set_multiple_features(self, manager, mock_store):
        """Test setting multiple features at once."""
        mock_store.set_feature_flag.return_value = {
            "enabled": True,
            "configuration": {},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        features = {
            ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED: True,
            ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION: False,
        }

        result = await manager.set_multiple_features(features)

        assert result is not None
        assert result["total"] == 2
        assert result["successful"] == 2
        assert result["failed"] == 0
        assert mock_store.set_feature_flag.call_count == 2

    async def test_get_all_features(self, manager, mock_store):
        """Test getting all features."""
        mock_store.get_feature_flag.side_effect = [
            {
                "enabled": True,
                "configuration": {"limits": {"max_outputs_per_request": 100}},
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            },
            {
                "enabled": False,
                "configuration": {},
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
        ]

        result = await manager.get_all_features()

        assert result is not None
        assert len(result) == len(ScenarioOutputFeature)
        # First feature should be enabled
        assert result[ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED.value]["enabled"] is True
        # Second feature should be disabled
        assert result[ScenarioOutputFeature.BULK_SCENARIO_RUNS.value]["enabled"] is False

    async def test_validate_scenario_output_request_pagination(self, manager, mock_store):
        """Test validating pagination request."""
        mock_store.get_feature_flag.return_value = {
            "enabled": True,
            "configuration": {"limits": {"max_outputs_per_request": 50}},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        request_data = {"limit": 100}  # Exceeds limit

        is_valid, error = await manager.validate_scenario_output_request(
            ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION,
            request_data
        )

        assert is_valid is False
        assert "exceeds maximum" in error

    async def test_validate_scenario_output_request_csv_export(self, manager, mock_store):
        """Test validating CSV export request."""
        mock_store.get_feature_flag.return_value = {
            "enabled": True,
            "configuration": {},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        request_data = {"format": "csv"}

        is_valid, error = await manager.validate_scenario_output_request(
            ScenarioOutputFeature.SCENARIO_OUTPUTS_CSV_EXPORT,
            request_data
        )

        assert is_valid is True
        assert error is None

    async def test_enforce_output_limits(self, manager, mock_store):
        """Test enforcing output limits."""
        mock_store.get_feature_flag.return_value = {
            "enabled": True,
            "configuration": {"limits": {"max_outputs_per_request": 5}},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        outputs = [{"id": i, "value": i} for i in range(10)]  # 10 outputs

        limited_outputs, error = await manager.enforce_output_limits(outputs)

        assert len(limited_outputs) == 5
        assert "limited to" in error

    def test_get_default_feature_state(self, manager):
        """Test getting default feature state."""
        # Core features should be enabled by default
        assert manager._get_default_feature_state(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED) is True
        assert manager._get_default_feature_state(ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION) is True
        assert manager._get_default_feature_state(ScenarioOutputFeature.SCENARIO_OUTPUTS_FILTERING) is True

        # Advanced features should be disabled by default
        assert manager._get_default_feature_state(ScenarioOutputFeature.SCENARIO_OUTPUTS_REAL_TIME) is False
        assert manager._get_default_feature_state(ScenarioOutputFeature.SCENARIO_OUTPUTS_ANALYTICS) is False

    async def test_get_feature_stats(self, manager, mock_store):
        """Test getting feature statistics."""
        mock_store.get_feature_flag.side_effect = [
            {
                "enabled": True,
                "configuration": {"limits": {"max_outputs_per_request": 100}},
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            },
            {
                "enabled": False,
                "configuration": {},
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            },
            {
                "enabled": True,
                "configuration": {},
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
        ]

        stats = await manager.get_feature_stats()

        assert stats is not None
        assert stats["total_features"] == 3
        assert stats["enabled_features"] == 2
        assert stats["disabled_features"] == 1
        assert stats["configured_features"] == 1
        assert stats["configuration_coverage"] == 1/3

    async def test_reset_to_defaults(self, manager, mock_store):
        """Test resetting features to defaults."""
        mock_store.set_feature_flag.return_value = {
            "enabled": True,
            "configuration": {},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        result = await manager.reset_to_defaults()

        assert result is not None
        assert result["total"] == len(ScenarioOutputFeature)
        assert result["successful"] == len(ScenarioOutputFeature)
        assert result["failed"] == 0

    async def test_enable_all_features(self, manager, mock_store):
        """Test enabling all features."""
        mock_store.set_feature_flag.return_value = {
            "enabled": True,
            "configuration": {},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        result = await manager.enable_all_features()

        assert result is not None
        assert result["total"] == len(ScenarioOutputFeature)
        assert result["successful"] == len(ScenarioOutputFeature)
        assert result["failed"] == 0

    async def test_disable_all_features(self, manager, mock_store):
        """Test disabling all features."""
        mock_store.set_feature_flag.return_value = {
            "enabled": False,
            "configuration": {},
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        result = await manager.disable_all_features()

        assert result is not None
        assert result["total"] == len(ScenarioOutputFeature)
        assert result["successful"] == len(ScenarioOutputFeature)
        assert result["failed"] == 0


class TestFeatureFlagDecorators:
    """Test feature flag decorators."""

    async def test_require_scenario_output_feature_decorator(self):
        """Test the require feature decorator."""
        with patch("aurum.scenarios.feature_flags.check_scenario_output_feature") as mock_check:
            mock_check.return_value = True

            @require_scenario_output_feature(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)
            async def test_function():
                return "success"

            result = await test_function()

            assert result == "success"
            mock_check.assert_called_once_with(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)

    async def test_require_scenario_output_feature_decorator_disabled(self):
        """Test the require feature decorator when feature is disabled."""
        with patch("aurum.scenarios.feature_flags.check_scenario_output_feature") as mock_check:
            mock_check.return_value = False

            @require_scenario_output_feature(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)
            async def test_function():
                return "success"

            with pytest.raises(Exception):  # Should raise ForbiddenException
                await test_function()

    async def test_validate_scenario_output_request_decorator(self):
        """Test the validate request decorator."""
        with patch("aurum.scenarios.feature_flags.enforce_scenario_output_limits") as mock_enforce:
            mock_enforce.return_value = (True, None)

            @validate_scenario_output_request(ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION)
            async def test_function():
                return "success"

            result = await test_function()

            assert result == "success"
            mock_enforce.assert_called_once()

    async def test_validate_scenario_output_request_decorator_invalid(self):
        """Test the validate request decorator when validation fails."""
        with patch("aurum.scenarios.feature_flags.enforce_scenario_output_limits") as mock_enforce:
            mock_enforce.return_value = (False, "Validation failed")

            @validate_scenario_output_request(ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION)
            async def test_function():
                return "success"

            with pytest.raises(Exception):  # Should raise ValidationException
                await test_function()


class TestFeatureFlagIntegration:
    """Integration tests for feature flags."""

    def test_feature_flag_enum_completeness(self):
        """Test that all expected features are defined."""
        expected_categories = [
            "Core scenario output functionality",
            "Advanced features",
            "Performance features",
            "Security and compliance",
            "Integration features"
        ]

        # Count features by category
        core_features = [f for f in ScenarioOutputFeature if f.value.startswith("scenario_outputs_") and not any(
            category in f.value for category in ["real_time", "analytics", "dashboard", "api", "compression", "caching", "optimization", "audit", "retention", "pii", "webhooks", "integrations", "notifications"]
        )]

        advanced_features = [f for f in ScenarioOutputFeature if any(
            category in f.value for category in ["real_time", "analytics", "dashboard", "api"]
        )]

        performance_features = [f for f in ScenarioOutputFeature if any(
            category in f.value for category in ["compression", "caching", "optimization"]
        )]

        security_features = [f for f in ScenarioOutputFeature if any(
            category in f.value for category in ["audit", "retention", "pii"]
        )]

        integration_features = [f for f in ScenarioOutputFeature if any(
            category in f.value for category in ["webhooks", "integrations", "notifications"]
        )]

        # Verify we have features in each category
        assert len(core_features) >= 4, "Should have at least 4 core features"
        assert len(advanced_features) >= 3, "Should have at least 3 advanced features"
        assert len(performance_features) >= 2, "Should have at least 2 performance features"
        assert len(security_features) >= 2, "Should have at least 2 security features"
        assert len(integration_features) >= 2, "Should have at least 2 integration features"

    def test_feature_flag_dependencies(self):
        """Test feature flag dependencies and relationships."""
        # Core features should be enabled by default
        core_features = {
            ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED,
            ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION,
            ScenarioOutputFeature.SCENARIO_OUTPUTS_FILTERING,
        }

        # Advanced features should be disabled by default
        advanced_features = {
            ScenarioOutputFeature.SCENARIO_OUTPUTS_REAL_TIME,
            ScenarioOutputFeature.SCENARIO_OUTPUTS_ANALYTICS,
            ScenarioOutputFeature.SCENARIO_OUTPUTS_DASHBOARD,
        }

        # Verify default states
        for feature in core_features:
            assert ScenarioOutputFeatureManager._get_default_feature_state(None, feature) is True

        for feature in advanced_features:
            assert ScenarioOutputFeatureManager._get_default_feature_state(None, feature) is False

    def test_feature_configuration_validation(self):
        """Test feature configuration validation."""
        # Test valid configuration
        config = ScenarioOutputConfiguration(
            limits=ScenarioOutputLimits(max_outputs_per_run=10000),
            enabled_features=["scenario_outputs_enabled"],
            disabled_features=["scenario_outputs_real_time"],
            custom_settings={"cache_enabled": True}
        )

        assert config.limits.max_outputs_per_run == 10000
        assert "scenario_outputs_enabled" in config.enabled_features
        assert "scenario_outputs_real_time" in config.disabled_features

        # Test overlapping features validation
        with pytest.raises(ValueError, match="Features cannot be both enabled and disabled"):
            ScenarioOutputConfiguration(
                enabled_features=["scenario_outputs_enabled"],
                disabled_features=["scenario_outputs_enabled"]
            )

    def test_limits_validation_scenarios(self):
        """Test various limits validation scenarios."""
        # Test reasonable limits
        limits = ScenarioOutputLimits(
            max_outputs_per_run=10000,
            max_outputs_per_request=1000,
            max_concurrent_requests=10,
            max_retention_days=365,
            max_file_size_mb=100
        )

        assert limits.max_outputs_per_run == 10000
        assert limits.max_outputs_per_request == 1000
        assert limits.max_concurrent_requests == 10
        assert limits.max_retention_days == 365
        assert limits.max_file_size_mb == 100

        # Test edge cases
        edge_limits = ScenarioOutputLimits(
            max_outputs_per_run=1,
            max_outputs_per_request=1,
            max_concurrent_requests=1,
            max_retention_days=1,
            max_file_size_mb=1
        )

        assert edge_limits.max_outputs_per_run == 1
        assert edge_limits.max_outputs_per_request == 1
        assert edge_limits.max_concurrent_requests == 1
        assert edge_limits.max_retention_days == 1
        assert edge_limits.max_file_size_mb == 1

    def test_feature_flag_error_handling(self):
        """Test error handling in feature flag operations."""
        # This would test various error scenarios:
        # 1. Database connection failures
        # 2. Invalid configuration
        # 3. Permission issues
        # 4. Network timeouts
        pass  # Placeholder for error handling tests

    def test_feature_flag_performance(self):
        """Test performance characteristics of feature flag system."""
        # This would test:
        # 1. Caching effectiveness
        # 2. Database query performance
        # 3. Memory usage with many features
        # 4. Concurrent access patterns
        pass  # Placeholder for performance tests

    def test_feature_flag_security(self):
        """Test security aspects of feature flags."""
        # This would test:
        # 1. Tenant isolation
        # 2. Authorization checks
        # 3. Input sanitization
        # 4. Audit logging
        pass  # Placeholder for security tests
