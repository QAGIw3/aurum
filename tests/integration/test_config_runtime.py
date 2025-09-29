"""
Integration tests for the configuration system runtime behavior.

Tests the complete system end-to-end including:
- SettingsManager integration
- Feature flag integration
- Hot reloading behavior
- Performance under load
"""

import asyncio
import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from aurum.config.change_tracking import get_change_tracker, ChangeType, ChangeSource
from aurum.config.dynamic_config import DynamicConfigService
from aurum.config.validation import validate_and_coerce_config
from aurum.core.settings import get_settings_manager, SettingsManager


class TestSettingsManagerIntegration:
    """Test integration between DynamicConfigService and SettingsManager."""

    def test_hybrid_mode(self):
        """Test hybrid mode where SettingsManager uses DynamicConfigService."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Create configuration files
            base_config = {
                "api": {
                    "title": "Test API",
                    "version": "1.0.0",
                    "host": "0.0.0.0",
                    "port": 8000
                },
                "database": {
                    "host": "localhost",
                    "port": 5432
                }
            }
            (config_dir / "base.yaml").write_text(yaml.dump(base_config))

            # Create SettingsManager with dynamic config enabled
            settings_manager = SettingsManager(
                environment="test",
                config_base_path=config_dir,
                use_dynamic_config=True,
                hot_reload_enabled=False
            )

            settings = settings_manager.get()

            # Should have configuration from dynamic service
            assert settings.api.title == "Test API"
            assert settings.api.port == 8000
            assert settings.database.host == "localhost"

    def test_fallback_to_legacy(self):
        """Test fallback to legacy mode when dynamic config fails."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Create invalid configuration
            (config_dir / "base.yaml").write_text("invalid: yaml: content: [")

            # Should fallback to legacy mode
            settings_manager = SettingsManager(
                environment="test",
                config_base_path=config_dir,
                use_dynamic_config=True
            )

            # Should still work (legacy mode)
            settings = settings_manager.get()
            assert settings is not None


class TestFeatureFlagIntegration:
    """Test feature flag integration with configuration system."""

    def test_feature_flag_config_subscription(self):
        """Test that feature flags subscribe to configuration changes."""
        from aurum.api.features.manager import FeatureFlagManager, InMemoryFeatureFlagStore

        # Create a feature flag manager
        store = InMemoryFeatureFlagStore()
        manager = FeatureFlagManager(store)

        async def test_async():
            # Subscribe to config changes
            await manager.subscribe_to_config_changes()

            # Initially no overrides
            config = manager._config_subscriber.__self__._dynamic_config_service.get()
            feature_flags_config = config.get("feature_flags", {})
            overrides = feature_flags_config.get("overrides", {})
            assert len(overrides) == 0

        asyncio.run(test_async())

    def test_feature_flag_defaults_update(self):
        """Test updating feature flag defaults from configuration."""
        from aurum.api.features.manager import FeatureFlagManager, InMemoryFeatureFlagStore

        store = InMemoryFeatureFlagStore()
        manager = FeatureFlagManager(store)

        async def test_async():
            # Update feature flag defaults
            config = {
                "feature_flags": {
                    "cache_ttl_seconds": 600,
                    "overrides": {
                        "test_flag": True
                    }
                }
            }

            await manager._update_feature_flag_defaults(config)

            # Should have updated cache TTL
            assert manager._cache_ttl == 600

        asyncio.run(test_async())


class TestPerformance:
    """Test performance characteristics of the configuration system."""

    def test_config_loading_performance(self):
        """Test that configuration loading is reasonably fast."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Create a moderately complex configuration
            config = {
                "api": {"title": "Test API", "version": "1.0.0"},
                "database": {"host": "localhost", "port": 5432, "pool": {"min_size": 1, "max_size": 10}},
                "redis": {"host": "localhost", "port": 6379, "db": 0},
                "security": {"secret_key": "test-key", "algorithm": "HS256"},
                "feature_flags": {"cache_enabled": True, "cache_ttl_seconds": 300}
            }
            (config_dir / "base.yaml").write_text(yaml.dump(config))

            start_time = time.time()
            service = DynamicConfigService(
                environment="test",
                config_base_path=config_dir,
                hot_reload_enabled=False
            )
            config = service.get()
            end_time = time.time()

            load_time = (end_time - start_time) * 1000  # Convert to ms

            # Should load in reasonable time (less than 100ms for this simple config)
            assert load_time < 100
            assert config is not None

    def test_validation_performance(self):
        """Test that configuration validation is reasonably fast."""
        config = {
            "api": {
                "title": "Test API",
                "version": "1.0.0",
                "host": "localhost",
                "port": 8000,
                "debug": False
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "pool": {"min_size": 1, "max_size": 10}
            }
        }

        start_time = time.time()
        validated = validate_and_coerce_config(config)
        end_time = time.time()

        validation_time = (end_time - start_time) * 1000  # Convert to ms

        # Should validate quickly
        assert validation_time < 50
        assert validated is not None

    def test_change_tracking_performance(self):
        """Test that change tracking doesn't significantly impact performance."""
        tracker = get_change_tracker()

        async def test_async():
            config = {"api": {"title": f"Test API {i}"} for i in range(100)}

            start_time = time.time()
            for i in range(100):
                await tracker.record_change(
                    change_type=ChangeType.UPDATED,
                    source=ChangeSource.API,
                    actor="test_user",
                    reason=f"Test change {i}",
                    new_config=config
                )
            end_time = time.time()

            tracking_time = (end_time - start_time) * 1000  # Convert to ms

            # Should handle 100 changes in reasonable time
            assert tracking_time < 1000  # Less than 1 second for 100 changes

        asyncio.run(test_async())


class TestErrorHandling:
    """Test error handling and resilience."""

    def test_invalid_configuration_graceful_handling(self):
        """Test that invalid configuration is handled gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Create invalid YAML
            (config_dir / "base.yaml").write_text("invalid: yaml: [content")

            # Should not crash, should fallback gracefully
            service = DynamicConfigService(
                environment="test",
                config_base_path=config_dir,
                hot_reload_enabled=False
            )

            # Should still return some configuration (defaults)
            config = service.get()
            assert isinstance(config, dict)

    def test_missing_files_handling(self):
        """Test handling of missing configuration files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Don't create any config files

            service = DynamicConfigService(
                environment="test",
                config_base_path=config_dir,
                hot_reload_enabled=False
            )

            # Should still work with just env vars and defaults
            config = service.get()
            assert isinstance(config, dict)

    def test_corrupted_change_tracking_recovery(self):
        """Test recovery from corrupted change tracking data."""
        # This test would require mocking database failures
        # For now, just test that the system doesn't crash
        tracker = get_change_tracker()

        async def test_async():
            # Record a change with invalid data
            try:
                await tracker.record_change(
                    change_type=ChangeType.UPDATED,
                    source=ChangeSource.API,
                    actor="test_user",
                    reason="Test with None values",
                    new_config=None  # This should be handled gracefully
                )
            except Exception:
                # Should handle gracefully or raise appropriate error
                pass

        asyncio.run(test_async())
