"""
Tests for the Dynamic Configuration Management System.

This module tests the core functionality of the dynamic configuration system including:
- Configuration source loading and merging
- Validation and schema enforcement
- Change tracking and versioning
- Hot reloading and file watching
"""

import asyncio
import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from aurum.config.change_tracking import ChangeTracker, ChangeType, ChangeSource
from aurum.config.dynamic_config import DynamicConfigService, EnvConfigSource, FileConfigSource, EphemeralOverrideSource, LayerMerger
from aurum.config.validation import SchemaRegistry, ConfigValidator, validate_and_coerce_config


class TestLayerMerger:
    """Test the LayerMerger functionality."""

    def test_deep_merge_basic(self):
        """Test basic deep merge functionality."""
        merger = LayerMerger()

        layer1 = {"a": 1, "b": {"c": 2}}
        layer2 = {"b": {"d": 3}, "e": 4}

        result = merger.merge([layer1, layer2])

        assert result == {
            "a": 1,
            "b": {"c": 2, "d": 3},
            "e": 4
        }

    def test_deep_merge_precedence(self):
        """Test that later layers take precedence."""
        merger = LayerMerger()

        layer1 = {"a": 1, "b": {"c": 2}}
        layer2 = {"a": 10, "b": {"d": 3}}

        result = merger.merge([layer1, layer2])

        assert result == {
            "a": 10,  # layer2 wins
            "b": {"c": 2, "d": 3}
        }

    def test_coercion_hooks(self):
        """Test coercion hook functionality."""
        merger = LayerMerger()

        # Register a coercion hook
        def string_to_int(value):
            if isinstance(value, str):
                try:
                    return int(value)
                except ValueError:
                    return value
            return value

        merger.register_coercion_hook("test.number", string_to_int)

        layer = {"test": {"number": "42"}}
        result = merger.merge([layer])

        assert result == {"test": {"number": 42}}


class TestEnvConfigSource:
    """Test the environment variable configuration source."""

    def test_load_env_vars(self):
        """Test loading configuration from environment variables."""
        source = EnvConfigSource("TEST_")

        # Mock environment variables
        with patch.dict(os.environ, {
            "TEST_API_TITLE": "Test API",
            "TEST_DATABASE__URL": "postgresql://localhost/test",
            "TEST_DEBUG": "true"
        }):
            config = source.load()

        assert config == {
            "api": {"title": "Test API"},
            "database": {"url": "postgresql://localhost/test"},
            "debug": "true"
        }

    def test_priority(self):
        """Test that EnvConfigSource has correct priority."""
        source = EnvConfigSource()
        assert source.priority == 100


class TestFileConfigSource:
    """Test the file-based configuration source."""

    def test_load_yaml_files(self):
        """Test loading YAML configuration files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Create base configuration
            base_config = {
                "api": {"title": "Base API"},
                "database": {"host": "localhost"}
            }
            (config_dir / "base.yaml").write_text(yaml.dump(base_config))

            # Create environment-specific configuration
            env_config = {
                "api": {"title": "Production API"},
                "database": {"host": "prod-db"}
            }
            (config_dir / "production.yaml").write_text(yaml.dump(env_config))

            source = FileConfigSource(config_dir, "production")
            config = source.load()

        assert config["api"]["title"] == "Production API"
        assert config["database"]["host"] == "prod-db"

    def test_load_json_files(self):
        """Test loading JSON configuration files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            config_data = {
                "api": {"title": "JSON API"},
                "redis": {"host": "redis-server"}
            }
            (config_dir / "base.json").write_text(json.dumps(config_data))

            source = FileConfigSource(config_dir, "development")
            config = source.load()

        assert config["api"]["title"] == "JSON API"
        assert config["redis"]["host"] == "redis-server"


class TestEphemeralOverrideSource:
    """Test the ephemeral override configuration source."""

    def test_basic_overrides(self):
        """Test basic ephemeral override functionality."""
        source = EphemeralOverrideSource()

        # Set some overrides
        source.set_override("test1", {"api": {"debug": True}})
        source.set_override("test2", {"database": {"pool_size": 10}})

        config = source.load()

        # Should merge all active overrides
        assert config["api"]["debug"] is True
        assert config["database"]["pool_size"] == 10

    def test_ttl_expiration(self):
        """Test TTL-based override expiration."""
        source = EphemeralOverrideSource()

        # Set override with short TTL
        source.set_override("test", {"api": {"debug": True}}, ttl_seconds=0.1)

        # Should be present initially
        config = source.load()
        assert config["api"]["debug"] is True

        # Wait for expiration
        time.sleep(0.2)

        # Should be expired
        config = source.load()
        assert "api" not in config or "debug" not in config.get("api", {})


class TestSchemaValidation:
    """Test configuration validation and schema enforcement."""

    def test_schema_registry(self):
        """Test schema registry functionality."""
        registry = SchemaRegistry()

        # Should have built-in schemas
        schemas = registry.list_schemas()
        assert "api" in schemas
        assert "redis" in schemas
        assert "database" in schemas
        assert "security" in schemas
        assert "feature_flags" in schemas

    def test_config_validation(self):
        """Test configuration validation."""
        registry = SchemaRegistry()
        validator = ConfigValidator(registry, strict_mode=False)

        # Valid configuration
        valid_config = {
            "api": {
                "title": "Test API",
                "version": "1.0.0",
                "host": "localhost",
                "port": 8000
            }
        }

        assert validator.validate_config(valid_config)

        # Invalid configuration
        invalid_config = {
            "api": {
                "title": "Test API",
                "port": "invalid"  # Should be integer
            }
        }

        # Should still validate (non-strict mode)
        assert validator.validate_config(invalid_config)

    def test_type_coercion(self):
        """Test automatic type coercion."""
        registry = SchemaRegistry()
        validator = ConfigValidator(registry)

        config = {
            "api": {
                "port": "8000",  # String that should be coerced to int
                "debug": "true"  # String that should be coerced to bool
            }
        }

        coerced = validator.coerce_types(config)

        assert isinstance(coerced["api"]["port"], int)
        assert coerced["api"]["port"] == 8000
        assert isinstance(coerced["api"]["debug"], bool)
        assert coerced["api"]["debug"] is True


class TestChangeTracker:
    """Test change tracking functionality."""

    def test_record_change(self):
        """Test recording configuration changes."""
        tracker = ChangeTracker()

        async def test_async():
            change_id = await tracker.record_change(
                change_type=ChangeType.UPDATED,
                source=ChangeSource.API,
                actor="test_user",
                namespace="api",
                reason="Test change",
                old_config={"version": "1.0"},
                new_config={"version": "1.1"}
            )

            assert change_id is not None
            assert len(change_id) > 0

            # Check that change was recorded
            changes = tracker.get_change_history(limit=1)
            assert len(changes) == 1
            assert changes[0].actor == "test_user"
            assert changes[0].namespace == "api"

        asyncio.run(test_async())

    def test_version_creation(self):
        """Test configuration version creation."""
        tracker = ChangeTracker()

        async def test_async():
            config = {"api": {"title": "Test API"}, "version": "1.0"}
            change_id = "test_change_id"

            version = await tracker.create_version(config, change_id)

            assert version > 0

            # Retrieve the version
            retrieved = tracker.get_version(version)
            assert retrieved is not None
            assert retrieved.config == config
            assert retrieved.change_id == change_id

        asyncio.run(test_async())

    def test_diff_calculation(self):
        """Test configuration diffing."""
        tracker = ChangeTracker()

        old_config = {"api": {"title": "Old API"}, "version": "1.0"}
        new_config = {"api": {"title": "New API"}, "version": "1.1"}

        diff = tracker._calculate_diff(old_config, new_config)

        assert "values_changed" in diff
        assert len(diff["values_changed"]) > 0


class TestDynamicConfigService:
    """Test the main DynamicConfigService."""

    def test_service_initialization(self):
        """Test service initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Create a simple config file
            config_data = {"api": {"title": "Test API"}}
            (config_dir / "base.yaml").write_text(yaml.dump(config_data))

            service = DynamicConfigService(
                environment="test",
                config_base_path=config_dir,
                hot_reload_enabled=False
            )

            config = service.get()
            assert "api" in config
            assert config["api"]["title"] == "Test API"

    def test_ephemeral_overrides(self):
        """Test ephemeral override functionality."""
        service = DynamicConfigService(hot_reload_enabled=False)

        # Set an override
        service.set_ephemeral_override("test", {"api": {"debug": True}})

        config = service.get()
        assert config["api"]["debug"] is True

        # Remove override
        service.remove_ephemeral_override("test")

        config = service.get()
        assert "api" not in config or "debug" not in config.get("api", {})

    def test_configuration_subscription(self):
        """Test configuration change subscription."""
        service = DynamicConfigService(hot_reload_enabled=False)

        changes_received = []

        def on_change(snapshot):
            changes_received.append(snapshot.config)

        service.subscribe(on_change)

        # Trigger a change
        service.set_ephemeral_override("test", {"api": {"debug": True}})

        # Should have received the change
        assert len(changes_received) > 0
        assert changes_received[-1]["api"]["debug"] is True


class TestIntegration:
    """Integration tests for the complete configuration system."""

    def test_full_config_pipeline(self):
        """Test the complete configuration pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Create configuration files
            base_config = {
                "api": {
                    "title": "Aurum API",
                    "version": "1.0.0",
                    "host": "0.0.0.0"
                },
                "database": {
                    "host": "localhost",
                    "port": 5432
                }
            }
            (config_dir / "base.yaml").write_text(yaml.dump(base_config))

            prod_config = {
                "api": {
                    "host": "api.example.com",
                    "port": 8000
                },
                "database": {
                    "host": "prod-db.example.com"
                }
            }
            (config_dir / "production.yaml").write_text(yaml.dump(prod_config))

            # Create service
            service = DynamicConfigService(
                environment="production",
                config_base_path=config_dir,
                hot_reload_enabled=False
            )

            # Get configuration
            config = service.get()

            # Should have merged configuration
            assert config["api"]["title"] == "Aurum API"  # From base
            assert config["api"]["host"] == "api.example.com"  # From production
            assert config["database"]["host"] == "prod-db.example.com"  # From production

            # Validate configuration
            validated = validate_and_coerce_config(config)
            assert validated["api"]["port"] == 8000  # Should be coerced to int

    def test_env_var_override(self):
        """Test that environment variables override file configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)

            # Create base configuration
            base_config = {"api": {"title": "File API", "port": 8080}}
            (config_dir / "base.yaml").write_text(yaml.dump(base_config))

            # Set environment variable
            with patch.dict(os.environ, {"AURUM_API__TITLE": "Env API"}):
                service = DynamicConfigService(
                    environment="development",
                    config_base_path=config_dir,
                    hot_reload_enabled=False
                )

                config = service.get()
                assert config["api"]["title"] == "Env API"  # Env var wins
                assert config["api"]["port"] == 8080  # From file
