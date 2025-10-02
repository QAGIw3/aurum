"""Consolidated configuration loader with environment-specific overrides."""

import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Simple mock for missing dependencies
class MockAurumSettings:
    def __init__(self):
        self.api = type('API', (), {'version': '1.0.0'})()
        self.database = type('Database', (), {'timescale_dsn': 'mock://'})()
        self.redis = type('Redis', (), {'redis_url': 'mock://'})()
        self.cache = type('Cache', (), {'high_frequency_ttl': 300})()
        self.enable_v2_only = True
        self.enable_timescale_caggs = True
        self.observability = type('Observability', (), {'enable_tracing': True})()


@dataclass
class ConfigurationProfile:
    """Configuration profile for different environments."""

    name: str
    base_config: Dict[str, Any]
    overrides: Dict[str, Any] = field(default_factory=dict)

    def get(self, key_path: str, default: Any = None) -> Any:
        """Get a configuration value using dot notation path."""
        keys = key_path.split('.')
        current = self.get_merged_config()

        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default

        return current

    def get_merged_config(self) -> Dict[str, Any]:
        """Get the merged configuration (base + overrides)."""
        merged = self.base_config.copy()

        def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
            """Deep merge two dictionaries."""
            result = base.copy()
            for key, value in override.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = deep_merge(result[key], value)
                else:
                    result[key] = value
            return result

        return deep_merge(merged, self.overrides)


class ConsolidatedConfigurationLoader:
    """Loader for consolidated configuration files with environment support."""

    def __init__(self, config_root: str = "config"):
        self.config_root = Path(config_root)
        self.profiles: Dict[str, ConfigurationProfile] = {}
        self._loaded = False

    def load_configuration(self, data_source: str) -> ConfigurationProfile:
        """Load configuration for a specific data source."""
        if not self._loaded:
            self._load_all_profiles()

        if data_source not in self.profiles:
            raise ValueError(f"Configuration for data source '{data_source}' not found")

        return self.profiles[data_source]

    def get_environment_profile(self, environment: str) -> ConfigurationProfile:
        """Get configuration profile for a specific environment."""
        # Get the base configuration (development by default)
        base_profile = self.profiles.get("base") or self.profiles.get("development")

        if not base_profile:
            raise ValueError("No base configuration found")

        # Apply environment-specific overrides
        if environment in self.profiles:
            env_profile = self.profiles[environment]
            return ConfigurationProfile(
                name=environment,
                base_config=base_profile.base_config,
                overrides=env_profile.get_merged_config()
            )

        return base_profile

    def _load_all_profiles(self) -> None:
        """Load all configuration profiles."""
        if not self.config_root.exists():
            logger.warning(f"Configuration root '{self.config_root}' does not exist")
            return

        # Load data source configurations
        data_sources_dir = self.config_root / "data_sources"
        if data_sources_dir.exists():
            for config_file in data_sources_dir.glob("*.json"):
                self._load_data_source_config(config_file)

        # Load environment configurations
        environments_dir = self.config_root / "environments"
        if environments_dir.exists():
            for config_file in environments_dir.glob("*.json"):
                self._load_environment_config(config_file)

        self._loaded = True

    def _load_data_source_config(self, config_file: Path) -> None:
        """Load a data source configuration file."""
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)

            source_name = config_file.stem  # filename without extension

            # Extract base configuration
            base_config = {
                key: value for key, value in config_data.items()
                if key not in ["environments"]
            }

            # Extract environment-specific overrides
            overrides = {}
            if "environments" in config_data:
                for env_name, env_config in config_data["environments"].items():
                    if env_name not in self.profiles:
                        self.profiles[env_name] = ConfigurationProfile(
                            name=env_name,
                            base_config={}
                        )
                    # Merge environment config into existing profile
                    existing_overrides = self.profiles[env_name].overrides
                    self.profiles[env_name].overrides.update(env_config)

            # Create main profile
            self.profiles[source_name] = ConfigurationProfile(
                name=source_name,
                base_config=base_config
            )

            logger.info(f"Loaded data source configuration: {source_name}")

        except Exception as exc:
            logger.error(f"Failed to load configuration {config_file}: {exc}")

    def _load_environment_config(self, config_file: Path) -> None:
        """Load an environment-specific configuration file."""
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)

            env_name = config_file.stem

            if env_name not in self.profiles:
                self.profiles[env_name] = ConfigurationProfile(
                    name=env_name,
                    base_config={}
                )

            self.profiles[env_name].overrides.update(config_data)

            logger.info(f"Loaded environment configuration: {env_name}")

        except Exception as exc:
            logger.error(f"Failed to load environment configuration {config_file}: {exc}")

    def list_available_profiles(self) -> List[str]:
        """List all available configuration profiles."""
        return list(self.profiles.keys())

    def get_data_source_summary(self, data_source: str) -> Dict[str, Any]:
        """Get a summary of a data source configuration."""
        profile = self.load_configuration(data_source)

        summary = {
            "name": profile.name,
            "metadata": profile.base_config.get("metadata", {}),
            "has_catalog": "catalog" in profile.base_config,
            "has_ingestion": "ingestion" in profile.base_config,
            "has_quality": "quality" in profile.base_config,
            "environments": list(profile.overrides.keys()) if profile.overrides else []
        }

        return summary


# Global configuration loader instance
_configuration_loader: Optional[ConsolidatedConfigurationLoader] = None


def get_configuration_loader() -> ConsolidatedConfigurationLoader:
    """Get the global configuration loader."""
    global _configuration_loader
    if _configuration_loader is None:
        _configuration_loader = ConsolidatedConfigurationLoader()
    return _configuration_loader


def get_data_source_config(data_source: str, environment: Optional[str] = None) -> ConfigurationProfile:
    """Get configuration for a data source, optionally for a specific environment."""
    loader = get_configuration_loader()

    if environment:
        return loader.get_environment_profile(environment)

    return loader.load_configuration(data_source)


def get_eia_config(environment: Optional[str] = None) -> ConfigurationProfile:
    """Get EIA data source configuration."""
    return get_data_source_config("eia", environment)


def get_fred_config(environment: Optional[str] = None) -> ConfigurationProfile:
    """Get FRED data source configuration."""
    return get_data_source_config("fred", environment)


def get_noaa_config(environment: Optional[str] = None) -> ConfigurationProfile:
    """Get NOAA data source configuration."""
    return get_data_source_config("noaa", environment)


def get_iso_config(environment: Optional[str] = None) -> ConfigurationProfile:
    """Get ISO data source configuration."""
    return get_data_source_config("iso", environment)
