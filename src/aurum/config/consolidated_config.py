"""Feature-flagged consolidated configuration management system.

Supports gradual migration from scattered config files to centralized management:
- Legacy mode: Individual config files scattered across project
- Consolidated mode: Single configuration manager with typed schemas
- Feature flags control migration between modes

Migration phases:
1. Scattered config files (current state)
2. Consolidated config manager
3. Schema-driven configuration validation
"""

import os
import json
import logging
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from enum import Enum
from dataclasses import dataclass
from abc import ABC, abstractmethod

# Feature flags for configuration consolidation
CONFIG_FEATURE_FLAGS = {
    "use_consolidated_config": os.getenv("AURUM_USE_CONSOLIDATED_CONFIG", "false").lower() == "true",
    "config_migration_phase": os.getenv("AURUM_CONFIG_MIGRATION_PHASE", "1"),
    "enable_config_validation": os.getenv("AURUM_ENABLE_CONFIG_VALIDATION", "false").lower() == "true",
    "config_base_path": os.getenv("AURUM_CONFIG_PATH", "/Users/mstudio/Library/Mobile Documents/com~apple~CloudDocs/dev/aurum/config"),
}

class ConfigType(str, Enum):
    """Types of configuration supported by the system."""
    DATA_SOURCES = "data_sources"
    DATASETS = "datasets"
    QUOTAS = "quotas"
    MONITORING = "monitoring"
    STORAGE = "storage"
    NETWORKING = "networking"
    SECURITY = "security"
    PERFORMANCE = "performance"
    ENVIRONMENT = "environment"

class ConfigFormat(str, Enum):
    """Supported configuration file formats."""
    JSON = "json"
    YAML = "yaml"
    CSV = "csv"
    ENV = "env"

@dataclass
class ConfigMetadata:
    """Metadata for configuration entries."""
    source_file: str
    format_type: ConfigFormat
    version: str
    description: str
    last_modified: str
    migration_status: str = "legacy"

class ConfigurationManager:
    """Centralized configuration management with feature flags."""

    def __init__(self, base_path: str = None):
        self.base_path = Path(base_path or CONFIG_FEATURE_FLAGS["config_base_path"])
        self.configs: Dict[str, Any] = {}
        self.metadata: Dict[str, ConfigMetadata] = {}
        self._loaded_files: List[str] = []

        # Log migration status
        self._log_migration_status()

    def _log_migration_status(self):
        """Log current configuration migration status."""
        phase = CONFIG_FEATURE_FLAGS["config_migration_phase"]
        consolidated = CONFIG_FEATURE_FLAGS["use_consolidated_config"]
        validation = CONFIG_FEATURE_FLAGS["enable_config_validation"]
        logger = logging.getLogger(__name__)

        logger.info(f"Config Migration Status: Phase {phase}, Consolidated: {consolidated}")
        logger.info(f"Config Validation Enabled: {validation}")
        logger.info(f"Config Base Path: {self.base_path}")

    def load_config(self, config_type: ConfigType, config_name: str) -> Dict[str, Any]:
        """Load configuration using feature flags for migration control."""
        if CONFIG_FEATURE_FLAGS["use_consolidated_config"]:
            return self._load_consolidated_config(config_type, config_name)
        else:
            return self._load_legacy_config(config_type, config_name)

    def _load_consolidated_config(self, config_type: ConfigType, config_name: str) -> Dict[str, Any]:
        """Load configuration from consolidated system."""
        cache_key = f"{config_type.value}:{config_name}"

        if cache_key in self.configs:
            return self.configs[cache_key]

        # Try to load from consolidated structure
        config_path = self.base_path / "consolidated" / f"{config_name}.{config_type.value}.json"

        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config_data = json.load(f)

                # Load metadata
                metadata_path = self.base_path / "consolidated" / f"{config_name}.{config_type.value}.meta.json"
                if metadata_path.exists():
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                    self.metadata[cache_key] = ConfigMetadata(**metadata)

                self.configs[cache_key] = config_data
                logging.info(f"Loaded consolidated config: {cache_key}")
                return config_data

            except Exception as e:
                logging.warning(f"Failed to load consolidated config {cache_key}: {e}")

        # Fallback to legacy
        return self._load_legacy_config(config_type, config_name)

    def _load_legacy_config(self, config_type: ConfigType, config_name: str) -> Dict[str, Any]:
        """Load configuration from legacy scattered files."""
        cache_key = f"legacy:{config_type.value}:{config_name}"

        if cache_key in self.configs:
            return self.configs[cache_key]

        # Map config types to legacy file patterns
        file_mappings = {
            ConfigType.DATA_SOURCES: f"{config_name}_catalog.json",
            ConfigType.DATASETS: f"{config_name}_ingest_datasets.json",
            ConfigType.QUOTAS: "data_source_quotas.json",
            ConfigType.MONITORING: f"{config_name}_monitoring_config.json",
            ConfigType.STORAGE: "storage/minio_buckets.json",
        }

        file_name = file_mappings.get(config_type, f"{config_name}.json")
        config_path = self.base_path / file_name

        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    if config_path.suffix == '.json':
                        config_data = json.load(f)
                    elif config_path.suffix in ['.yml', '.yaml']:
                        config_data = yaml.safe_load(f)
                    else:
                        config_data = {"error": f"Unsupported format: {config_path.suffix}"}

                # Create legacy metadata
                self.metadata[cache_key] = ConfigMetadata(
                    source_file=str(config_path),
                    format_type=ConfigFormat.JSON if config_path.suffix == '.json' else ConfigFormat.YAML,
                    version="1.0.0",
                    description=f"Legacy config for {config_type.value}",
                    last_modified="unknown",
                    migration_status="legacy"
                )

                self.configs[cache_key] = config_data
                logging.info(f"Loaded legacy config: {cache_key} from {config_path}")
                return config_data

            except Exception as e:
                logging.error(f"Failed to load legacy config {config_path}: {e}")

        # Return empty config if not found
        empty_config = {}
        self.configs[cache_key] = empty_config
        return empty_config

    def get_all_configs(self, config_type: ConfigType) -> Dict[str, Any]:
        """Get all configurations of a specific type."""
        configs = {}

        # Search for matching files
        if config_type == ConfigType.DATA_SOURCES:
            catalog_files = list(self.base_path.glob("*_catalog.json"))
            for file_path in catalog_files:
                source_name = file_path.stem.replace("_catalog", "")
                configs[source_name] = self.load_config(config_type, source_name)

        elif config_type == ConfigType.DATASETS:
            dataset_files = list(self.base_path.glob("*_ingest_datasets.json"))
            for file_path in dataset_files:
                dataset_name = file_path.stem.replace("_ingest_datasets", "")
                configs[dataset_name] = self.load_config(config_type, dataset_name)

        return configs

    def validate_config(self, config_type: ConfigType, config_data: Dict[str, Any]) -> bool:
        """Validate configuration against schema if validation is enabled."""
        if not CONFIG_FEATURE_FLAGS["enable_config_validation"]:
            return True

        # For now, just check basic structure
        required_fields = {
            ConfigType.DATA_SOURCES: ["name", "type", "endpoint"],
            ConfigType.DATASETS: ["source", "format", "schema"],
            ConfigType.QUOTAS: ["source", "limit", "period"],
        }

        fields = required_fields.get(config_type, [])
        for field in fields:
            if field not in config_data:
                logging.warning(f"Missing required field '{field}' in {config_type.value} config")
                return False

        return True

    def get_migration_status(self) -> Dict[str, Any]:
        """Get overall configuration migration status."""
        return {
            "migration_phase": CONFIG_FEATURE_FLAGS["config_migration_phase"],
            "consolidated_enabled": CONFIG_FEATURE_FLAGS["use_consolidated_config"],
            "validation_enabled": CONFIG_FEATURE_FLAGS["enable_config_validation"],
            "loaded_configs": len(self.configs),
            "loaded_files": len(self._loaded_files),
            "legacy_files": len([m for m in self.metadata.values() if m.migration_status == "legacy"]),
            "consolidated_files": len([m for m in self.metadata.values() if m.migration_status == "consolidated"]),
        }

# Convenience functions for common configurations
def get_data_source_config(source_name: str) -> Dict[str, Any]:
    """Get configuration for a specific data source."""
    manager = ConfigurationManager()
    return manager.load_config(ConfigType.DATA_SOURCES, source_name)

def get_dataset_config(dataset_name: str) -> Dict[str, Any]:
    """Get configuration for a specific dataset."""
    manager = ConfigurationManager()
    return manager.load_config(ConfigType.DATASETS, dataset_name)

def get_quota_config() -> Dict[str, Any]:
    """Get quota configuration."""
    manager = ConfigurationManager()
    return manager.load_config(ConfigType.QUOTAS, "quotas")

def get_storage_config() -> Dict[str, Any]:
    """Get storage configuration."""
    manager = ConfigurationManager()
    return manager.load_config(ConfigType.STORAGE, "storage")

def get_environment_config() -> Dict[str, Any]:
    """Get environment configuration."""
    manager = ConfigurationManager()
    return manager.load_config(ConfigType.ENVIRONMENT, "environment")
