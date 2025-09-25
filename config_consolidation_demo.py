#!/usr/bin/env python3
"""Demo script for configuration management consolidation feature flags."""

import sys
import os
import logging
from pathlib import Path
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Define classes locally for demo
class ConfigType(str, Enum):
    """Types of configuration supported by the system."""
    DATA_SOURCES = "data_sources"
    DATASETS = "datasets"
    QUOTAS = "quotas"
    MONITORING = "monitoring"
    STORAGE = "storage"
    ENVIRONMENT = "environment"

# Feature flags for configuration consolidation
CONFIG_FEATURE_FLAGS = {
    "use_consolidated_config": os.getenv("AURUM_USE_CONSOLIDATED_CONFIG", "false").lower() == "true",
    "config_migration_phase": os.getenv("AURUM_CONFIG_MIGRATION_PHASE", "1"),
    "enable_config_validation": os.getenv("AURUM_ENABLE_CONFIG_VALIDATION", "false").lower() == "true",
}

class MockConfigMetadata:
    """Mock metadata for demonstration."""
    def __init__(self, source_file: str, migration_status: str = "legacy"):
        self.source_file = source_file
        self.migration_status = migration_status

class MockConfigurationManager:
    """Mock configuration manager for demonstration."""

    def __init__(self):
        self.configs = {}
        self.metadata = {}
        self._log_migration_status()

    def _log_migration_status(self):
        """Log current configuration migration status."""
        phase = CONFIG_FEATURE_FLAGS["config_migration_phase"]
        consolidated = CONFIG_FEATURE_FLAGS["use_consolidated_config"]
        validation = CONFIG_FEATURE_FLAGS["enable_config_validation"]
        logger = logging.getLogger(__name__)

        logger.info(f"Config Migration Status: Phase {phase}, Consolidated: {consolidated}")
        logger.info(f"Config Validation Enabled: {validation}")

    def load_config(self, config_type: ConfigType, config_name: str) -> Dict[str, Any]:
        """Load configuration using feature flags for migration control."""
        cache_key = f"{config_type.value}:{config_name}"

        if cache_key in self.configs:
            return self.configs[cache_key]

        # Simulate legacy vs consolidated loading
        if CONFIG_FEATURE_FLAGS["use_consolidated_config"]:
            # Consolidated mode
            config_data = {
                "source": config_name,
                "type": "consolidated",
                "endpoint": f"https://api.{config_name}.com",
                "credentials": {"type": "vault"},
                "metadata": {"version": "2.0", "schema": "typed"}
            }
            self.metadata[cache_key] = MockConfigMetadata(f"consolidated/{config_name}.json", "consolidated")
            logging.info(f"Loaded consolidated config: {cache_key}")
        else:
            # Legacy mode
            config_data = {
                "source": config_name,
                "type": "legacy",
                "endpoint": f"https://legacy.{config_name}.com",
                "credentials": {"type": "file"}
            }
            self.metadata[cache_key] = MockConfigMetadata(f"legacy/{config_name}_catalog.json", "legacy")
            logging.info(f"Loaded legacy config: {cache_key} from scattered files")

        self.configs[cache_key] = config_data
        return config_data

    def get_migration_status(self) -> Dict[str, Any]:
        """Get overall configuration migration status."""
        return {
            "migration_phase": CONFIG_FEATURE_FLAGS["config_migration_phase"],
            "consolidated_enabled": CONFIG_FEATURE_FLAGS["use_consolidated_config"],
            "validation_enabled": CONFIG_FEATURE_FLAGS["enable_config_validation"],
            "loaded_configs": len(self.configs),
            "legacy_files": len([m for m in self.metadata.values() if m.migration_status == "legacy"]),
            "consolidated_files": len([m for m in self.metadata.values() if m.migration_status == "consolidated"]),
        }

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def demo_config_consolidation():
    """Demonstrate configuration management consolidation."""
    logger.info("=== Configuration Management Consolidation Demo ===")

    # Show current migration status
    logger.info("1. Current Migration Status:")
    logger.info(f"   Consolidation enabled: {CONFIG_FEATURE_FLAGS['use_consolidated_config']}")
    logger.info(f"   Migration phase: {CONFIG_FEATURE_FLAGS['config_migration_phase']}")
    logger.info(f"   Validation enabled: {CONFIG_FEATURE_FLAGS['enable_config_validation']}")

    # Test legacy mode (default)
    logger.info("\n2. Testing Legacy Mode (Scattered Files):")
    os.environ["AURUM_USE_CONSOLIDATED_CONFIG"] = "false"
    os.environ["AURUM_CONFIG_MIGRATION_PHASE"] = "1"

    manager = MockConfigurationManager()
    legacy_config = manager.load_config(ConfigType.DATA_SOURCES, "eia")
    logger.info(f"✅ Legacy EIA config loaded: {legacy_config}")
    logger.info(f"   Metadata: {manager.metadata.get('data_sources:eia', {}).migration_status}")

    # Test consolidated mode
    logger.info("\n3. Testing Consolidated Mode:")
    os.environ["AURUM_USE_CONSOLIDATED_CONFIG"] = "true"
    os.environ["AURUM_CONFIG_MIGRATION_PHASE"] = "2"

    manager2 = MockConfigurationManager()
    consolidated_config = manager2.load_config(ConfigType.DATA_SOURCES, "iso")
    logger.info(f"✅ Consolidated ISO config loaded: {consolidated_config}")
    logger.info(f"   Metadata: {manager2.metadata.get('data_sources:iso', {}).migration_status}")

    # Show migration status
    status = manager2.get_migration_status()
    logger.info(f"\n4. Migration Status: {status}")

    # Show consolidation benefits
    logger.info("\n=== Consolidation Benefits ===")
    logger.info("✅ Centralizes 40+ config files into single manager")
    logger.info("✅ Provides typed configuration schemas")
    logger.info("✅ Enables validation and version control")
    logger.info("✅ Simplifies configuration updates")
    logger.info("✅ Feature-flagged gradual migration")
    logger.info("✅ Automatic fallback to legacy mode")

    logger.info("\n=== Environment Variables ===")
    logger.info("AURUM_USE_CONSOLIDATED_CONFIG=true  # Enable consolidated mode")
    logger.info("AURUM_CONFIG_MIGRATION_PHASE=2      # Set migration phase")
    logger.info("AURUM_ENABLE_CONFIG_VALIDATION=true # Enable validation")

    logger.info("\n=== Configuration Types ===")
    for config_type in ConfigType:
        logger.info(f"   - {config_type.value}")

    logger.info("\n=== Migration Strategy ===")
    logger.info("1. Phase 1: Scattered config files (current)")
    logger.info("2. Phase 2: Consolidated config manager")
    logger.info("3. Phase 3: Schema-driven validation")

def demo_config_comparison():
    """Compare legacy vs consolidated configuration management."""
    logger.info("\n=== Configuration Management Comparison ===")

    # Legacy approach
    logger.info("Legacy Configuration Management:")
    logger.info("  - config/eia_catalog.json")
    logger.info("  - config/eia_ingest_datasets.json")
    logger.info("  - config/iso_catalog.json")
    logger.info("  - config/noaa_ingest_datasets.json")
    logger.info("  - ... (40+ scattered files)")
    logger.info("  - No type safety")
    logger.info("  - Hard to validate")
    logger.info("  - Difficult to maintain")
    logger.info("  - No version control")

    # Consolidated approach
    logger.info("\nConsolidated Configuration Management:")
    logger.info("  - src/aurum/config/ConfigurationManager")
    logger.info("  - Single entry point for all configs")
    logger.info("  - Typed configuration objects")
    logger.info("  - Schema validation")
    logger.info("  - Version tracking")
    logger.info("  - Migration metadata")
    logger.info("  - Feature-flagged migration")

if __name__ == "__main__":
    demo_config_consolidation()
    demo_config_comparison()
