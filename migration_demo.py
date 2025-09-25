#!/usr/bin/env python3
"""
Aurum Migration Demo Script

This script demonstrates how to use the feature-flagged refactoring system
for gradual migration from legacy to simplified components with comprehensive
monitoring and rollback capabilities.

Usage:
    python migration_demo.py [phase]

Phases:
    - setup: Configure feature flags for monitoring
    - test-settings: Test settings migration
    - test-database: Test database migration
    - advance: Advance to hybrid phase
    - monitor: Show migration status
    - rollback: Rollback to legacy phase
    - health: Validate migration health

Environment Variables:
    AURUM_USE_SIMPLIFIED_SETTINGS=1    # Enable simplified settings
    AURUM_ENABLE_MIGRATION_MONITORING=1 # Enable monitoring
    AURUM_SETTINGS_MIGRATION_PHASE=hybrid # Migration phase
"""

import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Import Aurum components
try:
    from aurum.core.settings import (
        AurumSettings,
        is_feature_enabled,
        get_migration_phase,
        log_migration_status,
        validate_migration_health,
        advance_migration_phase,
        rollback_migration_phase,
        FEATURE_FLAGS,
    )
    from aurum.api.database.trino_client import (
        get_trino_client,
        advance_db_migration_phase,
        rollback_db_migration_phase,
        get_db_migration_status,
        DB_FEATURE_FLAGS,
    )
except ImportError as e:
    logger.error(f"Failed to import Aurum components: {e}")
    sys.exit(1)


class MigrationDemo:
    """Demonstrates the migration capabilities."""

    def __init__(self):
        self.results = []

    def log_step(self, step: str, status: str, details: str = ""):
        """Log a migration step."""
        message = f"[{status}] {step}"
        if details:
            message += f" - {details}"

        if status == "SUCCESS":
            logger.info(message)
        elif status == "WARNING":
            logger.warning(message)
        elif status == "ERROR":
            logger.error(message)
        else:
            logger.info(message)

        self.results.append({
            "step": step,
            "status": status,
            "details": details,
            "timestamp": time.time()
        })

    def setup_environment(self):
        """Setup environment for migration testing."""
        self.log_step("Environment Setup", "STARTING")

        # Enable monitoring
        os.environ[FEATURE_FLAGS["ENABLE_MIGRATION_MONITORING"]] = "1"
        os.environ[DB_FEATURE_FLAGS["ENABLE_DB_MIGRATION_MONITORING"]] = "1"

        # Set initial phases
        os.environ[FEATURE_FLAGS["SETTINGS_MIGRATION_PHASE"]] = "legacy"
        os.environ[DB_FEATURE_FLAGS["DB_MIGRATION_PHASE"]] = "legacy"

        self.log_step("Environment Setup", "SUCCESS", "Monitoring enabled, phases set to legacy")

    def test_settings_migration(self):
        """Test settings system migration."""
        self.log_step("Settings Migration Test", "STARTING")

        try:
            # Test legacy settings
            legacy_settings = AurumSettings.from_env()
            self.log_step("Settings Migration Test", "SUCCESS", f"Legacy settings loaded: environment={legacy_settings.environment}")

            # Enable simplified settings
            os.environ[FEATURE_FLAGS["USE_SIMPLIFIED_SETTINGS"]] = "1"
            simplified_settings = AurumSettings.from_env()

            status = simplified_settings.get_migration_status()
            self.log_step("Settings Migration Test", "SUCCESS",
                         f"Simplified settings enabled: {status['using_simplified']}")

            return True
        except Exception as e:
            self.log_step("Settings Migration Test", "ERROR", str(e))
            return False

    def test_database_migration(self):
        """Test database layer migration."""
        self.log_step("Database Migration Test", "STARTING")

        try:
            # Test getting a database client
            client = get_trino_client("iceberg")
            self.log_step("Database Migration Test", "SUCCESS", f"Client type: {type(client).__name__}")

            # Test database migration status
            status = get_db_migration_status()
            self.log_step("Database Migration Test", "SUCCESS",
                         f"Migration phase: {status['migration_phase']}")

            return True
        except Exception as e:
            self.log_step("Database Migration Test", "ERROR", str(e))
            return False

    def advance_migration(self):
        """Advance migration phases."""
        self.log_step("Migration Advancement", "STARTING")

        # Advance settings migration
        success = advance_migration_phase("settings", "hybrid")
        if success:
            self.log_step("Settings Migration", "SUCCESS", "Advanced to hybrid phase")
        else:
            self.log_step("Settings Migration", "ERROR", "Failed to advance")

        # Advance database migration
        success = advance_db_migration_phase("hybrid")
        if success:
            self.log_step("Database Migration", "SUCCESS", "Advanced to hybrid phase")
        else:
            self.log_step("Database Migration", "ERROR", "Failed to advance")

    def show_monitoring_status(self):
        """Show current migration monitoring status."""
        self.log_step("Migration Monitoring", "STARTING")

        # Show settings status
        try:
            log_migration_status()
        except Exception as e:
            self.log_step("Settings Status", "ERROR", str(e))

        # Show database status
        try:
            status = get_db_migration_status()
            logger.info(f"Database Migration Status: {json.dumps(status, indent=2)}")
        except Exception as e:
            self.log_step("Database Status", "ERROR", str(e))

        # Validate health
        try:
            health = validate_migration_health()
            if health["healthy"]:
                self.log_step("Migration Health", "SUCCESS", "All systems healthy")
            else:
                self.log_step("Migration Health", "WARNING", f"Issues found: {health['issues']}")
        except Exception as e:
            self.log_step("Health Validation", "ERROR", str(e))

    def rollback_migration(self):
        """Rollback migration phases."""
        self.log_step("Migration Rollback", "STARTING")

        # Rollback settings
        success = rollback_migration_phase("settings")
        if success:
            self.log_step("Settings Rollback", "SUCCESS", "Rolled back to legacy")
        else:
            self.log_step("Settings Rollback", "WARNING", "Already at legacy phase")

        # Rollback database
        success = rollback_db_migration_phase()
        if success:
            self.log_step("Database Rollback", "SUCCESS", "Rolled back to legacy")
        else:
            self.log_step("Database Rollback", "WARNING", "Already at legacy phase")

    def run_demo(self, phase: str = "all"):
        """Run the complete migration demo."""
        logger.info("🚀 Starting Aurum Migration Demo")
        logger.info("=" * 50)

        if phase == "setup":
            self.setup_environment()
        elif phase == "test-settings":
            self.test_settings_migration()
        elif phase == "test-database":
            self.test_database_migration()
        elif phase == "advance":
            self.advance_migration()
        elif phase == "monitor":
            self.show_monitoring_status()
        elif phase == "rollback":
            self.rollback_migration()
        elif phase == "health":
            self.show_monitoring_status()
        else:
            # Run all phases
            self.setup_environment()
            time.sleep(1)

            self.test_settings_migration()
            time.sleep(1)

            self.test_database_migration()
            time.sleep(1)

            self.advance_migration()
            time.sleep(1)

            self.show_monitoring_status()
            time.sleep(1)

        logger.info("=" * 50)
        logger.info("✅ Migration Demo Complete")
        logger.info(f"Results: {len(self.results)} steps executed")

        return len([r for r in self.results if r["status"] == "ERROR"]) == 0


def main():
    """Main entry point."""
    demo = MigrationDemo()

    # Get phase from command line argument
    phase = "all"
    if len(sys.argv) > 1:
        phase = sys.argv[1].lower()

    # Run demo
    success = demo.run_demo(phase)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
