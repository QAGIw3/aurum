#!/usr/bin/env python3
"""
Aurum Migration System Demonstration

This script shows how to properly use the feature-flagged migration system.
"""

import os
import sys
import json
from pathlib import Path

def setup_environment():
    """Setup environment variables for migration."""
    print("🔧 Setting up migration environment...")

    # Set feature flags
    os.environ["AURUM_USE_SIMPLIFIED_SETTINGS"] = "1"
    os.environ["AURUM_ENABLE_MIGRATION_MONITORING"] = "1"
    os.environ["AURUM_SETTINGS_MIGRATION_PHASE"] = "hybrid"
    os.environ["AURUM_USE_SIMPLE_DB_CLIENT"] = "1"
    os.environ["AURUM_DB_MIGRATION_PHASE"] = "hybrid"

    print("✅ Environment variables set:")
    for key, value in [
        ("AURUM_USE_SIMPLIFIED_SETTINGS", "1"),
        ("AURUM_ENABLE_MIGRATION_MONITORING", "1"),
        ("AURUM_SETTINGS_MIGRATION_PHASE", "hybrid"),
        ("AURUM_USE_SIMPLE_DB_CLIENT", "1"),
        ("AURUM_DB_MIGRATION_PHASE", "hybrid")
    ]:
        print(f"   {key} = {value}")

def test_feature_flags():
    """Test feature flag functionality."""
    print("\n🚩 Testing Feature Flags...")

    # Add src to path
    sys.path.insert(0, str(Path(__file__).parent / "src"))

    try:
        from aurum.core.settings import is_feature_enabled, FEATURE_FLAGS

        print("✅ Feature flags checked:")
        for name, flag in FEATURE_FLAGS.items():
            enabled = is_feature_enabled(flag)
            value = os.getenv(flag, "not set")
            print(f"   {name}: {flag} = {value} → enabled: {enabled}")

        # Test database flags
        from aurum.api.database.trino_client import is_db_feature_enabled, DB_FEATURE_FLAGS
        print("✅ Database feature flags:")
        for name, flag in DB_FEATURE_FLAGS.items():
            enabled = is_db_feature_enabled(flag)
            value = os.getenv(flag, "not set")
            print(f"   {name}: {flag} = {value} → enabled: {enabled}")

        return True
    except Exception as e:
        print(f"❌ Feature flags test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_settings_system():
    """Test the settings system with feature flags."""
    print("\n🧪 Testing Settings System...")

    sys.path.insert(0, str(Path(__file__).parent / "src"))

    try:
        from aurum.core.settings import SimplifiedSettings, HybridAurumSettings

        # Test simplified settings
        settings = SimplifiedSettings()
        print(f"✅ Environment: {settings.environment}")
        print(f"✅ Database URL: {settings.database_url}")
        print(f"✅ Debug mode: {settings.debug}")

        # Test hybrid settings
        hybrid = HybridAurumSettings()
        print(f"✅ Hybrid settings using simplified: {hybrid._use_simplified}")
        print(f"✅ Migration phase: {hybrid._migration_phase}")

        # Test settings attributes
        print(f"✅ Hybrid database URL: {hybrid.database_url}")
        print(f"✅ Hybrid environment: {hybrid.environment}")

        return True
    except Exception as e:
        print(f"❌ Settings test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_migration_metrics():
    """Test migration metrics."""
    print("\n📊 Testing Migration Metrics...")

    sys.path.insert(0, str(Path(__file__).parent / "src"))

    try:
        from aurum.core.settings import MigrationMetrics

        metrics = MigrationMetrics()
        print("✅ Migration metrics initialized")

        # Record some calls
        metrics.record_settings_call("simplified", 45.2)
        metrics.record_settings_call("legacy", 123.5)
        metrics.record_settings_call("simplified", 23.1, error=True)

        # Get status
        status = metrics.get_migration_status()
        print(f"✅ Migration status: {json.dumps(status, indent=2)}")

        # Check if monitoring is enabled
        monitoring_enabled = metrics.is_monitoring_enabled()
        print(f"✅ Monitoring enabled: {monitoring_enabled}")

        return True
    except Exception as e:
        print(f"❌ Metrics test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_database_client():
    """Test database client with feature flags."""
    print("\n🗄️ Testing Database Client...")

    sys.path.insert(0, str(Path(__file__).parent / "src"))

    try:
        from aurum.api.database.trino_client import get_trino_client, SimpleTrinoClient

        # Get client (will use simplified based on feature flags)
        client = get_trino_client("test_catalog")
        print(f"✅ Client type: {type(client).__name__}")

        # Check if it's the simple client
        if isinstance(client, SimpleTrinoClient):
            print("✅ Using simplified Trino client")
        else:
            print("⚠️ Using legacy Trino client")

        return True
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def demonstrate_migration_control():
    """Demonstrate migration control functions."""
    print("\n🎮 Testing Migration Control...")

    sys.path.insert(0, str(Path(__file__).parent / "src"))

    try:
        from aurum.core.settings import (
            get_migration_phase,
            advance_migration_phase,
            rollback_migration_phase,
            validate_migration_health
        )

        print(f"✅ Current migration phase: {get_migration_phase()}")

        # Try to advance phase
        success = advance_migration_phase("settings", "simplified")
        print(f"✅ Advanced to simplified: {success}")

        print(f"✅ New migration phase: {get_migration_phase()}")

        # Validate health
        health = validate_migration_health()
        print(f"✅ Migration healthy: {health['healthy']}")
        if not health['healthy']:
            print(f"⚠️ Issues: {health['issues']}")

        # Rollback
        success = rollback_migration_phase("settings")
        print(f"✅ Rollback successful: {success}")

        print(f"✅ Final migration phase: {get_migration_phase()}")

        return True
    except Exception as e:
        print(f"❌ Migration control test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run the complete demonstration."""
    print("🚀 Aurum Migration System Demonstration")
    print("=" * 60)

    # Setup
    setup_environment()

    # Run tests
    tests = [
        test_feature_flags,
        test_settings_system,
        test_migration_metrics,
        test_database_client,
        demonstrate_migration_control
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
            if result:
                print("✅ Test passed")
            else:
                print("❌ Test failed")
        except Exception as e:
            print(f"💥 Test crashed: {e}")
            results.append(False)
        print()

    print("=" * 60)
    passed = sum(results)
    total = len(results)

    if passed == total:
        print(f"🎉 All demonstrations passed ({passed}/{total})")
        print("\n📋 Next Steps:")
        print("1. Run 'python migration_demo.py setup' to configure environment")
        print("2. Use 'python migration_demo.py monitor' to check status")
        print("3. Use 'python migration_demo.py advance' to migrate")
        print("4. Use 'python migration_demo.py rollback' if needed")
        return 0
    else:
        print(f"⚠️ Some demonstrations failed ({passed}/{total})")
        return 1

if __name__ == "__main__":
    sys.exit(main())
