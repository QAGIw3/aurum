#!/usr/bin/env python3
"""
Simple test for the migration system components
"""

import os
import sys
import time
import json
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Set environment variables before importing
os.environ["AURUM_USE_SIMPLIFIED_SETTINGS"] = "1"
os.environ["AURUM_ENABLE_MIGRATION_MONITORING"] = "1"
os.environ["AURUM_SETTINGS_MIGRATION_PHASE"] = "hybrid"

def test_settings_system():
    """Test the simplified settings system."""
    print("🧪 Testing Settings System...")

    try:
        from aurum.core.settings import (
            SimplifiedSettings,
            HybridAurumSettings,
            is_feature_enabled,
            FEATURE_FLAGS
        )
        print("✅ Settings system imported successfully")

        # Test simplified settings
        settings = SimplifiedSettings()
        print(f"✅ Environment: {settings.environment}")
        print(f"✅ Debug mode: {settings.debug}")
        print(f"✅ Database URL: {settings.database_url}")

        # Test feature flags
        print("✅ Feature flags available:")
        for name, flag in FEATURE_FLAGS.items():
            value = os.getenv(flag, "not set")
            print(f"   {name}: {flag} = {value}")

        # Test hybrid settings
        hybrid_settings = HybridAurumSettings()
        print(f"✅ Hybrid settings created, using simplified: {hybrid_settings._use_simplified}")

        return True
    except Exception as e:
        print(f"❌ Settings test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_migration_metrics():
    """Test migration metrics tracking."""
    print("\n📊 Testing Migration Metrics...")

    try:
        from aurum.core.settings import MigrationMetrics

        # Create metrics instance
        metrics = MigrationMetrics()
        print("✅ Migration metrics initialized")

        # Test recording calls
        metrics.record_settings_call("simplified", 45.2)
        metrics.record_settings_call("legacy", 123.5)
        metrics.record_settings_call("simplified", 23.1, error=True)

        print("✅ Metrics recorded successfully")

        # Test status
        status = metrics.get_migration_status()
        print(f"✅ Migration status: {json.dumps(status, indent=2)}")

        return True
    except Exception as e:
        print(f"❌ Metrics test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_feature_flags():
    """Test feature flag functionality."""
    print("\n🚩 Testing Feature Flags...")

    try:
        from aurum.core.settings import is_feature_enabled, FEATURE_FLAGS

        # Test flag checking (already set at module level)
        for name, flag in FEATURE_FLAGS.items():
            enabled = is_feature_enabled(flag)
            value = os.getenv(flag, "not set")
            print(f"✅ {name}: {flag} = {value} → enabled: {enabled}")

        return True
    except Exception as e:
        print(f"❌ Feature flags test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("🚀 Aurum Migration System Test")
    print("=" * 50)

    tests = [
        test_settings_system,
        test_migration_metrics,
        test_feature_flags
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
        print()

    print("=" * 50)
    passed = sum(results)
    total = len(results)

    if passed == total:
        print(f"✅ All tests passed ({passed}/{total})")
        return 0
    else:
        print(f"❌ Some tests failed ({passed}/{total})")
        return 1

if __name__ == "__main__":
    sys.exit(main())
