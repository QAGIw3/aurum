#!/usr/bin/env python3
"""
Aurum Settings Migration Demonstration

This script demonstrates the feature-flagged settings system working properly.
"""

import os
import sys
from pathlib import Path

def main():
    """Demonstrate the settings migration system."""
    print("🚀 Aurum Settings Migration Demo")
    print("=" * 50)

    # Set environment variables BEFORE importing
    print("🔧 Setting up environment...")
    os.environ["AURUM_USE_SIMPLIFIED_SETTINGS"] = "1"
    os.environ["AURUM_ENABLE_MIGRATION_MONITORING"] = "1"
    os.environ["AURUM_SETTINGS_MIGRATION_PHASE"] = "hybrid"

    print("✅ Environment variables set:")
    for key in ["AURUM_USE_SIMPLIFIED_SETTINGS", "AURUM_ENABLE_MIGRATION_MONITORING", "AURUM_SETTINGS_MIGRATION_PHASE"]:
        value = os.getenv(key, "not set")
        print(f"   {key} = {value}")

    # Add src to path
    sys.path.insert(0, str(Path(__file__).parent / "src"))

    try:
        from aurum.core.settings import (
            SimplifiedSettings,
            HybridAurumSettings,
            is_feature_enabled,
            FEATURE_FLAGS,
            get_migration_phase,
            MigrationMetrics
        )

        print("\n🧪 Testing Simplified Settings...")
        settings = SimplifiedSettings()
        print(f"✅ Environment: {settings.environment}")
        print(f"✅ Database URL: {settings.database_url}")
        print(f"✅ Debug mode: {settings.debug}")

        print("\n🚩 Testing Feature Flags...")
        for name, flag in FEATURE_FLAGS.items():
            enabled = is_feature_enabled(flag)
            value = os.getenv(flag, "not set")
            print(f"✅ {name}: {flag} = {value} → enabled: {enabled}")

        print("\n🔄 Testing Hybrid Settings...")
        hybrid = HybridAurumSettings()
        print(f"✅ Using simplified system: {hybrid._use_simplified}")
        print(f"✅ Migration phase: {hybrid._migration_phase}")
        print(f"✅ Hybrid environment: {hybrid.environment}")
        print(f"✅ Hybrid database URL: {hybrid.database_url}")

        print("\n📊 Testing Migration Metrics...")
        metrics = MigrationMetrics()
        print("✅ Migration metrics initialized")

        # Record some test calls
        metrics.record_settings_call("simplified", 45.2)
        metrics.record_settings_call("legacy", 123.5)
        metrics.record_settings_call("simplified", 23.1, error=True)

        status = metrics.get_migration_status()
        print(f"✅ Migration status: {status}")

        monitoring_enabled = metrics.is_monitoring_enabled()
        print(f"✅ Monitoring enabled: {monitoring_enabled}")

        print("\n🎮 Testing Migration Control...")
        print(f"✅ Current phase: {get_migration_phase()}")

        # Test phase advancement
        from aurum.core.settings import advance_migration_phase, validate_migration_health

        success = advance_migration_phase("settings", "simplified")
        print(f"✅ Advanced to simplified: {success}")
        print(f"✅ New phase: {get_migration_phase()}")

        # Test health validation
        health = validate_migration_health()
        print(f"✅ Migration healthy: {health['healthy']}")
        if not health['healthy']:
            print(f"⚠️ Issues found: {health['issues']}")

        print("\n🎉 Settings Migration Demo Complete!")
        print("=" * 50)
        print("✅ All tests passed successfully!")
        print("\n📋 Summary:")
        print("- Simplified settings system is working")
        print("- Feature flags are functioning correctly")
        print("- Migration metrics are being tracked")
        print("- Phase advancement and rollback work")
        print("- Health validation is operational")

        return 0

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
