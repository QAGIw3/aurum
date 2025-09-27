#!/usr/bin/env python3
"""
Demo script showcasing the enhanced V1 split flag system with runtime toggles.

This script demonstrates the key capabilities of the enhanced V1 split flag system:
- Runtime flag toggling without service restart
- Environment variable synchronization  
- API management capabilities
- Backward compatibility
- Error handling and recovery
"""

import asyncio
import os
import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from aurum.api.features.v1_split_integration import (
    get_v1_split_manager,
    initialize_v1_split_flags
)
from aurum.api.features.feature_flags import (
    InMemoryFeatureFlagStore,
    FeatureFlagManager
)


async def demo_runtime_toggles():
    """Demonstrate runtime flag toggling."""
    print("\n🚀 Demo: Runtime Flag Toggling")
    print("=" * 50)
    
    # Get the V1 split manager
    manager = get_v1_split_manager()
    
    # Initialize with some environment setup
    os.environ["AURUM_API_V1_SPLIT_EIA"] = "0"  # Start disabled
    await manager.initialize_v1_flags()
    
    print("Initial state:")
    status = await manager.get_v1_split_status()
    for flag, info in status.items():
        state = "🟢 ENABLED" if info["enabled"] else "🔴 DISABLED"
        print(f"  {flag}: {state}")
    
    print("\n⚡ Enabling EIA flag at runtime...")
    success = await manager.set_v1_split_enabled("eia", True, "demo_user")
    if success:
        print("✅ EIA flag enabled successfully!")
        print(f"   Environment variable synced: AURUM_API_V1_SPLIT_EIA={os.getenv('AURUM_API_V1_SPLIT_EIA')}")
    
    print("\n⚡ Enabling Drought flag at runtime...")
    success = await manager.set_v1_split_enabled("drought", True, "demo_user")
    if success:
        print("✅ Drought flag enabled successfully!")
    
    print("\nCurrent state after runtime changes:")
    status = await manager.get_v1_split_status()
    enabled_count = sum(1 for info in status.values() if info["enabled"])
    print(f"📊 {enabled_count}/{len(status)} flags are enabled")
    
    for flag, info in status.items():
        if info["enabled"]:
            state = "🟢 ENABLED"
            if info.get("last_updated"):
                state += f" (updated: {info['last_updated'][:19]})"
        else:
            state = "🔴 DISABLED"
        print(f"  {flag}: {state}")


async def demo_backward_compatibility():
    """Demonstrate backward compatibility with environment variables."""
    print("\n🔄 Demo: Backward Compatibility")
    print("=" * 50)
    
    # Set flags the traditional way
    print("Setting flags via environment variables (traditional method):")
    flags_to_set = {
        "AURUM_API_V1_SPLIT_ISO": "1",
        "AURUM_API_V1_SPLIT_ADMIN": "1",
    }
    
    for env_var, value in flags_to_set.items():
        os.environ[env_var] = value
        print(f"  export {env_var}={value}")
    
    # Create fresh manager to simulate service restart
    print("\n🔄 Simulating service restart...")
    store = InMemoryFeatureFlagStore()
    feature_manager = FeatureFlagManager(store)
    manager = get_v1_split_manager()
    manager.feature_manager = feature_manager
    
    await manager.initialize_v1_flags()
    print("✅ Service restarted, flags initialized from environment")
    
    # Show that environment flags are reflected
    status = await manager.get_v1_split_status()
    print("\nFlags loaded from environment:")
    for flag, info in status.items():
        if info["enabled"]:
            source = "env var" if info.get("status") != "not_in_feature_system" else "default"
            print(f"  🟢 {flag}: ENABLED (from {source})")
    
    print("\n✨ Now these can be controlled at runtime too!")
    success = await manager.set_v1_split_enabled("iso", False, "demo_user")
    if success:
        print("  📝 ISO flag disabled via runtime API")
        print(f"  🔄 Environment variable synced: AURUM_API_V1_SPLIT_ISO={os.getenv('AURUM_API_V1_SPLIT_ISO')}")


async def demo_error_handling():
    """Demonstrate error handling and recovery."""
    print("\n🛡️ Demo: Error Handling & Recovery")
    print("=" * 50)
    
    manager = get_v1_split_manager()
    
    print("Testing error scenarios:")
    
    # Test unknown flag
    print("\n1. Attempting to toggle unknown flag...")
    success = await manager.set_v1_split_enabled("unknown_flag", True)
    if not success:
        print("   ✅ Gracefully handled unknown flag (returned False)")
    
    # Test flag checking
    print("\n2. Checking flag status...")
    is_enabled = await manager.is_v1_split_enabled("eia")
    print(f"   ✅ EIA flag check successful: {is_enabled}")
    
    # Test recovery
    print("\n3. System continues working after errors...")
    status = await manager.get_v1_split_status()
    print(f"   ✅ Status retrieval successful: {len(status)} flags found")


async def demo_api_integration():
    """Demonstrate integration with feature flag API."""
    print("\n🔌 Demo: Feature Flag API Integration")
    print("=" * 50)
    
    manager = get_v1_split_manager()
    await manager.initialize_v1_flags()
    
    print("V1 split flags in the broader feature flag system:")
    
    # List all flags including V1 splits
    all_flags = await manager.feature_manager.list_flags()
    v1_flags = [flag for flag in all_flags if flag.key.startswith("v1_split_")]
    
    print(f"\n📋 Found {len(v1_flags)} V1 split flags:")
    for flag in v1_flags:
        status_icon = "🟢" if flag.status.value == "enabled" else "🔴"
        print(f"  {status_icon} {flag.name}")
        print(f"     Key: {flag.key}")
        print(f"     Tags: {', '.join(flag.tags)}")
        print(f"     Created: {flag.created_at}")
        print()
    
    # Show feature flag statistics
    stats = await manager.feature_manager.get_feature_stats()
    print(f"📊 Feature Flag System Stats:")
    print(f"   Total flags: {stats.get('total_flags', 'N/A')}")
    print(f"   Enabled flags: {stats.get('enabled_flags', 'N/A')}")


def demo_api_endpoints():
    """Show example API calls (simulated)."""
    print("\n🌐 Demo: API Endpoints Usage")
    print("=" * 50)
    
    base_url = "https://api.example.com"
    
    print("Example API calls for V1 split flag management:")
    print()
    
    api_examples = [
        {
            "title": "List all V1 split flags",
            "method": "GET",
            "url": f"{base_url}/v1/admin/features/v1-splits",
            "description": "Get status of all V1 split flags"
        },
        {
            "title": "Get specific flag details", 
            "method": "GET",
            "url": f"{base_url}/v1/admin/features/v1-splits/eia",
            "description": "Get detailed information about EIA flag"
        },
        {
            "title": "Enable EIA flag",
            "method": "PUT", 
            "url": f"{base_url}/v1/admin/features/v1-splits/eia",
            "body": "true",
            "description": "Enable EIA router at runtime"
        },
        {
            "title": "Disable ISO flag",
            "method": "PUT",
            "url": f"{base_url}/v1/admin/features/v1-splits/iso", 
            "body": "false",
            "description": "Disable ISO router at runtime"
        }
    ]
    
    for i, example in enumerate(api_examples, 1):
        print(f"{i}. {example['title']}")
        print(f"   {example['method']} {example['url']}")
        if 'body' in example:
            print(f"   Body: {example['body']}")
        print(f"   → {example['description']}")
        print()


async def demo_real_world_scenario():
    """Demonstrate a real-world deployment scenario."""
    print("\n🌍 Demo: Real-World Deployment Scenario")
    print("=" * 50)
    
    print("Scenario: Gradual rollout of new EIA API router")
    print()
    
    manager = get_v1_split_manager()
    await manager.initialize_v1_flags()
    
    # Step 1: Start with monolith (flag disabled)
    print("Step 1: Production starts with monolith handling EIA requests")
    await manager.set_v1_split_enabled("eia", False, "deployment_system")
    print("   🔴 EIA split router: DISABLED (monolith handles requests)")
    
    # Step 2: Enable for staging/testing
    print("\nStep 2: Enable split router in staging for testing")
    await manager.set_v1_split_enabled("eia", True, "devops_team")
    print("   🟢 EIA split router: ENABLED")
    print("   📊 Testing parity between monolith and split router...")
    
    # Step 3: Monitoring shows good results
    print("\nStep 3: Monitoring shows split router is performing well")
    print("   ✅ Response times improved")
    print("   ✅ Error rates unchanged")
    print("   ✅ Feature parity validated")
    
    # Step 4: Emergency rollback simulation
    print("\nStep 4: (Hypothetical) Emergency detected - immediate rollback")
    await manager.set_v1_split_enabled("eia", False, "oncall_engineer")
    print("   🚨 EIA split router: DISABLED (rolled back to monolith)")
    print("   ⚡ Change took effect immediately - no service restart needed")
    
    # Step 5: Re-enable after fix
    print("\nStep 5: Issue fixed, re-enable split router")
    await manager.set_v1_split_enabled("eia", True, "devops_team")
    print("   🟢 EIA split router: ENABLED")
    print("   🎯 Gradual rollout successful!")
    
    print("\n💡 Key Benefits Demonstrated:")
    print("   • Zero-downtime flag changes")
    print("   • Immediate rollback capability")
    print("   • Audit trail of who changed what")
    print("   • Environment variable sync for external systems")


async def main():
    """Run all demonstrations."""
    print("🎬 V1 Split Flag Enhancement System Demo")
    print("=" * 60)
    print("This demo showcases the enhanced V1 split flag system with:")
    print("• Runtime toggle capabilities")
    print("• API management endpoints")
    print("• Backward compatibility")
    print("• Error handling & recovery")
    print("• Real-world deployment scenarios")
    
    try:
        await demo_runtime_toggles()
        await demo_backward_compatibility()
        await demo_error_handling()
        await demo_api_integration()
        demo_api_endpoints()
        await demo_real_world_scenario()
        
        print("\n🎉 Demo Complete!")
        print("=" * 60)
        print("The enhanced V1 split flag system provides powerful runtime")
        print("control while maintaining full backward compatibility.")
        print("\nKey capabilities demonstrated:")
        print("✅ Runtime flag toggling without service restart")
        print("✅ Environment variable synchronization")
        print("✅ API endpoints for management")
        print("✅ Graceful error handling")
        print("✅ Real-world deployment scenarios")
        print("✅ Integration with advanced feature flag system")
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())