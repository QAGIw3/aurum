"""Example showing how to migrate from scattered cache implementations to UnifiedCacheManager.

This example demonstrates the migration process and shows the benefits of 
the unified cache manager approach.
"""

import asyncio
from typing import Dict, Any, Optional

# Old imports (being replaced)
from aurum.api.cache.cache import CacheManager, AsyncCache, CacheBackend
from aurum.api.cache.enhanced_cache_manager import EnhancedCacheManager
from aurum.api.cache.cache_governance import CacheNamespace

# New unified imports
from aurum.api.cache.unified_cache_manager import UnifiedCacheManager
from aurum.api.cache.initialization import initialize_unified_cache_system
from aurum.api.cache.compatibility import CacheManagerCompat
from aurum.api.cache.config import CacheConfig


async def old_cache_usage_example():
    """Example of how caching was done with the old scattered approach."""
    print("=== Old Scattered Cache Approach ===")
    
    # Had to manage multiple cache instances
    config = CacheConfig()
    async_cache = AsyncCache(config, CacheBackend.MEMORY)
    cache_manager = CacheManager(async_cache)
    enhanced_manager = EnhancedCacheManager()
    await enhanced_manager.initialize(async_cache)
    
    # Different APIs for different use cases
    await cache_manager.cache_curve_data(
        {"values": [1, 2, 3]}, "CAISO", "RT", "ZONE1", ttl=300
    )
    
    curve_data = await cache_manager.get_curve_data("CAISO", "RT", "ZONE1")
    print(f"Retrieved curve data: {curve_data}")
    
    # Different namespace handling
    await enhanced_manager.set(CacheNamespace.METADATA, "test_key", {"meta": "data"})
    metadata = await enhanced_manager.get(CacheNamespace.METADATA, "test_key")
    print(f"Retrieved metadata: {metadata}")
    
    # Inconsistent TTL policies
    print("Old approach had inconsistent TTL policies across different managers")


async def new_unified_cache_example():
    """Example of the new unified cache approach."""
    print("\n=== New Unified Cache Approach ===")
    
    # Single initialization
    unified_manager = await initialize_unified_cache_system(backend=CacheBackend.MEMORY)
    
    # Consistent API for all operations
    await unified_manager.cache_curve_data(
        {"values": [1, 2, 3]}, "CAISO", "RT", "ZONE1"
        # TTL automatically determined by governance policies
    )
    
    curve_data = await unified_manager.get_curve_data("CAISO", "RT", "ZONE1")
    print(f"Retrieved curve data: {curve_data}")
    
    # Unified namespace handling with governance
    await unified_manager.set(
        "test_key", 
        {"meta": "data"}, 
        namespace=CacheNamespace.METADATA
        # TTL automatically set based on namespace policy
    )
    
    metadata = await unified_manager.get("test_key", namespace=CacheNamespace.METADATA)
    print(f"Retrieved metadata: {metadata}")
    
    # Comprehensive analytics
    stats = await unified_manager.get_stats()
    print(f"Cache statistics: {stats['unified_cache']}")
    
    # Advanced features
    await unified_manager.warm_cache(
        CacheNamespace.METADATA,
        async_warm_function
    )
    
    print("New approach provides consistent TTL policies via governance system")


async def async_warm_function() -> Dict[str, Any]:
    """Example warming function."""
    return {
        "warm_key_1": {"data": "warmed_value_1"},
        "warm_key_2": {"data": "warmed_value_2"}
    }


async def migration_compatibility_example():
    """Example showing backward compatibility during migration."""
    print("\n=== Migration with Backward Compatibility ===")
    
    # Initialize unified cache system
    unified_manager = await initialize_unified_cache_system(backend=CacheBackend.MEMORY)
    
    # Old code can continue working with compatibility layer
    config = CacheConfig()
    async_cache = AsyncCache(config, CacheBackend.MEMORY)
    compat_manager = CacheManagerCompat(async_cache)
    
    # This uses the unified manager under the hood
    await compat_manager.cache_curve_data(
        {"legacy": "data"}, "PJM", "DA", "AEP", ttl=600
    )
    
    # Can retrieve using either interface
    legacy_data = await compat_manager.get_curve_data("PJM", "DA", "AEP")
    unified_data = await unified_manager.get_curve_data("PJM", "DA", "AEP")
    
    print(f"Legacy interface: {legacy_data}")
    print(f"Unified interface: {unified_data}")
    print("Both interfaces access the same underlying cache!")


async def governance_and_ttl_example():
    """Example showing governance and TTL policy benefits."""
    print("\n=== Governance and TTL Policy Benefits ===")
    
    unified_manager = await initialize_unified_cache_system(backend=CacheBackend.MEMORY)
    
    # TTL automatically determined by namespace
    await unified_manager.set("curve_key", {"data": "curve"}, namespace=CacheNamespace.CURVES)
    await unified_manager.set("metadata_key", {"data": "metadata"}, namespace=CacheNamespace.METADATA)
    await unified_manager.set("config_key", {"data": "config"}, namespace=CacheNamespace.SYSTEM_CONFIG)
    
    # Different TTLs applied automatically:
    # - CURVES: 5 minutes (300 seconds)
    # - METADATA: 30 minutes (1800 seconds) 
    # - SYSTEM_CONFIG: 24 hours (86400 seconds)
    
    print("TTL policies automatically applied based on data type:")
    print("- Curves: 5 minutes (frequently changing)")
    print("- Metadata: 30 minutes (semi-static)")
    print("- System Config: 24 hours (rarely changing)")
    
    # Can override when needed
    await unified_manager.set(
        "urgent_curve", 
        {"data": "urgent"}, 
        ttl=30,  # Override to 30 seconds
        namespace=CacheNamespace.CURVES
    )
    
    print("TTL can still be overridden when needed for specific use cases")


async def analytics_and_monitoring_example():
    """Example showing enhanced analytics and monitoring."""
    print("\n=== Analytics and Monitoring Benefits ===")
    
    unified_manager = await initialize_unified_cache_system(backend=CacheBackend.MEMORY)
    
    # Perform various operations
    await unified_manager.set("key1", "value1", namespace=CacheNamespace.CURVES)
    await unified_manager.set("key2", "value2", namespace=CacheNamespace.METADATA)
    
    # Generate some hits and misses
    await unified_manager.get("key1", namespace=CacheNamespace.CURVES)  # hit
    await unified_manager.get("key2", namespace=CacheNamespace.METADATA)  # hit
    await unified_manager.get("nonexistent", namespace=CacheNamespace.CURVES)  # miss
    
    # Get comprehensive statistics
    stats = await unified_manager.get_stats()
    
    print("Unified analytics provide comprehensive insights:")
    print(f"Total requests: {stats['unified_cache']['total_requests']}")
    print(f"Hit rate: {stats['unified_cache']['hit_rate']}")
    print(f"Average response time: {stats['unified_cache']['average_response_time_ms']}ms")
    
    print("\nNamespace-specific statistics:")
    for namespace, ns_stats in stats['unified_cache']['namespace_stats'].items():
        if ns_stats['hits'] > 0 or ns_stats['misses'] > 0:
            print(f"- {namespace}: {ns_stats['hits']} hits, {ns_stats['misses']} misses")


async def main():
    """Run all examples."""
    print("Cache Migration Examples")
    print("=" * 50)
    
    await old_cache_usage_example()
    await new_unified_cache_example()
    await migration_compatibility_example()
    await governance_and_ttl_example()
    await analytics_and_monitoring_example()
    
    print("\n" + "=" * 50)
    print("Migration Benefits Summary:")
    print("1. Single unified interface instead of multiple scattered managers")
    print("2. Consistent TTL policies via governance system")
    print("3. Comprehensive analytics and monitoring")
    print("4. Backward compatibility during migration")
    print("5. Advanced features like cache warming and pattern invalidation")
    print("6. Better error handling and logging")
    print("7. Context manager support for resource cleanup")


if __name__ == "__main__":
    asyncio.run(main())