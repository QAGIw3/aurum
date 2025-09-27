"""Cache initialization utilities for application startup.

This module provides utilities to initialize the unified cache manager 
during application startup and configure it properly.
"""

from __future__ import annotations

import asyncio
from typing import Optional, Dict, Any

from ...logging.structured_logger import get_logger
from .config import CacheConfig
from .cache import AsyncCache, CacheBackend
from .unified_cache_manager import UnifiedCacheManager, set_unified_cache_manager
from ...core.settings import get_settings


logger = get_logger(__name__)


async def initialize_unified_cache_system(
    config: Optional[CacheConfig] = None,
    backend: CacheBackend = CacheBackend.HYBRID
) -> UnifiedCacheManager:
    """Initialize the unified cache system.
    
    This function should be called during application startup to set up
    the unified cache manager with proper configuration.
    
    Args:
        config: Cache configuration. If None, will create from settings.
        backend: Cache backend to use.
        
    Returns:
        Initialized UnifiedCacheManager instance.
    """
    logger.info("Initializing unified cache system")
    
    try:
        # Create configuration if not provided
        if config is None:
            settings = get_settings()
            config = CacheConfig.from_settings(settings)
            
        # Create AsyncCache instance
        cache = AsyncCache(config, backend)
        
        # Create UnifiedCacheManager
        unified_manager = UnifiedCacheManager(config)
        
        # Initialize the manager
        await unified_manager.initialize(cache)
        
        # Set as global instance
        set_unified_cache_manager(unified_manager)
        
        logger.info(
            "Unified cache system initialized successfully",
            extra={
                "backend": backend.value,
                "ttl_seconds": config.ttl_seconds,
                "redis_enabled": config.redis_host is not None
            }
        )
        
        return unified_manager
        
    except Exception as e:
        logger.error(f"Failed to initialize unified cache system: {e}")
        raise


async def shutdown_unified_cache_system(unified_manager: Optional[UnifiedCacheManager] = None) -> None:
    """Shutdown the unified cache system gracefully.
    
    This function should be called during application shutdown to clean up
    cache resources properly.
    
    Args:
        unified_manager: Manager to shutdown. If None, uses global instance.
    """
    logger.info("Shutting down unified cache system")
    
    try:
        if unified_manager is None:
            from .unified_cache_manager import get_unified_cache_manager
            unified_manager = get_unified_cache_manager()
            
        if unified_manager:
            # Use context manager for proper cleanup
            async with unified_manager:
                pass  # Context manager handles cleanup
                
        # Clear global instance
        set_unified_cache_manager(None)
        
        logger.info("Unified cache system shutdown complete")
        
    except Exception as e:
        logger.error(f"Error during cache system shutdown: {e}")


def create_cache_config_from_env() -> CacheConfig:
    """Create cache configuration from environment/settings.
    
    This is a convenience function that creates a CacheConfig from
    the current application settings.
    
    Returns:
        Configured CacheConfig instance.
    """
    try:
        settings = get_settings()
        return CacheConfig.from_settings(settings)
    except Exception as e:
        logger.warning(f"Failed to load settings, using default cache config: {e}")
        return CacheConfig()


async def migrate_existing_cache_data(
    old_cache: AsyncCache,
    new_unified_manager: UnifiedCacheManager,
    namespace_mapping: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Migrate data from existing cache to unified cache manager.
    
    This utility helps migrate cache data when switching from old cache
    implementations to the unified cache manager.
    
    Args:
        old_cache: The old cache instance to migrate from.
        new_unified_manager: The new unified cache manager.
        namespace_mapping: Optional mapping of old keys to new namespaces.
        
    Returns:
        Migration statistics.
    """
    logger.info("Starting cache data migration")
    
    migration_stats = {
        "total_keys": 0,
        "migrated_keys": 0,
        "failed_keys": 0,
        "skipped_keys": 0,
        "errors": []
    }
    
    try:
        # This is a simplified migration - in practice, you'd need to
        # implement proper key enumeration based on your cache backend
        logger.warning("Cache migration is a placeholder - implement based on your cache backend")
        
        # For now, just log the attempt
        migration_stats["total_keys"] = 0
        migration_stats["migrated_keys"] = 0
        
        logger.info(
            "Cache data migration completed",
            extra=migration_stats
        )
        
    except Exception as e:
        logger.error(f"Cache migration failed: {e}")
        migration_stats["errors"].append(str(e))
        
    return migration_stats


async def validate_cache_system() -> Dict[str, Any]:
    """Validate that the cache system is working correctly.
    
    This function performs basic health checks on the cache system
    to ensure it's working properly.
    
    Returns:
        Validation results.
    """
    from .unified_cache_manager import get_unified_cache_manager
    
    validation_results = {
        "unified_manager_available": False,
        "cache_backend_accessible": False,
        "governance_system_active": False,
        "test_operations_successful": False,
        "errors": []
    }
    
    try:
        # Check if unified manager is available
        unified_manager = get_unified_cache_manager()
        if unified_manager:
            validation_results["unified_manager_available"] = True
            
            # Check if cache backend is accessible
            if unified_manager.cache:
                validation_results["cache_backend_accessible"] = True
                
                # Test basic operations
                test_key = "cache_validation_test"
                test_value = {"test": "data", "timestamp": asyncio.get_event_loop().time()}
                
                try:
                    await unified_manager.set(test_key, test_value, ttl=10)
                    retrieved_value = await unified_manager.get(test_key)
                    
                    if retrieved_value == test_value:
                        validation_results["test_operations_successful"] = True
                        
                    # Clean up test data
                    await unified_manager.delete(test_key)
                    
                except Exception as e:
                    validation_results["errors"].append(f"Test operations failed: {e}")
                    
            # Check governance system
            if unified_manager.governance_manager:
                validation_results["governance_system_active"] = True
                
        else:
            validation_results["errors"].append("Unified cache manager not initialized")
            
    except Exception as e:
        validation_results["errors"].append(f"Validation failed: {e}")
        
    logger.info(
        "Cache system validation completed",
        extra=validation_results
    )
    
    return validation_results


__all__ = [
    "initialize_unified_cache_system",
    "shutdown_unified_cache_system", 
    "create_cache_config_from_env",
    "migrate_existing_cache_data",
    "validate_cache_system"
]