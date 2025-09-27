"""Backward compatibility facades for existing cache implementations.

This module provides compatibility wrappers that allow existing code to 
continue working while gradually migrating to the unified cache manager.
"""

from __future__ import annotations

import warnings
from typing import Any, Dict, Optional, Union

from .unified_cache_manager import UnifiedCacheManager, get_unified_cache_manager
from .cache import AsyncCache, CacheManager as OriginalCacheManager
from .enhanced_cache_manager import EnhancedCacheManager as OriginalEnhancedCacheManager
from .cache_governance import CacheNamespace


class CacheManagerCompat(OriginalCacheManager):
    """Backward-compatible CacheManager that delegates to UnifiedCacheManager."""
    
    def __init__(self, cache_service: Optional[AsyncCache] = None):
        """Initialize with optional cache service."""
        # Don't call super().__init__ to avoid duplicate initialization
        self._cache = cache_service
        self._unified_manager = get_unified_cache_manager()
        
        if not self._unified_manager and cache_service:
            # Create a unified manager if none exists
            from .unified_cache_manager import UnifiedCacheManager, set_unified_cache_manager
            self._unified_manager = UnifiedCacheManager()
            # Initialize it asynchronously when first used
            
    async def _ensure_unified_manager(self):
        """Ensure unified manager is initialized."""
        if not self._unified_manager:
            from .unified_cache_manager import UnifiedCacheManager, set_unified_cache_manager
            self._unified_manager = UnifiedCacheManager()
            set_unified_cache_manager(self._unified_manager)
            
        if self._cache and not self._unified_manager.cache:
            await self._unified_manager.initialize(self._cache)

    async def get_curve_data(self, iso: str, market: str, location: str, asof: Optional[str] = None) -> Optional[Any]:
        """Get cached curve data."""
        await self._ensure_unified_manager()
        if self._unified_manager:
            return await self._unified_manager.get_curve_data(iso, market, location, asof)
        # Fallback to original implementation
        return await super().get_curve_data(iso, market, location, asof)

    async def cache_curve_data(self, data: Any, iso: str, market: str, location: str, 
                             asof: Optional[str] = None, ttl: Optional[int] = None) -> None:
        """Cache curve data."""
        await self._ensure_unified_manager()
        if self._unified_manager:
            await self._unified_manager.cache_curve_data(data, iso, market, location, asof, ttl)
        else:
            # Fallback to original implementation
            await super().cache_curve_data(data, iso, market, location, asof, ttl)

    async def get_metadata(self, metadata_type: str, **filters) -> Optional[Any]:
        """Get cached metadata."""
        await self._ensure_unified_manager()
        if self._unified_manager:
            return await self._unified_manager.get_metadata(metadata_type, **filters)
        # Fallback to original implementation
        return await super().get_metadata(metadata_type, **filters)

    async def cache_metadata(self, data: Any, metadata_type: str, ttl: Optional[int] = None, **filters) -> None:
        """Cache metadata."""
        await self._ensure_unified_manager()
        if self._unified_manager:
            await self._unified_manager.cache_metadata(data, metadata_type, ttl, **filters)
        else:
            # Fallback to original implementation
            await super().cache_metadata(data, metadata_type, ttl, **filters)

    async def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate cache entries matching a pattern."""
        await self._ensure_unified_manager()
        if self._unified_manager:
            await self._unified_manager.invalidate_pattern(pattern)
        else:
            # Fallback to original implementation
            await super().invalidate_pattern(pattern)

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        await self._ensure_unified_manager()
        if self._unified_manager:
            return await self._unified_manager.get_stats()
        # Fallback to original implementation
        return await super().get_cache_stats()

    async def get_cache_entry(self, key: str) -> Optional[Any]:
        """Get cache entry (alias for get method)."""
        await self._ensure_unified_manager()
        if self._unified_manager:
            return await self._unified_manager.get_cache_entry(key)
        # Fallback to original implementation
        return await super().get_cache_entry(key)

    async def set_cache_entry(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Set cache entry (alias for set method)."""
        await self._ensure_unified_manager()
        if self._unified_manager:
            await self._unified_manager.set_cache_entry(key, value, ttl_seconds)
        else:
            # Fallback to original implementation
            await super().set_cache_entry(key, value, ttl_seconds)


class EnhancedCacheManagerCompat:
    """Backward-compatible EnhancedCacheManager that delegates to UnifiedCacheManager."""
    
    def __init__(self):
        """Initialize the compatibility wrapper."""
        self._unified_manager = get_unified_cache_manager()
        self.cache: Optional[AsyncCache] = None
        
        # Emit deprecation warning
        warnings.warn(
            "EnhancedCacheManager is deprecated. Use UnifiedCacheManager instead.",
            DeprecationWarning,
            stacklevel=2
        )
        
    async def initialize(self, cache: AsyncCache):
        """Initialize with cache."""
        self.cache = cache
        
        if not self._unified_manager:
            from .unified_cache_manager import UnifiedCacheManager, set_unified_cache_manager
            self._unified_manager = UnifiedCacheManager()
            set_unified_cache_manager(self._unified_manager)
            
        await self._unified_manager.initialize(cache)

    async def get(self, namespace: Union[CacheNamespace, str], key: str, **kwargs) -> Optional[Any]:
        """Get value from cache."""
        if self._unified_manager:
            return await self._unified_manager.get(key, namespace)
        return None

    async def set(self, namespace: Union[CacheNamespace, str], key: str, value: Any, 
                 ttl: Optional[int] = None, **kwargs) -> None:
        """Set value in cache."""
        if self._unified_manager:
            await self._unified_manager.set(key, value, ttl, namespace)

    async def delete(self, namespace: Union[CacheNamespace, str], key: str, **kwargs) -> None:
        """Delete value from cache."""
        if self._unified_manager:
            await self._unified_manager.delete(key, namespace)

    async def invalidate_namespace(self, namespace: Union[CacheNamespace, str]) -> int:
        """Invalidate all entries in a namespace."""
        if self._unified_manager:
            await self._unified_manager.clear(namespace)
            return 1  # Return dummy count
        return 0

    def _get_namespace_ttl(self, namespace: Union[CacheNamespace, str]) -> int:
        """Get TTL for a namespace."""
        if self._unified_manager:
            return self._unified_manager._get_namespace_ttl(namespace)
        return 300  # Default 5 minutes


def create_compatibility_cache_manager(cache_service: AsyncCache) -> CacheManagerCompat:
    """Create a backward-compatible cache manager.
    
    This function helps with the migration by creating a compatibility wrapper
    around the new unified cache manager.
    """
    return CacheManagerCompat(cache_service)


def create_compatibility_enhanced_manager() -> EnhancedCacheManagerCompat:
    """Create a backward-compatible enhanced cache manager.
    
    This function helps with the migration by creating a compatibility wrapper
    around the new unified cache manager.
    """
    return EnhancedCacheManagerCompat()


# Migration utilities
def migrate_to_unified_cache_manager(old_manager: Union[OriginalCacheManager, OriginalEnhancedCacheManager]) -> UnifiedCacheManager:
    """Migrate from old cache manager to unified cache manager.
    
    This utility function helps migrate existing cache manager instances
    to the new unified cache manager.
    """
    warnings.warn(
        "Migrating to UnifiedCacheManager. Update your code to use UnifiedCacheManager directly.",
        DeprecationWarning,
        stacklevel=2
    )
    
    unified_manager = get_unified_cache_manager()
    if not unified_manager:
        from .unified_cache_manager import UnifiedCacheManager, set_unified_cache_manager
        unified_manager = UnifiedCacheManager()
        set_unified_cache_manager(unified_manager)
        
        # If the old manager has a cache instance, use it
        if hasattr(old_manager, '_cache') and old_manager._cache:
            # This will need to be awaited in an async context
            pass
            
    return unified_manager


__all__ = [
    "CacheManagerCompat",
    "EnhancedCacheManagerCompat", 
    "create_compatibility_cache_manager",
    "create_compatibility_enhanced_manager",
    "migrate_to_unified_cache_manager"
]