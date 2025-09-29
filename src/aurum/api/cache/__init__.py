"""Cache functionality for the Aurum API - Unified interface for all caching needs."""

# Primary unified cache interface (recommended)
def _load_unified():  # pragma: no cover - lazy import to avoid circulars
    from .consolidated_manager import (
        UnifiedCacheManager,
        get_unified_cache_manager,
        get_cache,
        set_cache,
        invalidate_cache_pattern,
        CacheAnalytics,
        CacheHealth,
        CacheInvalidationEvent,
        CacheWarmingConfig,
        CacheStrategy,
    )
    return (
        UnifiedCacheManager,
        get_unified_cache_manager,
        get_cache,
        set_cache,
        invalidate_cache_pattern,
        CacheAnalytics,
        CacheHealth,
        CacheInvalidationEvent,
        CacheWarmingConfig,
        CacheStrategy,
    )

UnifiedCacheManager, get_unified_cache_manager, get_cache, set_cache, invalidate_cache_pattern, CacheAnalytics, CacheHealth, CacheInvalidationEvent, CacheWarmingConfig, CacheStrategy = _load_unified()

# Backward compatibility imports
from .cache import AsyncCache, CacheBackend, CacheEntry, CacheManager

__all__ = [
    # Primary unified interface (recommended)
    "UnifiedCacheManager",
    "get_unified_cache_manager",
    "get_cache",
    "set_cache",
    "invalidate_cache_pattern",
    "CacheAnalytics",
    "CacheHealth",
    "CacheInvalidationEvent",
    "CacheWarmingConfig",
    "CacheStrategy",

    # Backward compatibility
    "AsyncCache",
    "CacheBackend",
    "CacheEntry",
    "CacheManager",
]
