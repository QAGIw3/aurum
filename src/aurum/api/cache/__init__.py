"""Cache functionality for the Aurum API - Unified interface for all caching needs."""

# Primary unified cache interface (recommended)
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
    CacheStrategy
)

# Backward compatibility imports
from .cache import AsyncCache, CacheBackend, CacheEntry, CacheManager
from .unified_cache_manager import (
    LegacyUnifiedCacheManager as UnifiedCacheManagerCompat,
    CacheStrategy as LegacyCacheStrategy,
    CacheAnalytics as LegacyCacheAnalytics,
    get_unified_cache_manager as get_unified_cache_manager_compat,
    set_unified_cache_manager as set_unified_cache_manager_compat
)

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
    "UnifiedCacheManagerCompat",
    "LegacyCacheStrategy",
    "LegacyCacheAnalytics",
    "get_unified_cache_manager_compat",
    "set_unified_cache_manager_compat"
]
