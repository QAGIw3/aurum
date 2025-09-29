"""Compatibility shim for unified cache manager.

This module re-exports the authoritative unified cache manager from
``aurum.api.cache.consolidated_manager`` so callers can import from either
``aurum.api.cache.unified_cache_manager`` or ``aurum.api.cache.consolidated_manager``
without ambiguity. This eliminates duplicated implementations while preserving
backwards-compatibility.
"""

from __future__ import annotations

from typing import Any

# Re-export the canonical implementation
# Import inside functions to avoid circular import during module initialization
def _impl():  # pragma: no cover - simple import indirection
    from .consolidated_manager import (
        UnifiedCacheManager,
        LegacyUnifiedCacheManager,
        CacheStrategy,
        CacheAnalytics,
        get_unified_cache_manager,
        set_unified_cache_manager,
    )
    return (
        UnifiedCacheManager,
        LegacyUnifiedCacheManager,
        CacheStrategy,
        CacheAnalytics,
        get_unified_cache_manager,
        set_unified_cache_manager,
    )

UnifiedCacheManager, LegacyUnifiedCacheManager, CacheStrategy, CacheAnalytics, get_unified_cache_manager, set_unified_cache_manager = _impl()
from .cache_governance import CacheNamespace  # noqa: F401

__all__ = [
    "UnifiedCacheManager",
    "LegacyUnifiedCacheManager",
    "CacheStrategy",
    "CacheAnalytics",
    "CacheNamespace",
    "get_unified_cache_manager",
    "set_unified_cache_manager",
]
