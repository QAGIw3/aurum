"""Compatibility shim for legacy import path aurum.cache.cache.

This module re-exports the unified cache manager API from
`aurum.api.cache.consolidated_manager` so legacy code importing
`from aurum.cache.cache import get_cache_manager` continues to work.
"""
from __future__ import annotations

from typing import Any, Tuple


def _impl() -> Tuple[Any, Any]:  # pragma: no cover - import indirection
    from aurum.api.cache.consolidated_manager import (
        get_unified_cache_manager,
        UnifiedCacheManager,
    )

    def get_cache_manager() -> UnifiedCacheManager:
        return get_unified_cache_manager()

    return get_cache_manager, UnifiedCacheManager


get_cache_manager, UnifiedCacheManager = _impl()

__all__ = [
    "get_cache_manager",
    "UnifiedCacheManager",
]
