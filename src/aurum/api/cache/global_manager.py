"""Global CacheManager handle for API modules (transitional).

This provides a lightweight registry so non-request code (e.g., service layer)
can access the process CacheManager without reaching into FastAPI app state.
Use sparingly; prefer passing CacheManager explicitly via DI.
"""

from __future__ import annotations

from typing import Optional

from .cache import CacheManager

_GLOBAL_CACHE_MANAGER: Optional[CacheManager] = None


def set_global_cache_manager(manager: Optional[CacheManager]) -> None:
    global _GLOBAL_CACHE_MANAGER
    _GLOBAL_CACHE_MANAGER = manager


def get_global_cache_manager() -> Optional[CacheManager]:
    return _GLOBAL_CACHE_MANAGER


__all__ = ["set_global_cache_manager", "get_global_cache_manager"]

