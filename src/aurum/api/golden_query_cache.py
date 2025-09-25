"""Public facade for the golden query cache utilities.

This module intentionally mirrors the historical ``aurum.api.golden_query_cache``
import path while delegating to the implementation that now lives in
``aurum.api.cache.golden_query_cache``. It provides light wrappers where tests
expect to patch module-level helpers (e.g. ``get_golden_query_cache``) without
reaching into the private cache package.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional

from aurum.api.cache import golden_query_cache as _impl
CacheInvalidationStrategy = _impl.CacheInvalidationStrategy
GoldenQueryCache = _impl.GoldenQueryCache
QueryPattern = _impl.QueryPattern
QueryType = _impl.QueryType

cache_golden_query = _impl.cache_golden_query
generate_cache_recommendations = _impl.generate_cache_recommendations
initialize_golden_query_cache = _impl.initialize_golden_query_cache
invalidate_on_write = _impl.invalidate_on_write
register_custom_pattern = _impl.register_custom_pattern


def get_golden_query_cache() -> GoldenQueryCache:
    """Return the globally configured golden query cache."""
    return _impl.get_golden_query_cache()


async def invalidate_cache_on_data_publish(
    table_name: str,
    asof_date: str,
    tenant_id: Optional[str] = None,
) -> int:
    """Invalidate cache entries when fresh data is published."""
    cache = get_golden_query_cache()
    return await cache.invalidate_on_asof_publish(table_name, asof_date, tenant_id)


async def get_cache_performance_report() -> Dict[str, Any]:
    """Proxy access to the cache performance report."""
    return await _impl.get_cache_performance_report()


async def get_query_patterns() -> Dict[str, Dict[str, Any]]:
    """Expose registered query patterns."""
    return await _impl.get_query_patterns()


def register_publish_hook(
    table_name: str,
    invalidation_func: Optional[Callable[[str, str, Optional[str]], Awaitable[int]]] = None,
):
    """Register a publish hook, defaulting to this module's invalidation helper."""
    invalidation = invalidation_func or invalidate_cache_on_data_publish

    def decorator(func_to_wrap: Callable[..., Awaitable[Any]]):
        return _impl.register_publish_hook(table_name, invalidation)(func_to_wrap)

    return decorator


__all__ = [
    "CacheInvalidationStrategy",
    "GoldenQueryCache",
    "QueryPattern",
    "QueryType",
    "cache_golden_query",
    "generate_cache_recommendations",
    "get_cache_performance_report",
    "get_golden_query_cache",
    "get_query_patterns",
    "initialize_golden_query_cache",
    "invalidate_cache_on_data_publish",
    "invalidate_on_write",
    "register_custom_pattern",
    "register_publish_hook",
]
