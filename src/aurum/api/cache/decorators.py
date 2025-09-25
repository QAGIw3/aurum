"""Caching decorators for API service functions.

Designed to be opt-in and resilient: if no cache manager is provided
or available, the wrapped function executes normally.
"""

from __future__ import annotations

import asyncio
from functools import wraps
from typing import Any, Callable, Coroutine, Optional, Protocol

from .cache import CacheManager


class _SupportsCacheManager(Protocol):
    cache_manager: Optional[CacheManager]


def cached(ttl_seconds: int, key_builder: Callable[..., str]) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Async-aware caching decorator using CacheManager when available.

    Usage:
        @cached(ttl_seconds=60, key_builder=lambda *a, **k: f"curves:{k['iso']}:{k['limit']}")
        async def list_curves(*, iso: str, limit: int, cache_manager: CacheManager | None = None):
            ...

    The wrapped function must accept a `cache_manager` kwarg or be a bound method
    with a `self.cache_manager` attribute; otherwise caching is skipped.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        is_coro = asyncio.iscoroutinefunction(func)

        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            manager: Optional[CacheManager] = kwargs.get("cache_manager")
            if manager is None and args:
                # Attempt to read from bound instance attribute
                self_obj = args[0]
                manager = getattr(self_obj, "cache_manager", None)

            if isinstance(manager, CacheManager):
                key_suffix = key_builder(*args, **kwargs)
                cached = await manager.get_cache_entry(key_suffix)
                if cached is not None:
                    return cached
                result = await func(*args, **kwargs) if is_coro else func(*args, **kwargs)
                await manager.set_cache_entry(key_suffix, result, ttl_seconds=ttl_seconds)
                return result

            # No cache available; execute directly
            return await func(*args, **kwargs) if is_coro else func(*args, **kwargs)

        if is_coro:
            return async_wrapper

        # If original is sync, provide a sync wrapper that still respects manager
        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            manager: Optional[CacheManager] = kwargs.get("cache_manager")
            if manager is None and args:
                manager = getattr(args[0], "cache_manager", None)
            if isinstance(manager, CacheManager):
                key_suffix = key_builder(*args, **kwargs)
                # Use event loop if present to await cache ops
                loop = asyncio.get_event_loop()
                cached = loop.run_until_complete(manager.get_cache_entry(key_suffix))
                if cached is not None:
                    return cached
                result = func(*args, **kwargs)
                loop.run_until_complete(manager.set_cache_entry(key_suffix, result, ttl_seconds=ttl_seconds))
                return result
            return func(*args, **kwargs)

        return sync_wrapper

    return decorator


__all__ = ["cached"]

