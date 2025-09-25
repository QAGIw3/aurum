"""Cache utility helpers for API services.

These utilities standardize how API code interacts with CacheManager
without pulling CacheManager into pure business logic.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional

from .cache import CacheManager


async def cache_get_or_set(
    manager: Optional[CacheManager],
    key_suffix: str,
    supplier: Callable[[], Awaitable[Any]] | Callable[[], Any],
    ttl_seconds: int,
) -> Any:
    """Get from cache or set the value from a supplier.

    If `manager` is None, simply computes the value.
    The `supplier` can be sync or async.
    """
    if isinstance(manager, CacheManager):
        cached = await manager.get_cache_entry(key_suffix)
        if cached is not None:
            return cached
        value = supplier()
        if asyncio.iscoroutine(value):
            value = await value
        await manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds)
        return value

    value = supplier()
    if asyncio.iscoroutine(value):
        value = await value
    return value


__all__ = ["cache_get_or_set"]

