"""Skeletons for Phase 1 cache utils tests (not collected by pytest).

These serve as a starting point for PR 3 and are intentionally not
named `test_*.py` to avoid breaking CI before the implementation lands.
"""

from __future__ import annotations

import asyncio
from typing import Any

from aurum.api.cache.utils import cache_get_or_set


class _FakeCacheManager:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}

    async def get_cache_entry(self, key_suffix: str):
        return self.store.get(key_suffix)

    async def set_cache_entry(self, key_suffix: str, value: Any, ttl_seconds: int):
        self.store[key_suffix] = value


async def _supplier():
    return 123


async def example_usage() -> None:
    cm = _FakeCacheManager()
    # First call populates cache
    v1 = await cache_get_or_set(cm, "demo:key", _supplier, 60)
    # Second call hits cache
    v2 = await cache_get_or_set(cm, "demo:key", _supplier, 60)
    assert v1 == v2 == 123


if __name__ == "__main__":
    asyncio.run(example_usage())

