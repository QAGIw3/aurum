"""Fake CacheDAO for testing."""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ..cache_dao import CacheDAO, CacheEntry


class FakeCacheDAO(CacheDAO):
    """Fake implementation of CacheDAO for testing."""

    def __init__(self, cache_config: Optional[Dict[str, Any]] = None):
        """Initialize fake Cache DAO."""
        super().__init__(cache_config)
        self._entries: Dict[str, CacheEntry] = {}
        self._access_delay = 0.001
        self._fail_next_operations = False

    async def _connect(self) -> None:
        """Fake connection."""
        await asyncio.sleep(0.001)

    async def _disconnect(self) -> None:
        """Fake disconnection."""
        self._entries.clear()

    async def get_cached(
        self,
        key: str,
        fetcher: Optional[callable] = None,
        ttl_seconds: Optional[int] = None
    ) -> Optional[Any]:
        """Get cached value."""
        await asyncio.sleep(self._access_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated cache failure")

        if key in self._entries:
            entry = self._entries[key]
            if not entry.is_expired:
                entry.touch()
                return entry.value

        if fetcher:
            value = await fetcher()
            if value is not None:
                await self.set_cached(key, value, ttl_seconds)
            return value

        return None

    async def set_cached(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Set cached value."""
        await asyncio.sleep(self._access_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated cache set failure")

        entry = CacheEntry(key, value, ttl_seconds)
        self._entries[key] = entry

    async def invalidate(self, key: str) -> bool:
        """Invalidate cache entry."""
        await asyncio.sleep(self._access_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated cache invalidate failure")

        if key in self._entries:
            del self._entries[key]
            return True
        return False

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern."""
        await asyncio.sleep(self._access_delay)

        keys_to_delete = []
        for key in self._entries.keys():
            if pattern in key:
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del self._entries[key]

        return len(keys_to_delete)

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        await asyncio.sleep(self._access_delay)

        total_entries = len(self._entries)
        expired_entries = sum(1 for entry in self._entries.values() if entry.is_expired)
        active_entries = total_entries - expired_entries

        return {
            "total_entries": total_entries,
            "active_entries": active_entries,
            "expired_entries": expired_entries,
            "namespace": self.namespace,
            "backend": "fake_memory"
        }

    async def cleanup_expired(self) -> int:
        """Clean up expired entries."""
        await asyncio.sleep(self._access_delay)

        expired_keys = []
        for key, entry in self._entries.items():
            if entry.is_expired:
                expired_keys.append(key)

        for key in expired_keys:
            del self._entries[key]

        return len(expired_keys)

    def inject_entry(self, entry: CacheEntry) -> None:
        """Inject cache entry for testing."""
        self._entries[entry.key] = entry

    def get_entry(self, key: str) -> Optional[CacheEntry]:
        """Get cache entry for testing."""
        return self._entries.get(key)

    def clear_cache(self) -> None:
        """Clear all cache entries."""
        self._entries.clear()

    def set_access_delay(self, delay_seconds: float) -> None:
        """Set delay for cache access operations."""
        self._access_delay = delay_seconds
