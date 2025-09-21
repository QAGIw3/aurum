"""Enhanced caching service with multi-level caching and async support."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass
from enum import Enum

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from ..telemetry.context import get_request_id
from .config import CacheConfig


class CacheBackend(Enum):
    """Available cache backends."""
    MEMORY = "memory"
    REDIS = "redis"
    HYBRID = "hybrid"


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    value: Any
    created_at: float
    ttl: int
    access_count: int = 0
    last_accessed: float = 0

    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        return time.time() > (self.created_at + self.ttl)

    def touch(self) -> None:
        """Update access metadata."""
        self.access_count += 1
        self.last_accessed = time.time()


class AsyncCache:
    """Async cache with multi-backend support."""

    def __init__(self, config: CacheConfig, backend: CacheBackend = CacheBackend.HYBRID):
        self.config = config
        self.backend = backend
        self._memory_cache: Dict[str, CacheEntry] = {}
        self._redis_client: Optional[redis.Redis] = None
        self._lock = asyncio.Lock()

    async def _get_redis_client(self) -> Optional[redis.Redis]:
        """Get or create Redis client."""
        if self._redis_client is None and redis is not None:
            try:
                if self.config.redis_url:
                    self._redis_client = redis.from_url(
                        self.config.redis_url,
                        db=self.config.db,
                        decode_responses=True
                    )
                elif self.config.mode == "cluster" and self.config.cluster_nodes:
                    # Simplified cluster setup
                    self._redis_client = redis.Redis(
                        host=self.config.cluster_nodes[0].split(":")[0],
                        port=int(self.config.cluster_nodes[0].split(":")[1] or "6379"),
                        decode_responses=True
                    )
                await self._redis_client.ping()
            except Exception:
                self._redis_client = None
        return self._redis_client

    def _make_key(self, key: str) -> str:
        """Create a namespaced key."""
        namespace = self.config.namespace or "aurum"
        return f"{namespace}:{key}"

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        cache_key = self._make_key(key)
        request_id = get_request_id()

        # Try memory cache first
        if cache_key in self._memory_cache:
            entry = self._memory_cache[cache_key]
            if not entry.is_expired():
                entry.touch()
                return entry.value
            else:
                # Clean up expired entry
                async with self._lock:
                    self._memory_cache.pop(cache_key, None)

        # Try Redis if available
        redis_client = await self._get_redis_client()
        if redis_client:
            try:
                redis_value = await redis_client.get(cache_key)
                if redis_value:
                    try:
                        value = json.loads(redis_value)
                        # Store in memory cache for faster future access
                        entry = CacheEntry(
                            value=value,
                            created_at=time.time(),
                            ttl=self.config.ttl_seconds
                        )
                        async with self._lock:
                            self._memory_cache[cache_key] = entry
                        return value
                    except json.JSONDecodeError:
                        return None
            except Exception:
                pass

        return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        cache_key = self._make_key(key)
        ttl = ttl or self.config.ttl_seconds

        # Serialize value
        try:
            serialized_value = json.dumps(value, default=str)
        except (TypeError, ValueError):
            # Fallback for non-serializable values
            serialized_value = str(value)

        entry = CacheEntry(
            value=value,
            created_at=time.time(),
            ttl=ttl
        )

        # Store in memory cache
        async with self._lock:
            self._memory_cache[cache_key] = entry

        # Store in Redis if available
        redis_client = await self._get_redis_client()
        if redis_client:
            try:
                await redis_client.setex(cache_key, ttl, serialized_value)
            except Exception:
                pass

    async def delete(self, key: str) -> None:
        """Delete value from cache."""
        cache_key = self._make_key(key)

        # Remove from memory cache
        async with self._lock:
            self._memory_cache.pop(cache_key, None)

        # Remove from Redis if available
        redis_client = await self._get_redis_client()
        if redis_client:
            try:
                await redis_client.delete(cache_key)
            except Exception:
                pass

    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._memory_cache.clear()

        redis_client = await self._get_redis_client()
        if redis_client:
            try:
                # Clear all keys with our namespace
                namespace = self.config.namespace or "aurum"
                pattern = f"{namespace}:*"
                keys = await redis_client.keys(pattern)
                if keys:
                    await redis_client.delete(*keys)
            except Exception:
                pass

    async def get_or_set(
        self,
        key: str,
        factory: callable,
        ttl: Optional[int] = None
    ) -> Any:
        """Get value from cache or compute and cache it."""
        value = await self.get(key)
        if value is not None:
            return value

        # Compute value
        try:
            value = await factory()
            if value is not None:
                await self.set(key, value, ttl)
            return value
        except Exception:
            # If computation fails, return None but don't cache the failure
            return None

    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        async with self._lock:
            memory_entries = len(self._memory_cache)
            memory_size = sum(
                len(str(entry.value)) for entry in self._memory_cache.values()
            )

        redis_client = await self._get_redis_client()
        redis_info = {}
        if redis_client:
            try:
                info = await redis_client.info("memory")
                redis_info = {
                    "redis_used_memory": info.get("used_memory", 0),
                    "redis_used_memory_human": info.get("used_memory_human", "0B"),
                }
            except Exception:
                pass

        return {
            "backend": self.backend.value,
            "memory_entries": memory_entries,
            "memory_size_bytes": memory_size,
            **redis_info,
            "request_id": get_request_id(),
        }


class CacheManager:
    """High-level cache manager with specialized methods."""

    def __init__(self, cache_service: AsyncCache):
        self._cache = cache_service

    async def get_curve_data(
        self,
        iso: str,
        market: str,
        location: str,
        asof: Optional[str] = None
    ) -> Optional[Any]:
        """Get cached curve data."""
        key = f"curve:{iso}:{market}:{location}"
        if asof:
            key += f":{asof}"

        return await self._cache.get(key)

    async def cache_curve_data(
        self,
        data: Any,
        iso: str,
        market: str,
        location: str,
        asof: Optional[str] = None,
        ttl: Optional[int] = None
    ) -> None:
        """Cache curve data."""
        key = f"curve:{iso}:{market}:{location}"
        if asof:
            key += f":{asof}"

        await self._cache.set(key, data, ttl)

    async def get_metadata(self, metadata_type: str, **filters) -> Optional[Any]:
        """Get cached metadata."""
        filter_str = ":".join(f"{k}:{v}" for k, v in sorted(filters.items()))
        key = f"metadata:{metadata_type}:{filter_str}"

        return await self._cache.get(key)

    async def cache_metadata(
        self,
        data: Any,
        metadata_type: str,
        ttl: Optional[int] = None,
        **filters
    ) -> None:
        """Cache metadata."""
        filter_str = ":".join(f"{k}:{v}" for k, v in sorted(filters.items()))
        key = f"metadata:{metadata_type}:{filter_str}"

        await self._cache.set(key, data, ttl)

    async def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate cache entries matching a pattern."""
        # For now, just clear all cache - in production would implement pattern matching
        await self._cache.clear()

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        return await self._cache.get_stats()
