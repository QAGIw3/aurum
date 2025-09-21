"""Advanced caching strategies for performance optimization."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Generic

from opentelemetry import trace

from ..telemetry.context import get_request_id

T = TypeVar('T')

LOGGER = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass
class CacheConfig:
    """Configuration for caching strategies."""
    ttl_seconds: int = 300  # 5 minutes default
    max_size: int = 1000
    cleanup_interval: int = 60  # 1 minute
    compression_enabled: bool = True
    prefetch_enabled: bool = False


@dataclass
class CacheMetrics:
    """Metrics for cache performance."""
    hits: int = 0
    misses: int = 0
    sets: int = 0
    deletes: int = 0
    evictions: int = 0
    hit_rate: float = 0.0
    total_requests: int = 0
    average_get_time: float = 0.0
    average_set_time: float = 0.0


class CacheBackend(ABC, Generic[T]):
    """Abstract base class for cache backends."""

    def __init__(self, config: CacheConfig):
        self.config = config
        self.metrics = CacheMetrics()
        self._cleanup_task: Optional[asyncio.Task] = None

    async def get(self, key: str) -> Optional[T]:
        """Get value from cache."""
        start_time = time.time()
        with trace.get_tracer(__name__).start_as_current_span("cache.get") as span:
            span.set_attribute("cache.key", key)

            try:
                value = await self._get_impl(key)
                get_time = time.time() - start_time

                self.metrics.total_requests += 1
                self.metrics.average_get_time = (
                    (self.metrics.average_get_time * (self.metrics.total_requests - 1) + get_time)
                    / self.metrics.total_requests
                )

                if value is not None:
                    self.metrics.hits += 1
                    self._update_hit_rate()
                    span.set_attribute("cache.result", "hit")
                else:
                    self.metrics.misses += 1
                    self._update_hit_rate()
                    span.set_attribute("cache.result", "miss")

                return value

            except Exception as exc:
                self.metrics.misses += 1
                self._update_hit_rate()
                LOGGER.warning(f"Cache get failed for key {key}: {exc}")
                return None

    async def set(self, key: str, value: T, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        start_time = time.time()
        with trace.get_tracer(__name__).start_as_current_span("cache.set") as span:
            span.set_attribute("cache.key", key)
            span.set_attribute("cache.ttl", ttl or self.config.ttl_seconds)

            try:
                await self._set_impl(key, value, ttl)
                set_time = time.time() - start_time

                self.metrics.sets += 1
                self.metrics.average_set_time = (
                    (self.metrics.average_set_time * (self.metrics.sets - 1) + set_time)
                    / self.metrics.sets
                )

                span.set_attribute("cache.result", "success")

            except Exception as exc:
                LOGGER.error(f"Cache set failed for key {key}: {exc}")
                span.set_attribute("cache.result", "error")

    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        with trace.get_tracer(__name__).start_as_current_span("cache.delete") as span:
            span.set_attribute("cache.key", key)

            try:
                result = await self._delete_impl(key)
                if result:
                    self.metrics.deletes += 1
                return result
            except Exception as exc:
                LOGGER.warning(f"Cache delete failed for key {key}: {exc}")
                return False

    async def clear(self) -> None:
        """Clear all cache entries."""
        await self._clear_impl()
        self.metrics = CacheMetrics()

    async def start_cleanup_task(self) -> None:
        """Start the cleanup task."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop_cleanup_task(self) -> None:
        """Stop the cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            self._cleanup_task = None

    def _update_hit_rate(self) -> None:
        """Update cache hit rate."""
        total = self.metrics.hits + self.metrics.misses
        if total > 0:
            self.metrics.hit_rate = self.metrics.hits / total

    @abstractmethod
    async def _get_impl(self, key: str) -> Optional[T]:
        """Implementation-specific get operation."""
        pass

    @abstractmethod
    async def _set_impl(self, key: str, value: T, ttl: Optional[int] = None) -> None:
        """Implementation-specific set operation."""
        pass

    @abstractmethod
    async def _delete_impl(self, key: str) -> bool:
        """Implementation-specific delete operation."""
        pass

    @abstractmethod
    async def _clear_impl(self) -> None:
        """Implementation-specific clear operation."""
        pass

    async def _cleanup_loop(self) -> None:
        """Cleanup expired entries periodically."""
        while True:
            try:
                await asyncio.sleep(self.config.cleanup_interval)
                await self._cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                LOGGER.error(f"Cache cleanup failed: {exc}")

    @abstractmethod
    async def _cleanup_expired(self) -> None:
        """Implementation-specific cleanup of expired entries."""
        pass


class InMemoryCache(CacheBackend[T]):
    """In-memory cache implementation with TTL support."""

    def __init__(self, config: CacheConfig):
        super().__init__(config)
        self._cache: Dict[str, Tuple[T, datetime]] = {}
        self._lock = asyncio.Lock()

    async def _get_impl(self, key: str) -> Optional[T]:
        async with self._lock:
            if key not in self._cache:
                return None

            value, expiry = self._cache[key]
            if datetime.now() > expiry:
                del self._cache[key]
                return None

            return value

    async def _set_impl(self, key: str, value: T, ttl: Optional[int] = None) -> None:
        async with self._lock:
            expiry = datetime.now() + timedelta(seconds=ttl or self.config.ttl_seconds)
            self._cache[key] = (value, expiry)

            # Check if we need to evict entries
            if len(self._cache) > self.config.max_size:
                await self._evict_lru()

    async def _delete_impl(self, key: str) -> bool:
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    async def _clear_impl(self) -> None:
        async with self._lock:
            self._cache.clear()

    async def _cleanup_expired(self) -> None:
        async with self._lock:
            now = datetime.now()
            expired_keys = [
                key for key, (_, expiry) in self._cache.items()
                if now > expiry
            ]
            for key in expired_keys:
                del self._cache[key]

    async def _evict_lru(self) -> None:
        """Evict least recently used entries."""
        # Simple size-based eviction (could be enhanced with LRU tracking)
        while len(self._cache) > self.config.max_size:
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
            self.metrics.evictions += 1


class MultiLevelCache(CacheBackend[T]):
    """Multi-level cache with L1 (in-memory) and L2 (distributed) tiers."""

    def __init__(self, l1_cache: CacheBackend[T], l2_cache: CacheBackend[T]):
        super().__init__(l1_cache.config)
        self.l1_cache = l1_cache
        self.l2_cache = l2_cache

    async def _get_impl(self, key: str) -> Optional[T]:
        # Try L1 first
        value = await self.l1_cache.get(key)
        if value is not None:
            return value

        # Try L2
        value = await self.l2_cache.get(key)
        if value is not None:
            # Write back to L1
            await self.l1_cache.set(key, value)
            return value

        return None

    async def _set_impl(self, key: str, value: T, ttl: Optional[int] = None) -> None:
        # Write to both caches
        await self.l1_cache.set(key, value, ttl)
        await self.l2_cache.set(key, value, ttl)

    async def _delete_impl(self, key: str) -> bool:
        # Delete from both caches
        l1_deleted = await self.l1_cache.delete(key)
        l2_deleted = await self.l2_cache.delete(key)
        return l1_deleted or l2_deleted

    async def _clear_impl(self) -> None:
        await self.l1_cache.clear()
        await self.l2_cache.clear()

    async def _cleanup_expired(self) -> None:
        await self.l1_cache._cleanup_expired()
        await self.l2_cache._cleanup_expired()


class CacheKeyGenerator:
    """Generate cache keys with consistent hashing."""

    @staticmethod
    def generate_key(prefix: str, *args, **kwargs) -> str:
        """Generate a cache key from prefix, args, and kwargs."""
        key_parts = [prefix]

        # Add positional args
        for arg in args:
            key_parts.append(str(arg))

        # Add keyword args in sorted order for consistency
        for key in sorted(kwargs.keys()):
            key_parts.append(f"{key}:{kwargs[key]}")

        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()[:32]


class CachedDataProcessor:
    """Data processor with built-in caching."""

    def __init__(self, cache: CacheBackend, cache_prefix: str = "data_processor"):
        self.cache = cache
        self.cache_prefix = cache_prefix
        self.key_generator = CacheKeyGenerator()

    async def process_with_cache(
        self,
        data: Any,
        processor_func,
        cache_key_params: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> Any:
        """Process data with caching."""
        cache_key = self.key_generator.generate_key(
            self.cache_prefix,
            **cache_key_params
        )

        # Try to get from cache first
        cached_result = await self.cache.get(cache_key)
        if cached_result is not None:
            LOGGER.debug(f"Cache hit for {cache_key}")
            return cached_result

        # Process the data
        result = await processor_func(data)

        # Cache the result
        await self.cache.set(cache_key, result, ttl)

        return result

    async def invalidate_cache(self, cache_key_params: Dict[str, Any]) -> None:
        """Invalidate cache entries matching the pattern."""
        cache_key = self.key_generator.generate_key(
            self.cache_prefix,
            **cache_key_params
        )
        await self.cache.delete(cache_key)


class DatabaseQueryCache:
    """Specialized cache for database query results."""

    def __init__(self, cache: CacheBackend):
        self.cache = cache
        self.key_generator = CacheKeyGenerator()

    async def get_query_result(
        self,
        query: str,
        params: Optional[Tuple] = None,
        ttl: Optional[int] = None
    ) -> Optional[Any]:
        """Get cached query result."""
        cache_key = self.key_generator.generate_key(
            "db_query",
            query=query,
            params=params or ()
        )
        return await self.cache.get(cache_key)

    async def cache_query_result(
        self,
        query: str,
        params: Optional[Tuple],
        result: Any,
        ttl: Optional[int] = None
    ) -> None:
        """Cache query result."""
        cache_key = self.key_generator.generate_key(
            "db_query",
            query=query,
            params=params or ()
        )
        await self.cache.set(cache_key, result, ttl)

    async def invalidate_table_cache(self, table_name: str) -> None:
        """Invalidate all cached results for a table."""
        # This is a simplified implementation
        # In practice, you'd want to use a pattern-based invalidation
        cache_key = self.key_generator.generate_key("table_invalidation", table=table_name)
        await self.cache.delete(cache_key)


# Global cache instances
_redis_cache: Optional[CacheBackend] = None
_memory_cache: Optional[CacheBackend] = None


def get_redis_cache() -> Optional[CacheBackend]:
    """Get Redis cache instance."""
    return _redis_cache


def get_memory_cache() -> Optional[CacheBackend]:
    """Get in-memory cache instance."""
    return _memory_cache


def initialize_caches(redis_config: Optional[CacheConfig] = None, memory_config: Optional[CacheConfig] = None) -> None:
    """Initialize global cache instances."""
    global _redis_cache, _memory_cache

    if memory_config:
        _memory_cache = InMemoryCache(memory_config)

    # Redis cache would be initialized here with actual Redis backend
    # For now, we'll use in-memory as fallback
    if redis_config:
        _redis_cache = InMemoryCache(redis_config)
