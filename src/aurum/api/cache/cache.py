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
from ..database.redis_client import get_redis_manager
from ...observability.metrics import (
    set_redis_connection_pool_active,
    set_redis_connection_pool_idle,
    set_redis_connection_pool_total,
)


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
                    try:
                        manager = get_redis_manager()
                        self._redis_client = await manager.get_client(self.config.redis_url)
                    except Exception:
                        # Fallback to direct client
                        self._redis_client = redis.from_url(
                            self.config.redis_url,
                            db=self.config.db,
                            decode_responses=True
                        )
                elif self.config.mode == "cluster" and self.config.cluster_nodes:
                    # Simplified cluster setup (no manager for custom clustering here)
                    self._redis_client = redis.Redis(
                        host=self.config.cluster_nodes[0].split(":")[0],
                        port=int(self.config.cluster_nodes[0].split(":")[1] or "6379"),
                        decode_responses=True
                    )
                await self._redis_client.ping()
                # Update Redis pool metrics (best-effort)
                try:
                    pool = getattr(self._redis_client, "connection_pool", None)
                    if pool is not None:
                        active = int(getattr(pool, "_in_use_connections", 0) or 0)
                        total = int(getattr(pool, "_created_connections", 0) or 0)
                        max_conn = int(getattr(pool, "max_connections", 0) or 0)
                        idle = max(0, total - active)
                        await set_redis_connection_pool_active(active)
                        await set_redis_connection_pool_idle(idle)
                        await set_redis_connection_pool_total(total or max_conn)
                except Exception:
                    pass
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

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate keys matching a glob-style pattern.

        Args:
            pattern: Pattern excluding namespace prefix (e.g., "units:*")

        Returns:
            Number of keys removed across memory and Redis backends.
        """
        # Namespaced pattern for backends
        namespaced = self._make_key(pattern)
        removed = 0

        # Memory backend pruning
        try:
            async with self._lock:
                to_delete = [k for k in list(self._memory_cache.keys()) if _fnmatch(k, namespaced)]
                for k in to_delete:
                    self._memory_cache.pop(k, None)
                removed += len(to_delete)
        except Exception:
            pass

        # Redis backend pruning
        redis_client = await self._get_redis_client()
        if redis_client:
            try:
                # Use KEYS for simplicity in admin path; for large keyspaces SCAN would be safer
                keys = await redis_client.keys(namespaced)
                if keys:
                    await redis_client.delete(*keys)
                    removed += len(keys)
            except Exception:
                pass

        return removed


def _fnmatch(value: str, pattern: str) -> bool:
    try:
        from fnmatch import fnmatch
        return fnmatch(value, pattern)
    except Exception:
        return value == pattern


from typing import Any, Optional, Dict
from .consolidated_manager import get_unified_cache_manager


class CacheManager:
    """Lightweight adapter over UnifiedCacheManager for API use-sites.

    This replaces the previous re-export of libs.common.cache.CacheManager
    and delegates operations to the shared UnifiedCacheManager to avoid
    duplicate implementations.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Unified manager pulls configuration from core settings when not provided
        self._manager = get_unified_cache_manager()

    @staticmethod
    def build_cache_key(
        route: str,
        query_params: Optional[Dict[str, Any]] = None,
        *,
        version: str = "v2",
    ) -> str:
        """Generate a stable cache key for a route/query combination.

        Mirrors the semantics of the previous implementation to preserve keys.
        """
        import json as _json
        import hashlib as _hashlib

        if query_params:
            query_str = _json.dumps(query_params, sort_keys=True)
            query_hash = _hashlib.md5(query_str.encode()).hexdigest()[:12]
        else:
            query_hash = "none"
        return f"aurum:{version}:{route}:{query_hash}"

    async def get_cache_entry(self, key: str, *, namespace: Optional[str] = None) -> Optional[Any]:
        # Namespace handling is governed by UnifiedCacheManager; keys are already namespaced
        return await self._manager.get(key, default=None)

    async def set_cache_entry(
        self,
        key: str,
        value: Any,
        *,
        ttl_seconds: Optional[int] = None,
        namespace: Optional[str] = None,
    ) -> bool:
        try:
            await self._manager.set(key, value, ttl_seconds=ttl_seconds)
            return True
        except Exception:
            return False

    async def invalidate_pattern(self, pattern: str, *, namespace: Optional[str] = None) -> int:
        return await self._manager.invalidate_pattern(pattern)

    async def get_cache_stats(self) -> Dict[str, Any]:
        # Provide a compact stats view sourced from the unified manager
        try:
            snapshot = await self._manager.get_performance_snapshot()
        except Exception:
            snapshot = {}
        try:
            health = await self._manager.get_health()
            snapshot.update({
                "health": getattr(health, "is_healthy", False),
                "memory_usage_mb": getattr(health, "memory_usage_mb", 0.0),
                "error_rate": getattr(health, "error_rate", 0.0),
            })
        except Exception:
            pass
        return snapshot

    async def close(self) -> None:
        try:
            await self._manager.shutdown()
        except Exception:
            pass


__all__ = ["CacheBackend", "CacheEntry", "AsyncCache", "CacheManager"]
