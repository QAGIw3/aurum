"""Enhanced cache management with unified namespaces, stale-while-revalidate, and smart invalidation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Union, Callable
from enum import Enum

from pydantic import BaseModel

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger
from .cache import AsyncCache, CacheEntry
from ..core.settings import get_settings


class CacheNamespace(str, Enum):
    """Standardized cache namespaces for better organization."""
    CURVES = "curves"
    METADATA = "metadata"
    SCENARIOS = "scenarios"
    EIA_DATA = "eia"
    EXTERNAL_DATA = "external"
    USER_DATA = "users"
    SYSTEM_CONFIG = "config"


class CacheKey:
    """Unified cache key management."""

    @staticmethod
    def make_key(namespace: Union[CacheNamespace, str], *components: str) -> str:
        """Create a standardized cache key."""
        if isinstance(namespace, CacheNamespace):
            namespace = namespace.value
        return f"{namespace}:{':'.join(str(c) for c in components)}"

    @staticmethod
    def make_pattern(namespace: Union[CacheNamespace, str], *components: str) -> str:
        """Create a cache key pattern for invalidation."""
        if isinstance(namespace, CacheNamespace):
            namespace = namespace.value
        return f"{namespace}:{':'.join(str(c) for c in components)}:*"

    @staticmethod
    def hash_key(key: str) -> str:
        """Create a hash of a key for consistent length."""
        return hashlib.sha256(key.encode()).hexdigest()[:16]


@dataclass
class CacheInvalidationRule:
    """Rule for cache invalidation patterns."""
    namespace: Union[CacheNamespace, str]
    patterns: List[str]  # Patterns to match for invalidation
    ttl_seconds: Optional[int] = None  # Override TTL for this rule
    priority: int = 0  # Higher priority rules run first


class StaleWhileRevalidateConfig:
    """Configuration for stale-while-revalidate behavior."""
    def __init__(
        self,
        max_stale_seconds: int = 300,  # 5 minutes
        background_refresh_enabled: bool = True,
        refresh_threshold_ratio: float = 0.8,  # Refresh when 80% of TTL elapsed
        max_concurrent_refreshes: int = 10
    ):
        self.max_stale_seconds = max_stale_seconds
        self.background_refresh_enabled = background_refresh_enabled
        self.refresh_threshold_ratio = refresh_threshold_ratio
        self.max_concurrent_refreshes = max_concurrent_refreshes


class EnhancedCacheManager:
    """Enhanced cache manager with unified namespaces and smart invalidation."""

    def __init__(self):
        self.cache: Optional[AsyncCache] = None
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

        # Invalidation rules
        self._invalidation_rules: List[CacheInvalidationRule] = []
        self._invalidation_tasks: Set[str] = set()

        # Stale-while-revalidate
        self.stale_config = StaleWhileRevalidateConfig()
        self._refresh_tasks: Dict[str, asyncio.Task] = {}
        self._refresh_locks: Dict[str, asyncio.Lock] = {}

        # Namespace management
        self._namespaces: Dict[str, Dict[str, Any]] = {}
        self._namespace_ttls: Dict[str, int] = {}

    async def initialize(self, cache: AsyncCache):
        """Initialize the enhanced cache manager."""
        self.cache = cache
        await self._setup_default_invalidation_rules()
        await self._setup_namespace_defaults()

    async def _setup_default_invalidation_rules(self):
        """Set up default cache invalidation rules."""
        rules = [
            # Curve data invalidation
            CacheInvalidationRule(
                namespace=CacheNamespace.CURVES,
                patterns=["*"],
                ttl_seconds=300,  # 5 minutes
                priority=1
            ),

            # Metadata invalidation
            CacheInvalidationRule(
                namespace=CacheNamespace.METADATA,
                patterns=["*"],
                ttl_seconds=1800,  # 30 minutes
                priority=2
            ),

            # Scenario data invalidation
            CacheInvalidationRule(
                namespace=CacheNamespace.SCENARIOS,
                patterns=["runs:*", "results:*"],
                ttl_seconds=60,  # 1 minute
                priority=3
            ),

            # EIA data invalidation
            CacheInvalidationRule(
                namespace=CacheNamespace.EIA_DATA,
                patterns=["*"],
                ttl_seconds=600,  # 10 minutes
                priority=4
            ),
        ]

        self._invalidation_rules.extend(rules)
        self._invalidation_rules.sort(key=lambda x: x.priority, reverse=True)

    async def _setup_namespace_defaults(self):
        """Set up default TTLs for namespaces."""
        defaults = {
            CacheNamespace.CURVES.value: 300,
            CacheNamespace.METADATA.value: 1800,
            CacheNamespace.SCENARIOS.value: 60,
            CacheNamespace.EIA_DATA.value: 600,
            CacheNamespace.EXTERNAL_DATA.value: 1800,
            CacheNamespace.USER_DATA.value: 3600,
            CacheNamespace.SYSTEM_CONFIG.value: 86400,
        }

        self._namespace_ttls.update(defaults)

    async def get_or_set(
        self,
        namespace: Union[CacheNamespace, str],
        key_components: tuple,
        factory: Callable,
        ttl_seconds: Optional[int] = None,
        use_stale_while_revalidate: bool = True
    ) -> Any:
        """Get value from cache or compute and cache it with enhanced features."""
        cache_key = CacheKey.make_key(namespace, *key_components)

        # Try to get from cache
        cached_value = await self.cache.get(cache_key) if self.cache else None

        if cached_value is not None:
            # Check if we should refresh in background
            if use_stale_while_revalidate and self.stale_config.background_refresh_enabled:
                await self._schedule_background_refresh(
                    cache_key, factory, ttl_seconds
                )
            return cached_value

        # Cache miss - compute value
        try:
            value = await factory()

            # Cache the result
            effective_ttl = ttl_seconds or self._get_namespace_ttl(namespace)
            if self.cache:
                await self.cache.set(cache_key, value, effective_ttl)

            self.metrics.increment_counter("cache_misses")
            return value

        except Exception as e:
            self.metrics.increment_counter("cache_set_errors")
            self.logger.error(f"Error computing cache value for {cache_key}: {e}")
            raise

    async def _schedule_background_refresh(
        self,
        cache_key: str,
        factory: Callable,
        ttl_seconds: Optional[int]
    ):
        """Schedule background refresh for stale-while-revalidate."""
        # Check if already scheduled
        if cache_key in self._refresh_tasks:
            return

        # Check if we need to refresh
        # This would require getting the cache entry metadata
        # For now, assume we should refresh if key exists
        refresh_lock = self._refresh_locks.get(cache_key)
        if refresh_lock is None:
            refresh_lock = asyncio.Lock()
            self._refresh_locks[cache_key] = refresh_lock

        async def refresh_task():
            try:
                async with refresh_lock:
                    # Re-compute value
                    value = await factory()

                    # Update cache
                    if self.cache:
                        effective_ttl = ttl_seconds or self._get_namespace_ttl(cache_key.split(':')[0])
                        await self.cache.set(cache_key, value, effective_ttl)

                    self.metrics.increment_counter("cache_background_refreshes")

            except Exception as e:
                self.metrics.increment_counter("cache_background_refresh_errors")
                self.logger.error(f"Background refresh failed for {cache_key}: {e}")
            finally:
                if cache_key in self._refresh_tasks:
                    del self._refresh_tasks[cache_key]

        # Schedule refresh
        task = asyncio.create_task(refresh_task())
        self._refresh_tasks[cache_key] = task

    def _get_namespace_ttl(self, namespace: Union[CacheNamespace, str]) -> int:
        """Get TTL for a namespace."""
        if isinstance(namespace, CacheNamespace):
            namespace = namespace.value
        return self._namespace_ttls.get(namespace, 300)  # Default 5 minutes

    async def invalidate_by_pattern(
        self,
        namespace: Union[CacheNamespace, str],
        *pattern_components: str
    ) -> int:
        """Invalidate cache entries matching a pattern."""
        if not self.cache:
            return 0

        pattern = CacheKey.make_pattern(namespace, *pattern_components)
        count = 0

        # Invalidate from memory cache (simplified)
        keys_to_delete = []
        for key in self.cache._memory_cache.keys():
            if key.startswith(pattern.replace('*', '')):
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del self.cache._memory_cache[key]
            count += 1

        # Invalidate from Redis (if available)
        try:
            redis_client = await self.cache._get_redis_client()
            if redis_client:
                keys = await redis_client.keys(pattern)
                if keys:
                    await redis_client.delete(*keys)
                    count += len(keys)
        except Exception as e:
            self.logger.error(f"Error invalidating Redis keys: {e}")

        self.metrics.increment_counter("cache_invalidations", count)
        self.logger.info(f"Invalidated {count} cache entries matching pattern: {pattern}")

        return count

    async def invalidate_by_rule(self, rule_name: str, *args: Any) -> int:
        """Invalidate cache using a registered rule."""
        rule = next((r for r in self._invalidation_rules if r.namespace == rule_name), None)
        if not rule:
            return 0

        total_invalidated = 0
        for pattern in rule.patterns:
            try:
                # Replace placeholders in pattern
                formatted_pattern = pattern.format(*args)
                count = await self.invalidate_by_pattern(rule.namespace, formatted_pattern)
                total_invalidated += count
            except Exception as e:
                self.logger.error(f"Error invalidating pattern {pattern}: {e}")

        return total_invalidated

    async def invalidate_tenant_data(self, tenant_id: str) -> int:
        """Invalidate all cache entries for a specific tenant."""
        total_invalidated = 0

        # Invalidate tenant-specific data across namespaces
        for namespace in [CacheNamespace.CURVES, CacheNamespace.METADATA, CacheNamespace.SCENARIOS]:
            count = await self.invalidate_by_pattern(namespace, f"tenant:{tenant_id}")
            total_invalidated += count

        return total_invalidated

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        if not self.cache:
            return {}

        # Get memory cache stats
        memory_entries = len(self.cache._memory_cache)
        memory_size = sum(len(str(entry.value)) for entry in self.cache._memory_cache.values())

        # Get Redis stats if available
        redis_stats = {}
        try:
            redis_client = await self.cache._get_redis_client()
            if redis_client:
                redis_stats = {
                    "redis_keys": await redis_client.dbsize(),
                    "redis_memory": await redis_client.info("memory")
                }
        except Exception:
            pass

        return {
            "memory_cache": {
                "entries": memory_entries,
                "estimated_size_bytes": memory_size,
                "max_memory": "N/A"  # Would need actual memory tracking
            },
            "redis_cache": redis_stats,
            "invalidation_rules": len(self._invalidation_rules),
            "active_refresh_tasks": len(self._refresh_tasks),
            "namespaces": list(self._namespace_ttls.keys()),
            "stale_while_revalidate": {
                "enabled": self.stale_config.background_refresh_enabled,
                "max_stale_seconds": self.stale_config.max_stale_seconds,
                "refresh_threshold_ratio": self.stale_config.refresh_threshold_ratio,
                "max_concurrent_refreshes": self.stale_config.max_concurrent_refreshes
            }
        }

    async def clear_namespace(self, namespace: Union[CacheNamespace, str]) -> int:
        """Clear all cache entries in a namespace."""
        if not self.cache:
            return 0

        # Clear memory cache
        keys_to_delete = []
        namespace_prefix = f"{namespace}:" if isinstance(namespace, CacheNamespace) else f"{namespace.value}:"

        for key in self.cache._memory_cache.keys():
            if key.startswith(namespace_prefix):
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del self.cache._memory_cache[key]

        # Clear Redis
        try:
            redis_client = await self.cache._get_redis_client()
            if redis_client:
                pattern = f"{namespace_prefix}*"
                keys = await redis_client.keys(pattern)
                if keys:
                    await redis_client.delete(*keys)
                    return len(keys_to_delete) + len(keys)
        except Exception:
            pass

        return len(keys_to_delete)

    def add_invalidation_rule(self, rule: CacheInvalidationRule):
        """Add a custom invalidation rule."""
        self._invalidation_rules.append(rule)
        self._invalidation_rules.sort(key=lambda x: x.priority, reverse=True)

    def get_etag(self, data: Any, namespace: Union[CacheNamespace, str]) -> str:
        """Generate a strong ETag for data."""
        # Include namespace in ETag for better cache isolation
        etag_data = {
            "namespace": str(namespace),
            "data": data,
            "timestamp": int(time.time())
        }
        return f'"{hashlib.sha256(json.dumps(etag_data, sort_keys=True, default=str).encode()).hexdigest()}"'


# Global cache manager instance
_cache_manager: Optional[EnhancedCacheManager] = None


async def get_enhanced_cache_manager() -> EnhancedCacheManager:
    """Get the global enhanced cache manager."""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = EnhancedCacheManager()
        # Initialize with cache from app state if available
        from ..app import app
        if hasattr(app.state, 'cache_service'):
            await _cache_manager.initialize(app.state.cache_service)
    return _cache_manager


def generate_strong_etag(data: Any, namespace: str = "") -> str:
    """Generate a strong ETag with namespace isolation."""
    etag_data = {
        "namespace": namespace,
        "data": data,
        "version": "1.0",  # Version for future ETag format changes
        "timestamp": int(time.time())
    }
    return f'"{hashlib.sha256(json.dumps(etag_data, sort_keys=True, default=str).encode()).hexdigest()}"'


__all__ = [
    "CacheNamespace",
    "CacheKey",
    "CacheInvalidationRule",
    "StaleWhileRevalidateConfig",
    "EnhancedCacheManager",
    "get_enhanced_cache_manager",
    "generate_strong_etag",
]
