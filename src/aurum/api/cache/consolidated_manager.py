"""Consolidated Cache Manager - Unified interface for all caching needs.

This module provides a single, comprehensive cache management interface that:
- Consolidates functionality from CacheManager, EnhancedCacheManager, GoldenQueryCache
- Provides standardized TTL governance and metrics
- Supports both Redis and in-memory backends
- Includes advanced features like cache warming, invalidation patterns, and analytics
- Maintains backward compatibility with existing cache usage
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Union, Callable, Awaitable
from enum import Enum
from datetime import datetime, timedelta

from pydantic import BaseModel

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger
from .cache import AsyncCache, CacheBackend, CacheEntry
from .cache_governance import (
    CacheGovernanceManager,
    TTLPolicy,
    TTLConfiguration,
    CacheNamespace,
    KeyNamingPattern
)
from .enhanced_cache_manager import (
    CacheKey,
    CacheInvalidationRule,
    StaleWhileRevalidateConfig
)
from ..config import CacheConfig


class CacheStrategy(Enum):
    """Cache strategies for different data types."""
    LRU = "lru"              # Least Recently Used
    LFU = "lfu"              # Least Frequently Used
    TTL = "ttl"              # Time To Live based
    ADAPTIVE = "adaptive"    # Adaptive based on access patterns


@dataclass
class CacheAnalytics:
    """Analytics data for cache performance."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    sets: int = 0
    deletes: int = 0
    errors: int = 0
    total_requests: int = 0
    hit_rate: float = 0.0
    avg_response_time: float = 0.0

    def __post_init__(self):
        if self.total_requests > 0:
            self.hit_rate = self.hits / self.total_requests


@dataclass
class CacheHealth:
    """Cache health metrics."""
    is_healthy: bool = True
    total_entries: int = 0
    memory_usage_mb: float = 0.0
    connection_count: int = 0
    error_rate: float = 0.0
    last_health_check: Optional[datetime] = None


class CacheInvalidationEvent(BaseModel):
    """Event for cache invalidation."""
    key_pattern: str
    namespace: CacheNamespace
    reason: str
    triggered_by: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


class CacheWarmingConfig(BaseModel):
    """Configuration for cache warming."""
    enabled: bool = True
    warmup_keys: List[str] = field(default_factory=list)
    warmup_fetcher: Optional[Callable[[str], Awaitable[Any]]] = None
    warmup_concurrency: int = 10
    warmup_timeout_seconds: int = 300


class UnifiedCacheManager:
    """Unified cache manager consolidating all cache functionality."""

    def __init__(
        self,
        config: CacheConfig,
        governance_manager: Optional[CacheGovernanceManager] = None,
        default_strategy: CacheStrategy = CacheStrategy.LRU,
        enable_metrics: bool = True,
        enable_health_checks: bool = True
    ):
        """Initialize unified cache manager.

        Args:
            config: Cache configuration
            governance_manager: Optional governance manager (creates default if None)
            default_strategy: Default cache strategy
            enable_metrics: Enable metrics collection
            enable_health_checks: Enable health checks
        """
        self.config = config
        self.default_strategy = default_strategy
        self.enable_metrics = enable_metrics
        self.enable_health_checks = enable_health_checks

        # Initialize governance manager
        self.governance = governance_manager or CacheGovernanceManager()

        # Initialize backends
        self.backends: Dict[str, AsyncCache] = {}
        self._initialize_backends()

        # Analytics and health tracking
        self.analytics = CacheAnalytics()
        self.health = CacheHealth()
        self._operation_times: List[float] = []

        # Cache warming
        self.warming_config = CacheWarmingConfig()

        # Invalidation tracking
        self.invalidation_events: List[CacheInvalidationEvent] = []

        # Background tasks
        self._background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()

        # Start background tasks
        if enable_health_checks:
            self._start_background_tasks()

        logger = get_logger(__name__)
        logger.info("Unified cache manager initialized",
                   backends=list(self.backends.keys()),
                   strategy=default_strategy.value)

    def _initialize_backends(self) -> None:
        """Initialize cache backends based on configuration."""
        # Primary Redis backend
        if self.config.redis_url:
            try:
                self.backends["redis"] = AsyncCache(
                    backend=CacheBackend.REDIS,
                    config=self.config
                )
            except Exception as e:
                logger = get_logger(__name__)
                logger.warning("Failed to initialize Redis backend", error=str(e))

        # Memory backend as fallback
        self.backends["memory"] = AsyncCache(
            backend=CacheBackend.MEMORY,
            config=self.config
        )

        # Set default backend
        self.default_backend = self.backends.get("redis") or self.backends.get("memory")

    def _start_background_tasks(self) -> None:
        """Start background maintenance tasks."""
        # Health check task
        task = asyncio.create_task(self._health_check_loop())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

        # Cache warming task
        task = asyncio.create_task(self._cache_warming_loop())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

        # Cleanup expired entries
        task = asyncio.create_task(self._cleanup_loop())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _health_check_loop(self) -> None:
        """Background task for health checks."""
        while not self._shutdown_event.is_set():
            try:
                await self._perform_health_check()
                await asyncio.sleep(30)  # Check every 30 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger = get_logger(__name__)
                logger.error("Health check failed", error=str(e))
                await asyncio.sleep(60)  # Wait longer on error

    async def _cache_warming_loop(self) -> None:
        """Background task for cache warming."""
        if not self.warming_config.enabled:
            return

        while not self._shutdown_event.is_set():
            try:
                await self._perform_cache_warming()
                await asyncio.sleep(300)  # Warm every 5 minutes
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger = get_logger(__name__)
                logger.error("Cache warming failed", error=str(e))
                await asyncio.sleep(60)

    async def _cleanup_loop(self) -> None:
        """Background task for cleanup operations."""
        while not self._shutdown_event.is_set():
            try:
                await self._cleanup_expired_entries()
                await asyncio.sleep(60)  # Cleanup every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger = get_logger(__name__)
                logger.error("Cache cleanup failed", error=str(e))
                await asyncio.sleep(30)

    async def _perform_health_check(self) -> None:
        """Perform comprehensive health check."""
        start_time = time.time()

        try:
            # Check backend health
            backend_healthy = await self._check_backend_health()

            # Update health status
            self.health.is_healthy = backend_healthy
            self.health.last_health_check = datetime.utcnow()

            # Get memory usage estimate
            if hasattr(self.default_backend, '_redis'):
                # Estimate Redis memory usage
                try:
                    info = await self.default_backend._redis.info("memory")
                    self.health.memory_usage_mb = float(info.get("used_memory", 0)) / (1024 * 1024)
                except Exception:
                    pass

            # Calculate error rate
            total_ops = self.analytics.hits + self.analytics.misses + self.analytics.errors
            if total_ops > 0:
                self.health.error_rate = self.analytics.errors / total_ops

            duration = time.time() - start_time
            if self.enable_metrics:
                metrics = get_metrics_client()
                metrics.gauge("aurum_cache_health_check_duration", duration)

        except Exception as e:
            self.health.is_healthy = False
            self.analytics.errors += 1
            logger = get_logger(__name__)
            logger.error("Health check failed", error=str(e))

    async def _check_backend_health(self) -> bool:
        """Check if cache backends are healthy."""
        for name, backend in self.backends.items():
            try:
                # Simple ping operation
                if hasattr(backend, 'ping'):
                    await backend.ping()
                elif hasattr(backend, 'get'):
                    await backend.get("health_check", default=None)
            except Exception:
                return False
        return True

    async def _perform_cache_warming(self) -> None:
        """Perform cache warming for configured keys."""
        if not self.warming_config.warmup_keys or not self.warming_config.warmup_fetcher:
            return

        logger = get_logger(__name__)
        logger.info("Starting cache warming", key_count=len(self.warming_config.warmup_keys))

        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.warming_config.warmup_concurrency)

        async def warm_key(key: str):
            async with semaphore:
                try:
                    value = await self.warming_config.warmup_fetcher(key)
                    if value is not None:
                        await self.set(key, value, namespace=CacheNamespace.SYSTEM_CONFIG)
                except Exception as e:
                    logger.warning("Failed to warm cache key", key=key, error=str(e))

        # Create tasks for all keys
        tasks = [warm_key(key) for key in self.warming_config.warmup_keys]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _cleanup_expired_entries(self) -> None:
        """Clean up expired cache entries."""
        for backend_name, backend in self.backends.items():
            try:
                if hasattr(backend, 'cleanup_expired'):
                    count = await backend.cleanup_expired()
                    if count > 0:
                        self.analytics.evictions += count
                        logger = get_logger(__name__)
                        logger.debug("Cleaned up expired entries",
                                   backend=backend_name, count=count)
            except Exception as e:
                logger = get_logger(__name__)
                logger.error("Failed to cleanup expired entries",
                           backend=backend_name, error=str(e))

    async def get(
        self,
        key: str,
        namespace: CacheNamespace = CacheNamespace.GENERAL,
        default: Any = None,
        ttl_policy: Optional[TTLPolicy] = None
    ) -> Any:
        """Get value from cache with governance.

        Args:
            key: Cache key
            namespace: Cache namespace
            default: Default value if not found
            ttl_policy: TTL policy to apply

        Returns:
            Cached value or default
        """
        start_time = time.time()

        try:
            # Apply governance
            if not KeyNamingPattern.validate_key(key, namespace):
                logger = get_logger(__name__)
                logger.warning("Invalid cache key pattern", key=key, namespace=namespace.value)

            # Get from backend
            value = await self.default_backend.get(key, default=default)

            # Track metrics
            if value is not None:
                self.analytics.hits += 1
            else:
                self.analytics.misses += 1

            duration = time.time() - start_time
            self._operation_times.append(duration)
            self.analytics.total_requests += 1

            # Update analytics
            if len(self._operation_times) > 1000:
                self.analytics.avg_response_time = sum(self._operation_times[-100:]) / 100
                self._operation_times = self._operation_times[-100:]

            if self.enable_metrics:
                metrics = get_metrics_client()
                metrics.counter("aurum_cache_requests_total",
                              labels={"result": "hit" if value is not None else "miss"})

            return value

        except Exception as e:
            self.analytics.errors += 1
            logger = get_logger(__name__)
            logger.error("Cache get failed", key=key, error=str(e))
            return default

    async def set(
        self,
        key: str,
        value: Any,
        namespace: CacheNamespace = CacheNamespace.GENERAL,
        ttl_seconds: Optional[int] = None,
        ttl_policy: Optional[TTLPolicy] = None
    ) -> bool:
        """Set value in cache with governance.

        Args:
            key: Cache key
            value: Value to cache
            namespace: Cache namespace
            ttl_seconds: TTL in seconds
            ttl_policy: TTL policy to apply

        Returns:
            True if successful
        """
        try:
            # Apply governance
            if not KeyNamingPattern.validate_key(key, namespace):
                logger = get_logger(__name__)
                logger.warning("Invalid cache key pattern", key=key, namespace=namespace.value)

            # Determine TTL
            if ttl_seconds is None and ttl_policy:
                ttl_config = TTLConfiguration.get_default_policies()[ttl_policy]
                ttl_seconds = ttl_config.seconds

            # Set in backend
            await self.default_backend.set(key, value, ttl_seconds)

            self.analytics.sets += 1

            if self.enable_metrics:
                metrics = get_metrics_client()
                metrics.counter("aurum_cache_sets_total")

            return True

        except Exception as e:
            self.analytics.errors += 1
            logger = get_logger(__name__)
            logger.error("Cache set failed", key=key, error=str(e))
            return False

    async def delete(self, key: str, namespace: CacheNamespace = CacheNamespace.GENERAL) -> bool:
        """Delete value from cache.

        Args:
            key: Cache key
            namespace: Cache namespace

        Returns:
            True if deleted
        """
        try:
            result = await self.default_backend.delete(key)
            self.analytics.deletes += 1

            if result and self.enable_metrics:
                metrics = get_metrics_client()
                metrics.counter("aurum_cache_deletes_total")

            return result

        except Exception as e:
            self.analytics.errors += 1
            logger = get_logger(__name__)
            logger.error("Cache delete failed", key=key, error=str(e))
            return False

    async def invalidate_pattern(
        self,
        pattern: str,
        namespace: CacheNamespace = CacheNamespace.GENERAL,
        reason: str = "manual",
        triggered_by: str = "system"
    ) -> int:
        """Invalidate cache entries matching pattern.

        Args:
            pattern: Key pattern to invalidate
            namespace: Cache namespace
            reason: Reason for invalidation
            triggered_by: Who triggered the invalidation

        Returns:
            Number of entries invalidated
        """
        try:
            count = 0

            # Invalidate in all backends
            for backend in self.backends.values():
                if hasattr(backend, 'invalidate_pattern'):
                    count += await backend.invalidate_pattern(pattern)

            # Record invalidation event
            event = CacheInvalidationEvent(
                key_pattern=pattern,
                namespace=namespace,
                reason=reason,
                triggered_by=triggered_by
            )
            self.invalidation_events.append(event)

            # Keep only last 1000 events
            if len(self.invalidation_events) > 1000:
                self.invalidation_events = self.invalidation_events[-1000:]

            if self.enable_metrics:
                metrics = get_metrics_client()
                metrics.counter("aurum_cache_invalidations_total", labels={"count": str(count)})

            logger = get_logger(__name__)
            logger.info("Cache pattern invalidated",
                       pattern=pattern, count=count, reason=reason)

            return count

        except Exception as e:
            logger = get_logger(__name__)
            logger.error("Cache pattern invalidation failed", pattern=pattern, error=str(e))
            return 0

    async def get_or_set(
        self,
        key: str,
        fetcher: Callable[[], Awaitable[Any]],
        namespace: CacheNamespace = CacheNamespace.GENERAL,
        ttl_policy: Optional[TTLPolicy] = None,
        ttl_seconds: Optional[int] = None
    ) -> Any:
        """Get value from cache or set it using fetcher.

        Args:
            key: Cache key
            fetcher: Function to fetch value if not cached
            namespace: Cache namespace
            ttl_policy: TTL policy to apply
            ttl_seconds: TTL in seconds

        Returns:
            Cached or fetched value
        """
        value = await self.get(key, namespace, default=None)

        if value is None:
            value = await fetcher()
            if value is not None:
                await self.set(key, value, namespace, ttl_seconds, ttl_policy)

        return value

    async def multi_get(self, keys: List[str], namespace: CacheNamespace = CacheNamespace.GENERAL) -> Dict[str, Any]:
        """Get multiple values from cache.

        Args:
            keys: List of cache keys
            namespace: Cache namespace

        Returns:
            Dictionary of key-value pairs
        """
        results = {}

        for key in keys:
            value = await self.get(key, namespace)
            if value is not None:
                results[key] = value

        return results

    async def multi_set(
        self,
        items: Dict[str, Any],
        namespace: CacheNamespace = CacheNamespace.GENERAL,
        ttl_seconds: Optional[int] = None
    ) -> bool:
        """Set multiple values in cache.

        Args:
            items: Dictionary of key-value pairs
            namespace: Cache namespace
            ttl_seconds: TTL in seconds

        Returns:
            True if all successful
        """
        success = True

        for key, value in items.items():
            if not await self.set(key, value, namespace, ttl_seconds):
                success = False

        return success

    def configure_warming(self, config: CacheWarmingConfig) -> None:
        """Configure cache warming.

        Args:
            config: Warming configuration
        """
        self.warming_config = config

    async def get_analytics(self) -> CacheAnalytics:
        """Get cache analytics.

        Returns:
            Current cache analytics
        """
        # Update hit rate
        total = self.analytics.hits + self.analytics.misses
        if total > 0:
            self.analytics.hit_rate = self.analytics.hits / total

        return self.analytics

    async def get_health(self) -> CacheHealth:
        """Get cache health status.

        Returns:
            Current cache health
        """
        return self.health

    async def get_invalidation_events(
        self,
        limit: int = 100,
        since: Optional[datetime] = None
    ) -> List[CacheInvalidationEvent]:
        """Get recent invalidation events.

        Args:
            limit: Maximum events to return
            since: Filter events since timestamp

        Returns:
            List of invalidation events
        """
        events = self.invalidation_events

        if since:
            events = [e for e in events if e.timestamp >= since]

        return events[-limit:]

    async def shutdown(self) -> None:
        """Shutdown cache manager and cleanup resources."""
        self._shutdown_event.set()

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()

        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        # Close backends
        for backend in self.backends.values():
            if hasattr(backend, 'close'):
                await backend.close()

        logger = get_logger(__name__)
        logger.info("Cache manager shutdown complete")


# Global instance
_unified_cache_manager: Optional[UnifiedCacheManager] = None


def get_unified_cache_manager(
    config: Optional[CacheConfig] = None,
    governance_manager: Optional[CacheGovernanceManager] = None
) -> UnifiedCacheManager:
    """Get the global unified cache manager instance.

    Args:
        config: Optional cache configuration
        governance_manager: Optional governance manager

    Returns:
        Unified cache manager instance
    """
    global _unified_cache_manager

    if _unified_cache_manager is None:
        if config is None:
            from ..config import get_settings
            settings = get_settings()
            config = CacheConfig.from_settings(settings)

        _unified_cache_manager = UnifiedCacheManager(
            config=config,
            governance_manager=governance_manager
        )

    return _unified_cache_manager


# Backward compatibility functions
async def get_cache(key: str, namespace: CacheNamespace = CacheNamespace.GENERAL) -> Any:
    """Backward compatibility function."""
    manager = get_unified_cache_manager()
    return await manager.get(key, namespace)


async def set_cache(
    key: str,
    value: Any,
    namespace: CacheNamespace = CacheNamespace.GENERAL,
    ttl_seconds: Optional[int] = None
) -> bool:
    """Backward compatibility function."""
    manager = get_unified_cache_manager()
    return await manager.set(key, value, namespace, ttl_seconds)


async def invalidate_cache_pattern(pattern: str, namespace: CacheNamespace = CacheNamespace.GENERAL) -> int:
    """Backward compatibility function."""
    manager = get_unified_cache_manager()
    return await manager.invalidate_pattern(pattern, namespace)
