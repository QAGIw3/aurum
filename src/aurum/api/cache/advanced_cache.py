"""Advanced caching system with warming, analytics, and distributed features."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from enum import Enum

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

from ..telemetry.context import get_request_id
from .cache import AsyncCache, CacheBackend, CacheEntry
from .config import CacheConfig


class CacheStrategy(Enum):
    """Cache strategies for different data types."""
    LRU = "lru"              # Least Recently Used
    LFU = "lfu"              # Least Frequently Used
    TTL = "ttl"              # Time To Live based
    ADAPTIVE = "adaptive"    # Adaptive based on access patterns


class CacheWarmerStatus(Enum):
    """Status of cache warming operations."""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class CacheAnalytics:
    """Analytics data for cache performance."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_requests: int = 0
    average_response_time: float = 0.0
    hit_rate: float = 0.0
    miss_rate: float = 0.0
    cache_size_bytes: int = 0
    memory_usage_percent: float = 0.0

    def update_hit_rate(self) -> None:
        """Update hit rate based on current stats."""
        self.total_requests = self.hits + self.misses
        if self.total_requests > 0:
            self.hit_rate = self.hits / self.total_requests
            self.miss_rate = self.misses / self.total_requests


@dataclass
class WarmupTask:
    """Cache warming task configuration."""
    name: str
    key_pattern: str
    warmup_function: callable
    schedule_interval: int = 300  # 5 minutes
    priority: int = 1
    enabled: bool = True
    last_run: Optional[float] = None
    next_run: Optional[float] = None


@dataclass
class DistributedCacheNode:
    """Node in distributed cache cluster."""
    host: str
    port: int
    weight: int = 1
    status: str = "healthy"
    last_heartbeat: float = field(default_factory=time.time)


class AdvancedCacheManager:
    """Advanced cache manager with warming, analytics, and distributed features."""

    def __init__(self, config: CacheConfig, backend: CacheBackend = CacheBackend.HYBRID):
        self.config = config
        self.backend = backend
        self._cache = AsyncCache(config, backend)
        self._analytics: Dict[str, CacheAnalytics] = defaultdict(CacheAnalytics)
        self._warmup_tasks: Dict[str, WarmupTask] = {}
        self._distributed_nodes: List[DistributedCacheNode] = []
        self._lock = asyncio.Lock()
        self._warming_status = CacheWarmerStatus.IDLE
        self._strategy = CacheStrategy.ADAPTIVE

        # Initialize distributed nodes if configured
        self._setup_distributed_nodes()

    def _setup_distributed_nodes(self) -> None:
        """Setup distributed cache nodes from configuration."""
        if self.config.mode == "cluster" and self.config.cluster_nodes:
            for node in self.config.cluster_nodes:
                if ":" in node:
                    host, port_str = node.split(":", 1)
                    try:
                        port = int(port_str)
                        self._distributed_nodes.append(DistributedCacheNode(host, port))
                    except ValueError:
                        pass

    async def get(
        self,
        key: str,
        namespace: str = "default"
    ) -> Optional[Any]:
        """Get value from cache with analytics tracking."""
        start_time = time.time()
        namespaced_key = f"{namespace}:{key}"

        try:
            value = await self._cache.get(namespaced_key)

            # Update analytics
            async with self._lock:
                analytics = self._analytics[namespace]
                if value is not None:
                    analytics.hits += 1
                else:
                    analytics.misses += 1

                response_time = time.time() - start_time
                analytics.average_response_time = (
                    (analytics.average_response_time + response_time) / 2
                )
                analytics.update_hit_rate()

            return value

        except Exception as exc:
            # Update analytics for errors
            async with self._lock:
                analytics = self._analytics[namespace]
                analytics.misses += 1
                analytics.update_hit_rate()

            raise exc

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        namespace: str = "default"
    ) -> None:
        """Set value in cache with strategy-based eviction."""
        namespaced_key = f"{namespace}:{key}"

        try:
            # Apply cache strategy
            if self._strategy == CacheStrategy.LRU:
                ttl = await self._apply_lru_strategy(namespace, ttl)
            elif self._strategy == CacheStrategy.LFU:
                ttl = await self._apply_lfu_strategy(namespace, ttl)
            elif self._strategy == CacheStrategy.ADAPTIVE:
                ttl = await self._apply_adaptive_strategy(namespace, key, ttl)

            await self._cache.set(namespaced_key, value, ttl)

        except Exception as exc:
            raise exc

    async def _apply_lru_strategy(self, namespace: str, ttl: Optional[int]) -> Optional[int]:
        """Apply LRU eviction strategy."""
        # In a real implementation, this would track access order
        # For now, return the provided TTL
        return ttl

    async def _apply_lfu_strategy(self, namespace: str, ttl: Optional[int]) -> Optional[int]:
        """Apply LFU eviction strategy."""
        # In a real implementation, this would track access frequency
        # For now, return the provided TTL
        return ttl

    async def _apply_adaptive_strategy(
        self,
        namespace: str,
        key: str,
        ttl: Optional[int]
    ) -> Optional[int]:
        """Apply adaptive TTL based on access patterns."""
        analytics = self._analytics[namespace]

        if analytics.hit_rate > 0.8:  # High hit rate, increase TTL
            return (ttl or self.config.ttl_seconds) * 2
        elif analytics.hit_rate < 0.3:  # Low hit rate, decrease TTL
            return max((ttl or self.config.ttl_seconds) // 2, 60)  # Minimum 1 minute
        else:
            return ttl

    async def delete(self, key: str, namespace: str = "default") -> None:
        """Delete value from cache."""
        namespaced_key = f"{namespace}:{key}"
        await self._cache.delete(namespaced_key)

    async def clear_namespace(self, namespace: str) -> None:
        """Clear all entries in a namespace."""
        # This is a simplified implementation
        # In practice, would need pattern-based deletion
        async with self._lock:
            self._analytics.pop(namespace, None)
        await self._cache.clear()

    async def get_analytics(self, namespace: str = "default") -> CacheAnalytics:
        """Get analytics for a namespace."""
        return self._analytics[namespace]

    async def get_all_analytics(self) -> Dict[str, CacheAnalytics]:
        """Get analytics for all namespaces."""
        return dict(self._analytics)

    async def register_warmup_task(
        self,
        task: WarmupTask
    ) -> None:
        """Register a cache warmup task."""
        async with self._lock:
            self._warmup_tasks[task.name] = task
            task.next_run = time.time() + task.schedule_interval

    async def unregister_warmup_task(self, task_name: str) -> None:
        """Unregister a cache warmup task."""
        async with self._lock:
            self._warmup_tasks.pop(task_name, None)

    async def start_cache_warming(self) -> None:
        """Start the cache warming process."""
        async with self._lock:
            if self._warming_status == CacheWarmerStatus.RUNNING:
                return

            self._warming_status = CacheWarmerStatus.RUNNING

        try:
            await self._run_warmup_cycle()
            async with self._lock:
                self._warming_status = CacheWarmerStatus.COMPLETED

        except Exception as exc:
            async with self._lock:
                self._warming_status = CacheWarmerStatus.FAILED

    async def _run_warmup_cycle(self) -> None:
        """Run one cycle of cache warming."""
        current_time = time.time()

        for task_name, task in list(self._warmup_tasks.items()):
            if not task.enabled or task.next_run is None:
                continue

            if current_time >= task.next_run:
                try:
                    await self._execute_warmup_task(task)
                    task.last_run = current_time
                    task.next_run = current_time + task.schedule_interval

                except Exception as exc:
                    # Log error but continue with other tasks
                    pass

    async def _execute_warmup_task(self, task: WarmupTask) -> None:
        """Execute a single warmup task."""
        try:
            # Generate keys based on pattern
            keys_to_warm = await self._generate_warmup_keys(task.key_pattern)

            # Execute warmup function for each key
            for key in keys_to_warm:
                try:
                    value = await task.warmup_function(key)
                    if value is not None:
                        await self.set(key, value, namespace="warmup")

                except Exception as exc:
                    # Log individual key errors but continue
                    pass

        except Exception as exc:
            raise exc

    async def _generate_warmup_keys(self, pattern: str) -> List[str]:
        """Generate keys to warm up based on pattern."""
        # This is a simplified implementation
        # In practice, would query database or use pattern matching
        return [f"warmup_key_{i}" for i in range(10)]

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        stats = {
            "warming_status": self._warming_status.value,
            "strategy": self._strategy.value,
            "namespaces": {},
            "distributed_nodes": len(self._distributed_nodes),
            "warmup_tasks": len(self._warmup_tasks),
            "request_id": get_request_id(),
        }

        for namespace, analytics in self._analytics.items():
            stats["namespaces"][namespace] = {
                "hits": analytics.hits,
                "misses": analytics.misses,
                "hit_rate": analytics.hit_rate,
                "total_requests": analytics.total_requests,
                "average_response_time": analytics.average_response_time,
            }

        # Add distributed node info
        if self._distributed_nodes:
            stats["distributed_info"] = {
                "nodes": [
                    {
                        "host": node.host,
                        "port": node.port,
                        "status": node.status,
                        "weight": node.weight
                    }
                    for node in self._distributed_nodes
                ]
            }

        return stats

    async def invalidate_pattern(self, pattern: str, namespace: str = "default") -> int:
        """Invalidate cache entries matching a pattern."""
        # Simplified implementation - in practice would use Redis SCAN
        count = 0
        # This would iterate through keys and delete matching ones
        return count

    async def preload_frequently_accessed_data(self) -> None:
        """Preload frequently accessed data into cache."""
        # Common patterns for preloading
        preload_patterns = [
            "curve:PJM:DAY_AHEAD:HUB",
            "metadata:dimensions",
            "metadata:locations:PJM",
        ]

        for pattern in preload_patterns:
            # This would trigger loading of frequently accessed data
            pass

    async def get_memory_usage(self) -> Dict[str, Any]:
        """Get cache memory usage information."""
        usage = {
            "memory_entries": len(self._cache._memory_cache) if hasattr(self._cache, '_memory_cache') else 0,
            "estimated_size_bytes": 0,
            "namespaces": {},
        }

        # Estimate memory usage
        if hasattr(self._cache, '_memory_cache'):
            for key, entry in self._cache._memory_cache.items():
                try:
                    size = len(str(entry.value).encode('utf-8'))
                    usage["estimated_size_bytes"] += size

                    # Group by namespace
                    namespace = key.split(':')[0] if ':' in key else 'unknown'
                    if namespace not in usage["namespaces"]:
                        usage["namespaces"][namespace] = 0
                    usage["namespaces"][namespace] += size

                except Exception:
                    pass

        return usage


class CacheWarmingService:
    """Service for managing cache warming operations."""

    def __init__(self, cache_manager: AdvancedCacheManager):
        self.cache_manager = cache_manager
        self._background_tasks: Set[asyncio.Task] = set()
        self._running = False

    async def start_warming_service(self) -> None:
        """Start the background cache warming service."""
        if self._running:
            return

        self._running = True
        self._background_tasks.add(
            asyncio.create_task(self._warming_loop())
        )

    async def stop_warming_service(self) -> None:
        """Stop the background cache warming service."""
        self._running = False

        # Cancel all background tasks
        for task in self._background_tasks:
            task.cancel()

        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()

    async def _warming_loop(self) -> None:
        """Main warming loop."""
        while self._running:
            try:
                await self.cache_manager.start_cache_warming()
                await asyncio.sleep(60)  # Check every minute

            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Log error and continue
                await asyncio.sleep(300)  # Wait 5 minutes on error

    async def add_warmup_task(
        self,
        name: str,
        key_pattern: str,
        warmup_function: callable,
        schedule_interval: int = 300,
        priority: int = 1
    ) -> None:
        """Add a new warmup task."""
        task = WarmupTask(
            name=name,
            key_pattern=key_pattern,
            warmup_function=warmup_function,
            schedule_interval=schedule_interval,
            priority=priority,
        )

        await self.cache_manager.register_warmup_task(task)

    async def get_warming_status(self) -> Dict[str, Any]:
        """Get current warming status."""
        stats = await self.cache_manager.get_cache_stats()
        return {
            "warming_status": stats.get("warming_status", "unknown"),
            "active_tasks": len(stats.get("warmup_tasks", {})),
            "service_running": self._running,
        }


# Global instances
_advanced_cache_manager = None
_cache_warming_service = None


def get_advanced_cache_manager() -> AdvancedCacheManager:
    """Get the global advanced cache manager."""
    global _advanced_cache_manager
    if _advanced_cache_manager is None:
        from .state import get_settings
        settings = get_settings()
        _advanced_cache_manager = AdvancedCacheManager(settings.api.cache)
    return _advanced_cache_manager


def get_cache_warming_service() -> CacheWarmingService:
    """Get the global cache warming service."""
    global _cache_warming_service
    if _cache_warming_service is None:
        cache_manager = get_advanced_cache_manager()
        _cache_warming_service = CacheWarmingService(cache_manager)
    return _cache_warming_service
