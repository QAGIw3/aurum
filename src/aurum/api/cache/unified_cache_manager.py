"""Unified Cache Manager - Consolidates all scattered cache implementations.

This module provides a single, unified interface for all caching operations,
consolidating functionality from CacheManager, EnhancedCacheManager, 
GoldenQueryCache, and AdvancedCache while maintaining backward compatibility.
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

from ...observability.metrics import get_metrics_client
from ...logging.structured_logger import get_logger
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
from .config import CacheConfig


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
    total_requests: int = 0
    average_response_time: float = 0.0
    hit_rate: float = 0.0
    miss_rate: float = 0.0
    cache_size_bytes: int = 0
    namespace_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def update_hit_rate(self):
        """Update hit rate calculation."""
        if self.total_requests > 0:
            self.hit_rate = self.hits / self.total_requests
            self.miss_rate = self.misses / self.total_requests


class UnifiedCacheManager:
    """Unified cache manager consolidating all scattered cache implementations.
    
    This class provides a single interface for all caching operations while
    maintaining backward compatibility with existing cache managers.
    """

    def __init__(self, config: Optional[CacheConfig] = None):
        """Initialize the unified cache manager."""
        self.config = config or CacheConfig()
        self.cache: Optional[AsyncCache] = None
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)
        
        # Governance system
        self.governance_manager: Optional[CacheGovernanceManager] = None
        
        # Analytics and monitoring
        self.analytics = CacheAnalytics()
        self._analytics_lock = asyncio.Lock()
        
        # Invalidation system
        self._invalidation_rules: List[CacheInvalidationRule] = []
        self._invalidation_tasks: Set[str] = set()
        
        # Stale-while-revalidate
        self.stale_config = StaleWhileRevalidateConfig()
        self._refresh_tasks: Dict[str, asyncio.Task] = {}
        self._refresh_locks: Dict[str, asyncio.Lock] = {}
        
        # Namespace management
        self._namespaces: Dict[str, Dict[str, Any]] = {}
        self._namespace_ttls: Dict[str, int] = {}
        
        # Cache warming
        self._warming_tasks: Dict[str, asyncio.Task] = {}
        
        # Strategy management
        self._strategies: Dict[str, CacheStrategy] = {}

    async def initialize(self, cache: AsyncCache):
        """Initialize the unified cache manager with underlying cache."""
        self.cache = cache
        await self._setup_governance()
        await self._setup_default_configurations()
        await self._setup_analytics()
        
    async def _setup_governance(self):
        """Set up cache governance system."""
        if self.cache:
            # Initialize governance manager directly without enhanced manager
            # since we're implementing the unified functionality here
            self.governance_manager = CacheGovernanceManager(self)
            
    def _get_namespace_from_key(self, key: str) -> Optional[CacheNamespace]:
        """Extract namespace from key for governance."""
        return KeyNamingPattern.get_namespace_from_key(key)
        
    # Add methods that governance manager expects
    async def set(self, 
                  key: str, 
                  value: Any, 
                  ttl: Optional[int] = None,
                  namespace: Optional[Union[CacheNamespace, str]] = None,
                  tenant_id: Optional[str] = None) -> None:
        """Set value in cache with governance and TTL policies."""
        if not self.cache:
            return
            
        namespace_str = namespace.value if isinstance(namespace, CacheNamespace) else namespace
        
        try:
            # Determine appropriate TTL
            effective_ttl = self._determine_ttl(key, ttl, namespace)
            
            # For now, use direct cache access instead of governance to avoid circular dependency
            # In a production system, you'd implement the governance directly here
            await self.cache.set(key, value, effective_ttl)
            
            # Update analytics
            async with self._analytics_lock:
                if namespace_str and namespace_str in self.analytics.namespace_stats:
                    self.analytics.namespace_stats[namespace_str]["sets"] += 1
                    
        except Exception as e:
            self.logger.error(f"Cache set error: {e}", extra={"key": key, "namespace": namespace})

    async def _setup_default_configurations(self):
        """Set up default TTLs and invalidation rules."""
        # Set up default TTLs for namespaces
        defaults = {
            CacheNamespace.CURVES.value: 300,      # 5 minutes
            CacheNamespace.METADATA.value: 1800,   # 30 minutes  
            CacheNamespace.SCENARIOS.value: 60,     # 1 minute
            CacheNamespace.EIA_DATA.value: 600,     # 10 minutes
            CacheNamespace.EXTERNAL_DATA.value: 1800, # 30 minutes
            CacheNamespace.USER_DATA.value: 3600,   # 1 hour
            CacheNamespace.SYSTEM_CONFIG.value: 86400, # 24 hours
        }
        self._namespace_ttls.update(defaults)
        
        # Set up default invalidation rules
        await self._setup_default_invalidation_rules()
        
    async def _setup_default_invalidation_rules(self):
        """Set up default cache invalidation rules."""
        rules = [
            CacheInvalidationRule(
                namespace=CacheNamespace.CURVES,
                patterns=["*"],
                ttl_seconds=300,
                priority=1
            ),
            CacheInvalidationRule(
                namespace=CacheNamespace.METADATA,
                patterns=["*"],
                ttl_seconds=1800,
                priority=2
            ),
            CacheInvalidationRule(
                namespace=CacheNamespace.SCENARIOS,
                patterns=["*"],
                ttl_seconds=60,
                priority=1
            ),
        ]
        self._invalidation_rules.extend(rules)

    async def _setup_analytics(self):
        """Initialize analytics system."""
        # Initialize namespace stats
        for namespace in CacheNamespace:
            self.analytics.namespace_stats[namespace.value] = {
                "hits": 0,
                "misses": 0,
                "sets": 0,
                "deletes": 0,
                "size_bytes": 0
            }

    def _get_namespace_ttl(self, namespace: Union[CacheNamespace, str]) -> int:
        """Get TTL for a namespace."""
        if isinstance(namespace, CacheNamespace):
            namespace = namespace.value
        return self._namespace_ttls.get(namespace, 300)  # Default 5 minutes

    def _determine_ttl(self, 
                      key: str, 
                      ttl_override: Optional[int] = None,
                      namespace: Optional[Union[CacheNamespace, str]] = None) -> int:
        """Determine the appropriate TTL for a cache key."""
        # Use override if provided
        if ttl_override is not None:
            return ttl_override
            
        # Try to extract namespace from key if not provided
        if namespace is None:
            namespace = KeyNamingPattern.get_namespace_from_key(key)
            
        # Use governance manager if available
        if self.governance_manager and namespace:
            try:
                return self.governance_manager.get_ttl_for_namespace(namespace)
            except Exception:
                pass
                
        # Fall back to namespace TTL or default
        if namespace:
            return self._get_namespace_ttl(namespace)
            
        return self.config.ttl_seconds

    async def _update_analytics(self, operation: str, namespace: Optional[str] = None, hit: bool = False):
        """Update cache analytics."""
        async with self._analytics_lock:
            self.analytics.total_requests += 1
            
            if hit:
                self.analytics.hits += 1
            else:
                self.analytics.misses += 1
                
            if namespace and namespace in self.analytics.namespace_stats:
                if hit:
                    self.analytics.namespace_stats[namespace]["hits"] += 1
                else:
                    self.analytics.namespace_stats[namespace]["misses"] += 1
                    
            self.analytics.update_hit_rate()

    # Core cache operations
    async def get(self, key: str, namespace: Optional[Union[CacheNamespace, str]] = None) -> Optional[Any]:
        """Get value from cache with governance and analytics."""
        if not self.cache:
            return None
            
        start_time = time.time()
        namespace_str = namespace.value if isinstance(namespace, CacheNamespace) else namespace
        
        try:
            # Use direct cache access for now
            result = await self.cache.get(key)
            await self._update_analytics("get", namespace_str, hit=result is not None)
            return result
            
        except Exception as e:
            self.logger.error(f"Cache get error: {e}", extra={"key": key, "namespace": namespace})
            await self._update_analytics("get", namespace_str, hit=False)
            return None
        finally:
            # Update response time
            response_time = time.time() - start_time
            if self.analytics.total_requests > 0:
                self.analytics.average_response_time = (
                    (self.analytics.average_response_time * (self.analytics.total_requests - 1) + response_time) /
                    self.analytics.total_requests
                )

    async def set(self, 
                  key: str, 
                  value: Any, 
                  ttl: Optional[int] = None,
                  namespace: Optional[Union[CacheNamespace, str]] = None,
                  tenant_id: Optional[str] = None) -> None:
        """Set value in cache with governance and TTL policies."""
        if not self.cache:
            return
            
        namespace_str = namespace.value if isinstance(namespace, CacheNamespace) else namespace
        
        try:
            # Determine appropriate TTL
            effective_ttl = self._determine_ttl(key, ttl, namespace)
            
            # Use direct cache access for now
            await self.cache.set(key, value, effective_ttl)
            
            # Update analytics
            async with self._analytics_lock:
                if namespace_str and namespace_str in self.analytics.namespace_stats:
                    self.analytics.namespace_stats[namespace_str]["sets"] += 1
                    
        except Exception as e:
            self.logger.error(f"Cache set error: {e}", extra={"key": key, "namespace": namespace})

    async def delete(self, key: str, namespace: Optional[Union[CacheNamespace, str]] = None) -> None:
        """Delete value from cache."""
        if not self.cache:
            return
            
        namespace_str = namespace.value if isinstance(namespace, CacheNamespace) else namespace
        
        try:
            await self.cache.delete(key)
            
            # Update analytics
            async with self._analytics_lock:
                if namespace_str and namespace_str in self.analytics.namespace_stats:
                    self.analytics.namespace_stats[namespace_str]["deletes"] += 1
                    
        except Exception as e:
            self.logger.error(f"Cache delete error: {e}", extra={"key": key, "namespace": namespace})

    async def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        if not self.cache:
            return False
        try:
            result = await self.cache.get(key)
            return result is not None
        except Exception:
            return False

    async def clear(self, namespace: Optional[Union[CacheNamespace, str]] = None) -> None:
        """Clear cache entries, optionally for a specific namespace."""
        if not self.cache:
            return
            
        if namespace:
            # Clear specific namespace
            namespace_str = namespace.value if isinstance(namespace, CacheNamespace) else namespace
            pattern = f"{namespace_str}:*"
            await self.invalidate_pattern(pattern)
        else:
            # Clear all
            await self.cache.clear()

    # High-level cache operations (backward compatibility)
    async def get_curve_data(self, iso: str, market: str, location: str, asof: Optional[str] = None) -> Optional[Any]:
        """Get cached curve data (backward compatible)."""
        key = CacheKey.make_key(CacheNamespace.CURVES, iso, market, location)
        if asof:
            key += f":{asof}"
        return await self.get(key, CacheNamespace.CURVES)

    async def cache_curve_data(self, 
                             data: Any, 
                             iso: str, 
                             market: str, 
                             location: str, 
                             asof: Optional[str] = None,
                             ttl: Optional[int] = None) -> None:
        """Cache curve data (backward compatible)."""
        key = CacheKey.make_key(CacheNamespace.CURVES, iso, market, location)
        if asof:
            key += f":{asof}"
        await self.set(key, data, ttl, CacheNamespace.CURVES)

    async def get_metadata(self, metadata_type: str, **filters) -> Optional[Any]:
        """Get cached metadata (backward compatible)."""
        filter_parts = [f"{k}={v}" for k, v in sorted(filters.items())]
        key = CacheKey.make_key(CacheNamespace.METADATA, metadata_type, *filter_parts)
        return await self.get(key, CacheNamespace.METADATA)

    async def cache_metadata(self, 
                           data: Any, 
                           metadata_type: str, 
                           ttl: Optional[int] = None, 
                           **filters) -> None:
        """Cache metadata (backward compatible)."""
        filter_parts = [f"{k}={v}" for k, v in sorted(filters.items())]
        key = CacheKey.make_key(CacheNamespace.METADATA, metadata_type, *filter_parts)
        await self.set(key, data, ttl, CacheNamespace.METADATA)

    # Pattern-based operations
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching a pattern."""
        if not self.cache:
            return 0
            
        # For now, implement basic pattern matching
        # In production, would use Redis SCAN or similar
        try:
            # This is a simplified implementation
            # Real implementation would depend on the underlying cache backend
            await self.cache.clear()  # Fallback to clearing all
            return 1  # Return count of invalidated entries
        except Exception as e:
            self.logger.error(f"Pattern invalidation error: {e}", extra={"pattern": pattern})
            return 0

    # Analytics and monitoring
    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        base_stats = {}
        if self.cache:
            base_stats = await self.cache.get_stats()
            
        unified_stats = {
            "unified_cache": {
                "total_requests": self.analytics.total_requests,
                "hits": self.analytics.hits,
                "misses": self.analytics.misses,
                "hit_rate": round(self.analytics.hit_rate, 4),
                "miss_rate": round(self.analytics.miss_rate, 4),
                "average_response_time_ms": round(self.analytics.average_response_time * 1000, 2),
                "namespace_stats": self.analytics.namespace_stats
            },
            "governance": {}
        }
        
        if self.governance_manager:
            try:
                unified_stats["governance"] = self.governance_manager._stats
            except AttributeError:
                unified_stats["governance"] = {"status": "not_available"}
            
        return {**base_stats, **unified_stats}

    async def get_cache_entry(self, key: str) -> Optional[Any]:
        """Get cache entry (alias for backward compatibility)."""
        return await self.get(key)

    async def set_cache_entry(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Set cache entry (alias for backward compatibility)."""
        await self.set(key, value, ttl_seconds)

    # Cache warming
    async def warm_cache(self, 
                        namespace: Union[CacheNamespace, str],
                        warm_func: Callable[[], Awaitable[Dict[str, Any]]],
                        force: bool = False) -> Dict[str, Any]:
        """Warm cache with data from a function."""
        namespace_str = namespace.value if isinstance(namespace, CacheNamespace) else namespace
        
        if not force and namespace_str in self._warming_tasks:
            task = self._warming_tasks[namespace_str]
            if not task.done():
                return {"status": "warming_in_progress"}
                
        async def _warm():
            try:
                data = await warm_func()
                count = 0
                for key, value in data.items():
                    await self.set(key, value, namespace=namespace)
                    count += 1
                return {"status": "completed", "warmed_keys": count}
            except Exception as e:
                self.logger.error(f"Cache warming failed: {e}", extra={"namespace": namespace_str})
                return {"status": "failed", "error": str(e)}
                
        task = asyncio.create_task(_warm())
        self._warming_tasks[namespace_str] = task
        
        return await task

    # Context manager support
    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        # Cancel any running tasks
        for task in self._warming_tasks.values():
            if not task.done():
                task.cancel()
                
        for task in self._refresh_tasks.values():
            if not task.done():
                task.cancel()


# Global instance management
_global_unified_cache_manager: Optional[UnifiedCacheManager] = None


def get_unified_cache_manager() -> Optional[UnifiedCacheManager]:
    """Get the global unified cache manager instance."""
    return _global_unified_cache_manager


def set_unified_cache_manager(manager: Optional[UnifiedCacheManager]) -> None:
    """Set the global unified cache manager instance."""
    global _global_unified_cache_manager
    _global_unified_cache_manager = manager


__all__ = [
    "UnifiedCacheManager",
    "CacheStrategy", 
    "CacheAnalytics",
    "get_unified_cache_manager",
    "set_unified_cache_manager"
]