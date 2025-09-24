"""Enhanced multi-level caching system with intelligent eviction and warming."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import weakref
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from collections import OrderedDict

logger = logging.getLogger(__name__)


class CacheLevel(Enum):
    """Cache levels for multi-tier caching."""
    L1_MEMORY = "l1_memory"      # In-process memory cache
    L2_DISTRIBUTED = "l2_distributed"  # Redis/distributed cache
    L3_PERSISTENT = "l3_persistent"    # Database/persistent cache


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"          # Least Recently Used
    LFU = "lfu"          # Least Frequently Used
    TTL = "ttl"          # Time To Live
    ADAPTIVE = "adaptive" # Adaptive eviction based on access patterns


@dataclass
class CacheMetrics:
    """Cache performance metrics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    size: int = 0
    memory_usage: int = 0
    avg_access_time: float = 0.0
    last_reset: float = field(default_factory=time.time)
    
    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


@dataclass
class CacheEntry:
    """Enhanced cache entry with access tracking."""
    key: str
    value: Any
    created_at: float
    ttl: Optional[int] = None
    access_count: int = 1
    last_accessed: float = field(default_factory=time.time)
    size_bytes: int = 0
    tags: Set[str] = field(default_factory=set)
    
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl is None:
            return False
        return time.time() > (self.created_at + self.ttl)
    
    def touch(self) -> None:
        """Update access metadata."""
        self.access_count += 1
        self.last_accessed = time.time()
    
    def age_seconds(self) -> float:
        """Get entry age in seconds."""
        return time.time() - self.created_at


class CacheBackend(ABC):
    """Abstract cache backend interface."""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Get value by key."""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None, tags: Optional[Set[str]] = None) -> None:
        """Set value with optional TTL and tags."""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete key, return True if existed."""
        pass
    
    @abstractmethod
    async def clear(self) -> None:
        """Clear all entries."""
        pass
    
    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        pass
    
    @abstractmethod
    async def invalidate_by_tags(self, tags: Set[str]) -> int:
        """Invalidate entries by tags, return count."""
        pass
    
    @abstractmethod
    def get_metrics(self) -> CacheMetrics:
        """Get cache metrics."""
        pass


class MemoryCacheBackend(CacheBackend):
    """In-memory cache backend with advanced eviction policies."""
    
    def __init__(self, max_size: int = 1000, max_memory_mb: int = 100, 
                 eviction_policy: EvictionPolicy = EvictionPolicy.LRU):
        self.max_size = max_size
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.eviction_policy = eviction_policy
        self._entries: Dict[str, CacheEntry] = {}
        self._access_order: OrderedDict[str, float] = OrderedDict()
        self._tag_index: Dict[str, Set[str]] = {}
        self._metrics = CacheMetrics()
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value by key."""
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._metrics.misses += 1
                return None
            
            if entry.is_expired():
                await self._remove_entry(key)
                self._metrics.misses += 1
                return None
            
            entry.touch()
            self._access_order[key] = time.time()
            self._metrics.hits += 1
            return entry.value
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None, tags: Optional[Set[str]] = None) -> None:
        """Set value with optional TTL and tags."""
        async with self._lock:
            # Calculate size
            size_bytes = self._estimate_size(value)
            
            # Remove existing entry if present
            if key in self._entries:
                await self._remove_entry(key)
            
            # Check if we need to evict entries
            await self._ensure_capacity(size_bytes)
            
            # Create new entry
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                ttl=ttl,
                size_bytes=size_bytes,
                tags=tags or set()
            )
            
            self._entries[key] = entry
            self._access_order[key] = time.time()
            
            # Update tag index
            for tag in entry.tags:
                if tag not in self._tag_index:
                    self._tag_index[tag] = set()
                self._tag_index[tag].add(key)
            
            self._metrics.size += 1
            self._metrics.memory_usage += size_bytes
    
    async def delete(self, key: str) -> bool:
        """Delete key, return True if existed."""
        async with self._lock:
            if key in self._entries:
                await self._remove_entry(key)
                return True
            return False
    
    async def clear(self) -> None:
        """Clear all entries."""
        async with self._lock:
            self._entries.clear()
            self._access_order.clear()
            self._tag_index.clear()
            self._metrics = CacheMetrics()
    
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        async with self._lock:
            entry = self._entries.get(key)
            if entry and not entry.is_expired():
                return True
            elif entry and entry.is_expired():
                await self._remove_entry(key)
            return False
    
    async def invalidate_by_tags(self, tags: Set[str]) -> int:
        """Invalidate entries by tags, return count."""
        async with self._lock:
            keys_to_remove = set()
            for tag in tags:
                if tag in self._tag_index:
                    keys_to_remove.update(self._tag_index[tag])
            
            count = 0
            for key in keys_to_remove:
                if key in self._entries:
                    await self._remove_entry(key)
                    count += 1
            
            return count
    
    def get_metrics(self) -> CacheMetrics:
        """Get cache metrics."""
        return self._metrics
    
    async def _remove_entry(self, key: str) -> None:
        """Remove entry and update indexes."""
        entry = self._entries.pop(key, None)
        if entry:
            self._access_order.pop(key, None)
            
            # Update tag index
            for tag in entry.tags:
                if tag in self._tag_index:
                    self._tag_index[tag].discard(key)
                    if not self._tag_index[tag]:
                        del self._tag_index[tag]
            
            self._metrics.size -= 1
            self._metrics.memory_usage -= entry.size_bytes
    
    async def _ensure_capacity(self, new_entry_size: int) -> None:
        """Ensure cache has capacity for new entry."""
        # Check memory limit
        while (self._metrics.memory_usage + new_entry_size) > self.max_memory_bytes and self._entries:
            await self._evict_one()
        
        # Check size limit
        while len(self._entries) >= self.max_size and self._entries:
            await self._evict_one()
    
    async def _evict_one(self) -> None:
        """Evict one entry based on policy."""
        if not self._entries:
            return
        
        if self.eviction_policy == EvictionPolicy.LRU:
            # Remove least recently used
            oldest_key = next(iter(self._access_order))
            await self._remove_entry(oldest_key)
        elif self.eviction_policy == EvictionPolicy.LFU:
            # Remove least frequently used
            min_count = min(entry.access_count for entry in self._entries.values())
            lfu_key = next(key for key, entry in self._entries.items() 
                          if entry.access_count == min_count)
            await self._remove_entry(lfu_key)
        elif self.eviction_policy == EvictionPolicy.TTL:
            # Remove expired entries first, then oldest
            expired_keys = [key for key, entry in self._entries.items() if entry.is_expired()]
            if expired_keys:
                await self._remove_entry(expired_keys[0])
            else:
                oldest_key = min(self._entries.keys(), 
                               key=lambda k: self._entries[k].created_at)
                await self._remove_entry(oldest_key)
        else:  # ADAPTIVE
            # Adaptive eviction based on access patterns
            current_time = time.time()
            scores = {}
            for key, entry in self._entries.items():
                age_factor = (current_time - entry.created_at) / 3600  # Age in hours
                frequency_factor = 1.0 / (entry.access_count + 1)
                recency_factor = (current_time - entry.last_accessed) / 3600
                scores[key] = age_factor + frequency_factor + recency_factor
            
            worst_key = max(scores.keys(), key=lambda k: scores[k])
            await self._remove_entry(worst_key)
        
        self._metrics.evictions += 1
    
    def _estimate_size(self, value: Any) -> int:
        """Estimate memory size of value."""
        try:
            if isinstance(value, (str, bytes)):
                return len(value)
            elif isinstance(value, (int, float)):
                return 8
            elif isinstance(value, (list, tuple)):
                return sum(self._estimate_size(item) for item in value)
            elif isinstance(value, dict):
                return sum(self._estimate_size(k) + self._estimate_size(v) 
                          for k, v in value.items())
            else:
                # Fallback to JSON serialization size
                return len(json.dumps(value, default=str))
        except Exception:
            return 1024  # Default estimate


class MultiLevelCache:
    """Multi-level cache with intelligent tier management."""
    
    def __init__(self, backends: Dict[CacheLevel, CacheBackend]):
        self.backends = backends
        self._metrics = {level: backend.get_metrics() for level, backend in backends.items()}
        self._promotion_threshold = 3  # Promote after 3 hits
        self._demotion_threshold = 0.1  # Demote if hit rate < 10%
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache, checking levels in order."""
        for level in [CacheLevel.L1_MEMORY, CacheLevel.L2_DISTRIBUTED, CacheLevel.L3_PERSISTENT]:
            if level not in self.backends:
                continue
                
            backend = self.backends[level]
            value = await backend.get(key)
            
            if value is not None:
                # Promote to higher levels
                await self._promote_to_higher_levels(key, value, level)
                return value
        
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None, 
                  tags: Optional[Set[str]] = None, level: Optional[CacheLevel] = None) -> None:
        """Set value in cache."""
        if level:
            # Set only in specified level
            if level in self.backends:
                await self.backends[level].set(key, value, ttl, tags)
        else:
            # Set in all levels
            for backend in self.backends.values():
                await backend.set(key, value, ttl, tags)
    
    async def delete(self, key: str) -> bool:
        """Delete from all levels."""
        deleted = False
        for backend in self.backends.values():
            if await backend.delete(key):
                deleted = True
        return deleted
    
    async def clear(self) -> None:
        """Clear all levels."""
        for backend in self.backends.values():
            await backend.clear()
    
    async def invalidate_by_tags(self, tags: Set[str]) -> int:
        """Invalidate entries by tags across all levels."""
        total_count = 0
        for backend in self.backends.values():
            total_count += await backend.invalidate_by_tags(tags)
        return total_count
    
    async def _promote_to_higher_levels(self, key: str, value: Any, current_level: CacheLevel) -> None:
        """Promote frequently accessed items to higher cache levels."""
        higher_levels = []
        
        if current_level == CacheLevel.L3_PERSISTENT:
            higher_levels = [CacheLevel.L2_DISTRIBUTED, CacheLevel.L1_MEMORY]
        elif current_level == CacheLevel.L2_DISTRIBUTED:
            higher_levels = [CacheLevel.L1_MEMORY]
        
        for level in higher_levels:
            if level in self.backends:
                await self.backends[level].set(key, value)
    
    def get_combined_metrics(self) -> Dict[CacheLevel, CacheMetrics]:
        """Get metrics from all cache levels."""
        return {level: backend.get_metrics() for level, backend in self.backends.items()}


class CacheWarmer:
    """Intelligent cache warming system."""
    
    def __init__(self, cache: MultiLevelCache):
        self.cache = cache
        self._warming_tasks: Set[asyncio.Task] = set()
        self._warming_patterns: List[Callable] = []
    
    def add_warming_pattern(self, pattern: Callable) -> None:
        """Add a cache warming pattern."""
        self._warming_patterns.append(pattern)
    
    async def warm_cache(self, keys: List[str], data_loader: Callable) -> None:
        """Warm cache with specified keys."""
        semaphore = asyncio.Semaphore(10)  # Limit concurrent warming
        
        async def warm_key(key: str):
            async with semaphore:
                try:
                    if not await self._key_exists_in_any_level(key):
                        value = await data_loader(key)
                        if value is not None:
                            await self.cache.set(key, value)
                except Exception as e:
                    logger.warning(f"Failed to warm cache for key {key}: {e}")
        
        tasks = [asyncio.create_task(warm_key(key)) for key in keys]
        self._warming_tasks.update(tasks)
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self._warming_tasks.difference_update(tasks)
    
    async def _key_exists_in_any_level(self, key: str) -> bool:
        """Check if key exists in any cache level."""
        for backend in self.cache.backends.values():
            if await backend.exists(key):
                return True
        return False
    
    async def stop_warming(self) -> None:
        """Stop all warming tasks."""
        for task in self._warming_tasks:
            task.cancel()
        
        if self._warming_tasks:
            await asyncio.gather(*self._warming_tasks, return_exceptions=True)
        
        self._warming_tasks.clear()


# Factory functions
def create_memory_cache(max_size: int = 1000, max_memory_mb: int = 100, 
                       eviction_policy: EvictionPolicy = EvictionPolicy.LRU) -> MemoryCacheBackend:
    """Create memory cache backend."""
    return MemoryCacheBackend(max_size, max_memory_mb, eviction_policy)


def create_multi_level_cache(
    memory_config: Optional[Dict] = None,
    distributed_backend: Optional[CacheBackend] = None,
    persistent_backend: Optional[CacheBackend] = None
) -> MultiLevelCache:
    """Create multi-level cache with specified backends."""
    backends = {}
    
    # L1 Memory cache
    memory_config = memory_config or {}
    backends[CacheLevel.L1_MEMORY] = create_memory_cache(**memory_config)
    
    # L2 Distributed cache (Redis, etc.)
    if distributed_backend:
        backends[CacheLevel.L2_DISTRIBUTED] = distributed_backend
    
    # L3 Persistent cache (Database, etc.)
    if persistent_backend:
        backends[CacheLevel.L3_PERSISTENT] = persistent_backend
    
    return MultiLevelCache(backends)