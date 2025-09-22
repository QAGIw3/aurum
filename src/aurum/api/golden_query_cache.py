"""Golden query cache for hot endpoints with TTL and bust-on-write hooks."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union, Callable, Awaitable
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps

from ..telemetry.context import get_correlation_id, get_tenant_id, log_structured
from .cache import AsyncCache, CacheManager, CacheBackend
from ..core import get_settings


class QueryType(Enum):
    """Types of queries that can be cached."""
    CURVE_DATA = "curve_data"
    CURVE_DIFF = "curve_diff"
    CURVE_STRIPS = "curve_strips"
    METADATA_DIMENSIONS = "metadata_dimensions"
    SCENARIO_OUTPUTS = "scenario_outputs"
    SCENARIO_METRICS = "scenario_metrics"
    AGGREGATION = "aggregation"
    CUSTOM = "custom"


class CacheInvalidationStrategy(Enum):
    """Strategies for cache invalidation."""
    TTL_ONLY = "ttl_only"  # Simple TTL-based expiration
    BUST_ON_WRITE = "bust_on_write"  # Invalidate when underlying data changes
    PATTERN_BASED = "pattern_based"  # Invalidate based on query patterns
    DEPENDENCY_BASED = "dependency_based"  # Invalidate based on data dependencies


@dataclass
class QueryPattern:
    """Pattern for matching and caching queries."""

    name: str  # Human-readable name for the pattern
    query_type: QueryType
    pattern: str  # Regex pattern to match queries
    key_template: str  # Template for generating cache keys
    ttl_seconds: int  # Default TTL for this pattern
    max_cache_size: int = 1000  # Maximum entries for this pattern
    invalidation_strategy: CacheInvalidationStrategy = CacheInvalidationStrategy.TTL_ONLY
    dependencies: List[str] = field(default_factory=list)  # Dependencies for invalidation

    def matches(self, query: str) -> bool:
        """Check if a query matches this pattern."""
        return bool(re.match(self.pattern, query, re.IGNORECASE))

    def extract_params(self, query: str) -> Dict[str, str]:
        """Extract parameters from a matching query."""
        match = re.match(self.pattern, query, re.IGNORECASE)
        if match:
            return match.groupdict()
        return {}

    def generate_key(self, params: Dict[str, str], tenant_id: str) -> str:
        """Generate cache key from parameters."""
        key = self.key_template.format(**params)
        return f"{tenant_id}:{self.query_type.value}:{key}"


@dataclass
class CacheEntry:
    """Enhanced cache entry with metadata for golden queries."""

    value: Any
    created_at: float
    ttl_seconds: int
    query_type: QueryType
    query_hash: str  # Hash of the original query
    tenant_id: str
    access_count: int = 0
    last_accessed: float = 0
    hit_rate: float = 0.0
    dependencies: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        return time.time() > (self.created_at + self.ttl_seconds)

    def touch(self) -> None:
        """Update access metadata."""
        self.access_count += 1
        self.last_accessed = time.time()
        # Update hit rate (simple moving average)
        self.hit_rate = (self.hit_rate + 1.0) / 2.0 if self.hit_rate > 0 else 1.0


class GoldenQueryCache:
    """Advanced cache for golden queries with bust-on-write and TTL support."""

    def __init__(self, cache_service: AsyncCache):
        self.cache = cache_service
        self._patterns: Dict[str, QueryPattern] = {}
        self._query_stats: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._settings = get_settings()

        # Register default query patterns
        self._register_default_patterns()

    def _get_ttl_for_query_type(self, query_type: QueryType) -> int:
        """Get appropriate TTL based on query type."""
        settings = self._settings.api.cache

        if query_type == QueryType.CURVE_DATA:
            return settings.cache_ttl_curve_data
        elif query_type == QueryType.METADATA_DIMENSIONS:
            return settings.cache_ttl_metadata
        elif query_type in [QueryType.SCENARIO_OUTPUTS, QueryType.SCENARIO_METRICS]:
            return settings.cache_ttl_scenario_data
        elif query_type == QueryType.CUSTOM:
            return settings.cache_ttl_low_frequency
        else:
            return settings.cache_ttl_medium_frequency

    def _register_default_patterns(self) -> None:
        """Register default query patterns for common endpoints."""

        # Get dynamic TTL values
        curve_ttl = self._get_ttl_for_query_type(QueryType.CURVE_DATA)
        metadata_ttl = self._get_ttl_for_query_type(QueryType.METADATA_DIMENSIONS)
        scenario_ttl = self._get_ttl_for_query_type(QueryType.SCENARIO_OUTPUTS)
        scenario_metrics_ttl = self._get_ttl_for_query_type(QueryType.SCENARIO_METRICS)
        diff_ttl = self._get_ttl_for_query_type(QueryType.CURVE_DIFF)
        strips_ttl = self._get_ttl_for_query_type(QueryType.CURVE_STRIPS)

        patterns = [
            # Curve data queries
            QueryPattern(
                name="curve_data_by_date",
                query_type=QueryType.CURVE_DATA,
                pattern=r"SELECT.*FROM.*curve_observation.*WHERE.*asof.*=.*'(?P<asof>\d{4}-\d{2}-\d{2})'.*AND.*iso.*=.*'(?P<iso>[A-Z]{3})'",
                key_template="curve_data:{asof}:{iso}",
                ttl_seconds=curve_ttl,  # Dynamic TTL
                invalidation_strategy=CacheInvalidationStrategy.BUST_ON_WRITE,
                dependencies=["curve_observation"]
            ),
            QueryPattern(
                name="curve_data_by_range",
                query_type=QueryType.CURVE_DATA,
                pattern=r"SELECT.*FROM.*curve_observation.*WHERE.*asof.*BETWEEN.*'(?P<start_date>\d{4}-\d{2}-\d{2})'.*AND.*'(?P<end_date>\d{4}-\d{2}-\d{2})'.*AND.*iso.*=.*'(?P<iso>[A-Z]{3})'",
                key_template="curve_data_range:{start_date}:{end_date}:{iso}",
                ttl_seconds=int(curve_ttl * 0.75),  # Slightly shorter for ranges
                invalidation_strategy=CacheInvalidationStrategy.BUST_ON_WRITE,
                dependencies=["curve_observation"]
            ),

            # Metadata dimension queries
            QueryPattern(
                name="metadata_dimensions",
                query_type=QueryType.METADATA_DIMENSIONS,
                pattern=r"SELECT.*DISTINCT.*(?P<dimension>\w+).*FROM.*(?P<table>\w+).*WHERE.*tenant_id.*=.*\$1",
                key_template="metadata_dimensions:{dimension}:{table}",
                ttl_seconds=metadata_ttl,  # Dynamic TTL
                invalidation_strategy=CacheInvalidationStrategy.BUST_ON_WRITE,
                dependencies=["metadata_dimensions"]
            ),

            # Scenario outputs queries
            QueryPattern(
                name="scenario_outputs_by_scenario",
                query_type=QueryType.SCENARIO_OUTPUTS,
                pattern=r"SELECT.*FROM.*scenario_output.*WHERE.*scenario_id.*=.*'(?P<scenario_id>[\w-]+)'.*ORDER BY.*timestamp.*DESC.*LIMIT.*(?P<limit>\d+)",
                key_template="scenario_outputs:{scenario_id}:limit_{limit}",
                ttl_seconds=scenario_ttl,  # Dynamic TTL
                invalidation_strategy=CacheInvalidationStrategy.BUST_ON_WRITE,
                dependencies=["scenario_output"]
            ),

            # Scenario metrics queries
            QueryPattern(
                name="scenario_metrics_latest",
                query_type=QueryType.SCENARIO_METRICS,
                pattern=r"SELECT.*FROM.*scenario_output.*WHERE.*scenario_id.*=.*'(?P<scenario_id>[\w-]+)'.*AND.*metric_name.*=.*'(?P<metric_name>[\w_]+)'.*ORDER BY.*timestamp.*DESC.*LIMIT.*1",
                key_template="scenario_metrics:{scenario_id}:{metric_name}",
                ttl_seconds=scenario_metrics_ttl,  # Dynamic TTL
                invalidation_strategy=CacheInvalidationStrategy.BUST_ON_WRITE,
                dependencies=["scenario_output"]
            ),

            # Aggregation queries
            QueryPattern(
                name="aggregation_by_date",
                query_type=QueryType.AGGREGATION,
                pattern=r"SELECT.*(?P<aggregate_func>AVG|SUM|COUNT|MIN|MAX)\((?P<column>\w+)\).*FROM.*(?P<table>\w+).*WHERE.*date.*=.*'(?P<date>\d{4}-\d{2}-\d{2})'",
                key_template="agg:{aggregate_func}:{column}:{table}:{date}",
                ttl_seconds=int(curve_ttl * 0.5),  # Shorter TTL for aggregations
                invalidation_strategy=CacheInvalidationStrategy.BUST_ON_WRITE,
                dependencies=["aggregation_table"]
            ),

            # Curve diff queries
            QueryPattern(
                name="curve_diff_by_dates",
                query_type=QueryType.CURVE_DIFF,
                pattern=r"SELECT.*FROM.*curve_observation.*WHERE.*asof.*IN.*\('(?P<asof_a>\d{4}-\d{2}-\d{2})'.*,\s*'(?P<asof_b>\d{4}-\d{2}-\d{2})'\).*AND.*iso.*=.*'(?P<iso>[A-Z]{3})'",
                key_template="curve_diff:{asof_a}:{asof_b}:{iso}",
                ttl_seconds=diff_ttl,  # Dynamic TTL
                invalidation_strategy=CacheInvalidationStrategy.BUST_ON_WRITE,
                dependencies=["curve_observation"]
            ),

            # Curve strips queries
            QueryPattern(
                name="curve_strips_by_type",
                query_type=QueryType.CURVE_STRIPS,
                pattern=r"SELECT.*FROM.*curve_observation.*WHERE.*asof.*=.*'(?P<asof>\d{4}-\d{2}-\d{2})'.*AND.*tenor_type.*=.*'(?P<tenor_type>\w+)'",
                key_template="curve_strips:{asof}:{tenor_type}",
                ttl_seconds=strips_ttl,  # Dynamic TTL
                invalidation_strategy=CacheInvalidationStrategy.BUST_ON_WRITE,
                dependencies=["curve_observation"]
            ),
        ]

        for pattern in patterns:
            self._patterns[pattern.name] = pattern

    def register_pattern(self, pattern: QueryPattern) -> None:
        """Register a custom query pattern."""
        self._patterns[pattern.name] = pattern
        log_structured(
            "info",
            "golden_query_pattern_registered",
            pattern_name=pattern.name,
            query_type=pattern.query_type.value,
            ttl_seconds=pattern.ttl_seconds
        )

    def _generate_query_hash(self, query: str) -> str:
        """Generate hash for a query."""
        return hashlib.sha256(query.encode()).hexdigest()[:16]

    def _find_matching_pattern(self, query: str) -> Optional[QueryPattern]:
        """Find the first matching pattern for a query."""
        for pattern in self._patterns.values():
            if pattern.matches(query):
                return pattern
        return None

    async def get_or_compute(
        self,
        query: str,
        compute_func: Callable[[], Awaitable[Any]],
        ttl_seconds: Optional[int] = None,
        force_refresh: bool = False
    ) -> Any:
        """Get cached result or compute and cache it."""
        tenant_id = get_tenant_id()
        query_hash = self._generate_query_hash(query)

        # Find matching pattern
        pattern = self._find_matching_pattern(query)
        if not pattern:
            # No pattern match, use simple caching
            key = f"query:{query_hash}"
            ttl = ttl_seconds or 300  # 5 minutes default
        else:
            # Use pattern-based caching
            params = pattern.extract_params(query)
            key = pattern.generate_key(params, tenant_id)
            ttl = ttl_seconds or pattern.ttl_seconds

        # Try to get from cache
        if not force_refresh:
            cached_value = await self.cache.get(key)
            if cached_value is not None:
                # Update stats
                async with self._lock:
                    if key not in self._query_stats:
                        self._query_stats[key] = {
                            "hits": 0,
                            "misses": 0,
                            "last_access": time.time()
                        }
                    self._query_stats[key]["hits"] += 1
                    self._query_stats[key]["last_access"] = time.time()

                log_structured(
                    "debug",
                    "golden_query_cache_hit",
                    query_hash=query_hash,
                    cache_key=key,
                    tenant_id=tenant_id
                )
                return cached_value

        # Cache miss or force refresh - compute value
        try:
            value = await compute_func()
            if value is not None:
                await self._cache_with_metadata(key, value, query, pattern, tenant_id, ttl)

                # Update stats
                async with self._lock:
                    if key not in self._query_stats:
                        self._query_stats[key] = {
                            "hits": 0,
                            "misses": 0,
                            "last_access": time.time()
                        }
                    self._query_stats[key]["misses"] += 1
                    self._query_stats[key]["last_access"] = time.time()

                log_structured(
                    "info",
                    "golden_query_cache_miss",
                    query_hash=query_hash,
                    cache_key=key,
                    tenant_id=tenant_id,
                    ttl_seconds=ttl
                )
            return value
        except Exception as e:
            log_structured(
                "error",
                "golden_query_cache_compute_error",
                query_hash=query_hash,
                cache_key=key,
                tenant_id=tenant_id,
                error=str(e)
            )
            raise

    async def _cache_with_metadata(
        self,
        key: str,
        value: Any,
        query: str,
        pattern: Optional[QueryPattern],
        tenant_id: str,
        ttl_seconds: int
    ) -> None:
        """Cache value with metadata."""
        query_hash = self._generate_query_hash(query)

        entry = CacheEntry(
            value=value,
            created_at=time.time(),
            ttl_seconds=ttl_seconds,
            query_type=pattern.query_type if pattern else QueryType.CUSTOM,
            query_hash=query_hash,
            tenant_id=tenant_id,
            dependencies=pattern.dependencies if pattern else [],
            tags=[pattern.name] if pattern else ["custom"]
        )

        await self.cache.set(key, entry.__dict__, ttl_seconds)

    async def invalidate_pattern(
        self,
        pattern_name: str,
        tenant_id: Optional[str] = None
    ) -> int:
        """Invalidate cache entries matching a pattern."""
        tenant_id = tenant_id or get_tenant_id()
        invalidated_count = 0

        # Find all cache keys matching the pattern
        pattern = self._patterns.get(pattern_name)
        if not pattern:
            log_structured(
                "warning",
                "golden_query_pattern_not_found",
                pattern_name=pattern_name
            )
            return 0

        # For now, we'll implement a simple invalidation
        # In a production system, this would scan for matching keys
        cache_keys_to_invalidate = [
            key for key in self._query_stats.keys()
            if key.startswith(f"{tenant_id}:{pattern.query_type.value}")
        ]

        for key in cache_keys_to_invalidate:
            await self.cache.delete(key)
            invalidated_count += 1

        log_structured(
            "info",
            "golden_query_pattern_invalidated",
            pattern_name=pattern_name,
            tenant_id=tenant_id,
            invalidated_count=invalidated_count
        )

        return invalidated_count

    async def invalidate_dependencies(
        self,
        dependencies: List[str],
        tenant_id: Optional[str] = None
    ) -> int:
        """Invalidate cache entries based on data dependencies."""
        tenant_id = tenant_id or get_tenant_id()
        invalidated_count = 0

        # Find all patterns that depend on the given dependencies
        dependent_patterns = [
            pattern for pattern in self._patterns.values()
            if any(dep in pattern.dependencies for dep in dependencies)
        ]

        for pattern in dependent_patterns:
            count = await self.invalidate_pattern(pattern.name, tenant_id)
            invalidated_count += count

        log_structured(
            "info",
            "golden_query_dependencies_invalidated",
            dependencies=dependencies,
            tenant_id=tenant_id,
            invalidated_count=invalidated_count,
            dependent_patterns=[p.name for p in dependent_patterns]
        )

        return invalidated_count

    async def invalidate_on_asof_publish(
        self,
        table_name: str,
        asof_date: str,
        tenant_id: Optional[str] = None
    ) -> int:
        """Invalidate cache entries when new as-of data is published."""
        tenant_id = tenant_id or get_tenant_id()
        invalidated_count = 0

        # Find patterns that depend on the published table and as-of date
        asof_patterns = [
            "curve_data_by_date",
            "curve_data_by_range",
            "curve_diff_by_dates",
            "curve_strips_by_type"
        ]

        for pattern_name in asof_patterns:
            pattern = self._patterns.get(pattern_name)
            if pattern and table_name in pattern.dependencies:
                # For as-of patterns, we need to invalidate all entries that might be affected
                # This is a simplified approach - in production would use more sophisticated pattern matching
                cache_keys_to_invalidate = [
                    key for key in self._query_stats.keys()
                    if key.startswith(f"{tenant_id}:{pattern.query_type.value}")
                ]

                for key in cache_keys_to_invalidate:
                    await self.cache.delete(key)
                    invalidated_count += 1

        log_structured(
            "info",
            "golden_query_asof_publish_invalidation",
            table_name=table_name,
            asof_date=asof_date,
            tenant_id=tenant_id,
            invalidated_count=invalidated_count
        )

        return invalidated_count

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        base_stats = await self.cache.get_stats()

        async with self._lock:
            total_queries = len(self._query_stats)
            total_hits = sum(stats.get("hits", 0) for stats in self._query_stats.values())
            total_misses = sum(stats.get("misses", 0) for stats in self._query_stats.values())

            # Calculate hit rate
            total_requests = total_hits + total_misses
            hit_rate = total_hits / total_requests if total_requests > 0 else 0

            # Calculate cache efficiency metrics
            cache_efficiency = hit_rate * 100
            average_hit_rate = 0
            if total_queries > 0:
                pattern_hit_rates = []
                for pattern in self._patterns.values():
                    pattern_queries = [
                        key for key in self._query_stats.keys()
                        if f":{pattern.query_type.value}:" in key
                    ]
                    pattern_hits = sum(
                        self._query_stats[key].get("hits", 0)
                        for key in pattern_queries
                    )
                    pattern_misses = sum(
                        self._query_stats[key].get("misses", 0)
                        for key in pattern_queries
                    )
                    if pattern_hits + pattern_misses > 0:
                        pattern_hit_rates.append(pattern_hits / (pattern_hits + pattern_misses))

                if pattern_hit_rates:
                    average_hit_rate = sum(pattern_hit_rates) / len(pattern_hit_rates)

            # Get pattern-specific stats
            pattern_stats = {}
            for pattern in self._patterns.values():
                pattern_queries = [
                    key for key in self._query_stats.keys()
                    if f":{pattern.query_type.value}:" in key
                ]
                pattern_hits = sum(
                    self._query_stats[key].get("hits", 0)
                    for key in pattern_queries
                )
                pattern_misses = sum(
                    self._query_stats[key].get("misses", 0)
                    for key in pattern_queries
                )
                pattern_requests = pattern_hits + pattern_misses
                pattern_hit_rate = pattern_hits / pattern_requests if pattern_requests > 0 else 0

                pattern_stats[pattern.name] = {
                    "query_type": pattern.query_type.value,
                    "total_queries": len(pattern_queries),
                    "hits": pattern_hits,
                    "misses": pattern_misses,
                    "hit_rate": pattern_hit_rate,
                    "ttl_seconds": pattern.ttl_seconds
                }

        return {
            **base_stats,
            "golden_queries": {
                "total_queries": total_queries,
                "total_hits": total_hits,
                "total_misses": total_misses,
                "hit_rate": hit_rate,
                "cache_efficiency_percent": cache_efficiency,
                "average_pattern_hit_rate": average_hit_rate,
                "patterns": pattern_stats,
                "last_updated": time.time()
            },
            "cache_performance": {
                "redis_enabled": "redis_used_memory" in base_stats,
                "memory_efficiency": f"{base_stats.get('memory_size_bytes', 0) / (1024 * 1024):.2f}MB" if "memory_size_bytes" in base_stats else "0MB",
                "total_cache_requests": total_requests,
                "cache_savings_ratio": hit_rate if total_requests > 0 else 0
            }
        }

    async def warm_cache(
        self,
        warmup_queries: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Warm the cache with predefined queries."""
        tenant_id = get_tenant_id()
        results = {
            "total": len(warmup_queries),
            "successful": 0,
            "failed": 0,
            "errors": []
        }

        for query_info in warmup_queries:
            try:
                query = query_info["query"]
                compute_func = query_info.get("compute_func")

                if compute_func:
                    await self.get_or_compute(query, compute_func)
                    results["successful"] += 1
                else:
                    results["errors"].append({
                        "query": query,
                        "error": "No compute function provided"
                    })
                    results["failed"] += 1
            except Exception as e:
                results["errors"].append({
                    "query": query_info.get("query", "unknown"),
                    "error": str(e)
                })
                results["failed"] += 1

        log_structured(
            "info",
            "golden_query_cache_warmed",
            tenant_id=tenant_id,
            total_queries=results["total"],
            successful_warmups=results["successful"],
            failed_warmups=results["failed"]
        )

        return results

    async def cleanup_expired_entries(self) -> int:
        """Clean up expired cache entries."""
        tenant_id = get_tenant_id()
        cleaned_count = 0

        # This is a simplified cleanup - in production would scan for expired entries
        # For now, we'll rely on TTL-based expiration in the cache backend

        log_structured(
            "info",
            "golden_query_cache_cleanup_completed",
            tenant_id=tenant_id,
            cleaned_count=cleaned_count
        )

        return cleaned_count


# Global golden query cache instance
_golden_query_cache: Optional[GoldenQueryCache] = None


def get_golden_query_cache() -> GoldenQueryCache:
    """Get the global golden query cache instance."""
    if _golden_query_cache is None:
        raise RuntimeError("GoldenQueryCache not initialized")
    return _golden_query_cache


async def initialize_golden_query_cache(
    cache_service: AsyncCache
) -> GoldenQueryCache:
    """Initialize the global golden query cache."""
    global _golden_query_cache
    if _golden_query_cache is None:
        _golden_query_cache = GoldenQueryCache(cache_service)
    return _golden_query_cache


# === DECORATORS FOR EASY CACHE INTEGRATION ===

def cache_golden_query(
    query_type: QueryType,
    ttl_seconds: Optional[int] = None,
    pattern_name: Optional[str] = None
):
    """Decorator to cache golden queries."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate query string from function call
            query_signature = f"{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
            query_hash = hashlib.sha256(query_signature.encode()).hexdigest()[:16]

            async def compute_func():
                return await func(*args, **kwargs)

            cache = get_golden_query_cache()
            return await cache.get_or_compute(
                query=query_signature,
                compute_func=compute_func,
                ttl_seconds=ttl_seconds
            )
        return wrapper
    return decorator


def invalidate_on_write(table_name: str, operation: str = "INSERT"):
    """Decorator to invalidate cache when data is written."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)

            # Invalidate cache based on table dependencies
            cache = get_golden_query_cache()
            await cache.invalidate_dependencies([table_name])

            return result
        return wrapper
    return decorator


# === UTILITY FUNCTIONS ===

async def get_query_patterns() -> Dict[str, Dict[str, Any]]:
    """Get all registered query patterns."""
    cache = get_golden_query_cache()
    patterns = {}

    for name, pattern in cache._patterns.items():
        patterns[name] = {
            "name": pattern.name,
            "query_type": pattern.query_type.value,
            "pattern": pattern.pattern,
            "key_template": pattern.key_template,
            "ttl_seconds": pattern.ttl_seconds,
            "invalidation_strategy": pattern.invalidation_strategy.value,
            "dependencies": pattern.dependencies
        }

    return patterns


async def register_custom_pattern(
    name: str,
    query_type: QueryType,
    pattern: str,
    key_template: str,
    ttl_seconds: int,
    invalidation_strategy: CacheInvalidationStrategy = CacheInvalidationStrategy.TTL_ONLY,
    dependencies: List[str] = None
) -> None:
    """Register a custom query pattern."""
    cache = get_golden_query_cache()

    custom_pattern = QueryPattern(
        name=name,
        query_type=query_type,
        pattern=pattern,
        key_template=key_template,
        ttl_seconds=ttl_seconds,
        invalidation_strategy=invalidation_strategy,
        dependencies=dependencies or []
    )

    cache.register_pattern(custom_pattern)


async def get_cache_performance_report() -> Dict[str, Any]:
    """Get a comprehensive cache performance report."""
    cache = get_golden_query_cache()
    stats = await cache.get_cache_stats()

    # Calculate additional metrics
    golden_stats = stats.get("golden_queries", {})
    total_queries = golden_stats.get("total_queries", 0)
    total_hits = golden_stats.get("total_hits", 0)
    total_misses = golden_stats.get("total_misses", 0)

    # Performance metrics
    hit_rate = total_hits / (total_hits + total_misses) if (total_hits + total_misses) > 0 else 0
    cache_efficiency = hit_rate * 100

    # Memory usage estimation
    memory_entries = stats.get("memory_entries", 0)
    memory_size = stats.get("memory_size_bytes", 0)

    return {
        "summary": {
            "total_queries": total_queries,
            "cache_hits": total_hits,
            "cache_misses": total_misses,
            "hit_rate": hit_rate,
            "cache_efficiency_percent": cache_efficiency,
            "memory_entries": memory_entries,
            "memory_size_mb": memory_size / (1024 * 1024)
        },
        "patterns": golden_stats.get("patterns", {}),
        "backend_stats": {
            "backend": stats.get("backend"),
            "redis_used_memory": stats.get("redis_used_memory", 0),
            "redis_used_memory_human": stats.get("redis_used_memory_human", "0B")
        },
        "recommendations": generate_cache_recommendations(stats)
    }


def generate_cache_recommendations(stats: Dict[str, Any]) -> List[str]:
    """Generate recommendations based on cache performance."""
    recommendations = []
    golden_stats = stats.get("golden_queries", {})

    hit_rate = golden_stats.get("total_hits", 0) / max(
        golden_stats.get("total_hits", 0) + golden_stats.get("total_misses", 0), 1
    )

    if hit_rate < 0.5:
        recommendations.append("Consider increasing cache TTL for frequently accessed queries")
        recommendations.append("Review query patterns to identify cache misses")

    if stats.get("memory_entries", 0) > 10000:
        recommendations.append("Consider implementing cache size limits or LRU eviction")

    patterns = golden_stats.get("patterns", {})
    for pattern_name, pattern_stats in patterns.items():
        if pattern_stats.get("misses", 0) > pattern_stats.get("hits", 0):
            recommendations.append(f"Pattern '{pattern_name}' has more misses than hits - consider optimization")

    return recommendations


# === AS-OF PUBLISH INVALIDATION UTILITIES ===

async def invalidate_cache_on_data_publish(
    table_name: str,
    asof_date: str,
    tenant_id: Optional[str] = None
) -> int:
    """Utility function to invalidate cache when new data is published."""
    cache = get_golden_query_cache()
    return await cache.invalidate_on_asof_publish(table_name, asof_date, tenant_id)


def register_publish_hook(
    table_name: str,
    invalidation_func: Optional[Callable[[str, str, Optional[str]], Awaitable[int]]] = None
):
    """Decorator to register a function to be called when data is published for a table."""
    if invalidation_func is None:
        invalidation_func = invalidate_cache_on_data_publish

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)

            # Extract asof_date from result or kwargs if available
            asof_date = kwargs.get('asof_date') or getattr(result, 'asof_date', None)
            if asof_date:
                tenant_id = kwargs.get('tenant_id') or get_tenant_id()
                await invalidation_func(table_name, asof_date, tenant_id)

            return result
        return wrapper
    return decorator
