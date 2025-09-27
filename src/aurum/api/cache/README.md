# Cache System - Unified Interface

This module provides a comprehensive, unified caching system for the Aurum API that consolidates all previous cache implementations.

## Key Features

- **Unified Interface**: Single API for all caching operations
- **TTL Governance**: Standardized TTL policies with automatic enforcement
- **Multiple Backends**: Redis primary, memory fallback
- **Advanced Analytics**: Comprehensive metrics and health monitoring
- **Cache Warming**: Automatic cache pre-population
- **Pattern Invalidation**: Flexible cache invalidation by patterns
- **Background Maintenance**: Automatic cleanup and health checks

## Quick Start

```python
from aurum.api.cache import get_unified_cache_manager, CacheNamespace, TTLPolicy

# Get the unified cache manager
cache = get_unified_cache_manager()

# Simple get/set operations
await cache.set("key", "value", namespace=CacheNamespace.CURVES, ttl_policy=TTLPolicy.SHORT)
value = await cache.get("key", namespace=CacheNamespace.CURVES)

# Pattern invalidation
count = await cache.invalidate_pattern("curves:*", namespace=CacheNamespace.CURVES)

# Get analytics
analytics = await cache.get_analytics()
health = await cache.get_health()
```

## Migration from Legacy Cache

### Old Way (Deprecated)
```python
from aurum.api.cache import AsyncCache, CacheBackend
cache = AsyncCache(backend=CacheBackend.REDIS)
await cache.set("key", "value")
```

### New Way (Recommended)
```python
from aurum.api.cache import get_unified_cache_manager, CacheNamespace, TTLPolicy
cache = get_unified_cache_manager()
await cache.set("key", "value", namespace=CacheNamespace.GENERAL, ttl_policy=TTLPolicy.MEDIUM)
```

## TTL Policies

The system provides standardized TTL policies:

- `ULTRA_SHORT` (30s): Real-time data
- `SHORT` (5m): Frequently changing data
- `MEDIUM` (30m): Semi-static data
- `LONG` (4h): Stable data
- `EXTENDED` (24h): Daily aggregates
- `PERSISTENT` (7d): Configuration data

## Cache Namespaces

Organize cache keys by namespace:

- `CURVES`: Curve data
- `SCENARIOS`: Scenario data
- `METADATA`: System metadata
- `EIA_DATA`: EIA data
- `EXTERNAL_DATA`: External data sources
- `USER_DATA`: User-specific data
- `SYSTEM_CONFIG`: System configuration

## Advanced Features

### Cache Warming
```python
from aurum.api.cache import CacheWarmingConfig

warming_config = CacheWarmingConfig(
    enabled=True,
    warmup_keys=["important_key_1", "important_key_2"],
    warmup_fetcher=my_fetcher_function,
    warmup_concurrency=5
)
cache.configure_warming(warming_config)
```

### Health Monitoring
```python
health = await cache.get_health()
if not health.is_healthy:
    print(f"Cache unhealthy: {health.error_rate:.2%} error rate")

analytics = await cache.get_analytics()
print(f"Hit rate: {analytics.hit_rate:.2%}")
```

### Pattern Invalidation with Tracking
```python
count = await cache.invalidate_pattern(
    "curves:*",
    namespace=CacheNamespace.CURVES,
    reason="data_update",
    triggered_by="curve_service"
)

events = await cache.get_invalidation_events(since=datetime.utcnow() - timedelta(hours=1))
```

## Configuration

The cache manager automatically configures itself based on your Aurum settings:

```python
from aurum.api.config import CacheConfig

config = CacheConfig.from_settings(get_settings())
cache = get_unified_cache_manager(config)
```

## Metrics

The cache system automatically collects metrics:

- Cache hits/misses
- Response times
- Error rates
- Memory usage (Redis)
- Invalidation counts

Metrics are available through the standard Aurum metrics interface.
