# Enhanced Tenant-Aware Rate Limiting

This module provides production-ready, tenant-aware rate limiting with circuit breaker behavior and graceful degradation.

## Features

### 🎯 Tenant-Aware Limiting
- **Per-Tenant Quotas**: Different rate limits based on tenant tier (Free, Basic, Premium, Enterprise)
- **Dynamic Adjustment**: Quotas automatically adjust based on system load
- **Tenant Overrides**: Custom limits for specific tenants
- **Tier Detection**: Automatic tier assignment based on tenant ID patterns

### 🛡️ Circuit Breaker Pattern
- **Automatic Recovery**: Opens when cache failures occur, closes when service restored
- **Graceful Degradation**: Falls back to in-memory limiting when Redis is unavailable
- **Failure Tracking**: Tracks and reports cache failures for monitoring
- **Configurable Thresholds**: Adjustable failure thresholds and reset timeouts

### 📊 Comprehensive Monitoring
- **Detailed Metrics**: Rate limit events, active windows, block durations by tenant and tier
- **Circuit Breaker State**: Real-time circuit breaker status
- **Cache Failures**: Tracking of Redis/cache failures
- **System Load**: Dynamic load calculation and quota adjustment

### ⚙️ Admin Interface
- **Tenant Inspection**: View rate limiting status for all tenants
- **Circuit Breaker Control**: Reset circuit breaker state
- **Quota Management**: Inspect and modify tenant quotas
- **Reset Operations**: Reset rate limiting state for specific tenants

## Architecture

```
┌─────────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│   API Request   │───▶│ Tenant Rate Limiter │───▶│ Redis Cache      │
│ (with tenant)   │    │                     │    │ (Primary)        │
└─────────────────┘    │ - Tier Detection    │    └──────────────────┘
                       │ - Quota Application │            │
                       │ - Circuit Breaker   │    ┌──────────────────┐
                       │ - Metrics Recording │───▶│ In-Memory Fallback│
                       └─────────────────────┘    │ (Graceful Deg.)  │
                                                └──────────────────┘
```

## Configuration

### Environment Variables

```bash
# Redis configuration for distributed limiting
AURUM_API_CACHE_REDIS_URL=redis://localhost:6379/0

# Circuit breaker settings
AURUM_API_RATE_LIMIT_CIRCUIT_THRESHOLD=5
AURUM_API_RATE_LIMIT_CIRCUIT_RESET_TIMEOUT=60

# Tenant identification
AURUM_API_RATE_LIMIT_TENANT_HEADERS=X-Tenant-ID,X-Aurum-Tenant

# Default quotas per tier
AURUM_API_RATE_LIMIT_FREE_RPS=5
AURUM_API_RATE_LIMIT_FREE_BURST=10
AURUM_API_RATE_LIMIT_BASIC_RPS=20
AURUM_API_RATE_LIMIT_BASIC_BURST=40
AURUM_API_RATE_LIMIT_PREMIUM_RPS=100
AURUM_API_RATE_LIMIT_PREMIUM_BURST=200
AURUM_API_RATE_LIMIT_ENTERPRISE_RPS=500
AURUM_API_RATE_LIMIT_ENTERPRISE_BURST=1000
```

### Tenant Tiers

| Tier | RPS | Burst | Max Concurrent | Priority Boost |
|------|-----|-------|---------------|----------------|
| Free | 5 | 10 | 50 | 0.5 |
| Basic | 20 | 40 | 100 | 0.8 |
| Premium | 100 | 200 | 500 | 1.0 |
| Enterprise | 500 | 1000 | 1000 | 1.2 |

### Tenant-Specific Overrides

Configure custom quotas for specific tenants:

```python
from aurum.api.ratelimit_enhanced import get_rate_limit_manager
from aurum.api.ratelimit_config import TenantQuotaConfig

manager = get_rate_limit_manager()
manager._tenant_overrides["special-tenant"] = TenantQuotaConfig(
    requests_per_second=200,
    burst_size=400,
    max_concurrent_requests=200,
    priority_boost=1.5
)
```

## Usage

### Basic Integration

```python
from aurum.api.ratelimit_integration import create_rate_limit_middleware

# Create middleware
middleware = create_rate_limit_middleware(app, settings)

# Use in FastAPI app
app.add_middleware(middleware)
```

### Manual Rate Limit Checking

```python
from aurum.api.ratelimit_enhanced import get_rate_limit_manager

manager = get_rate_limit_manager()

# Check rate limit for tenant
allowed, retry_after, metadata = await manager.check_rate_limit(
    tenant_id="acme-corp",
    path="/v1/scenarios",
    redis_client=redis_client
)

if not allowed:
    raise HTTPException(
        status_code=429,
        detail=f"Rate limit exceeded. Retry after {retry_after} seconds",
        headers={"Retry-After": str(int(retry_after))}
    )
```

### Tenant Status Inspection

```python
from aurum.api.ratelimit_enhanced import get_rate_limit_manager

manager = get_rate_limit_manager()

# Get status for specific tenant
status = manager.get_tenant_status("acme-corp")
print(f"Tenant: {status['tenant_id']}")
print(f"Tier: {status['tier']}")
print(f"Active windows: {status['active_windows']}")

# Get status for all tenants
all_status = manager.get_all_tenant_status()
```

## Admin Endpoints

The system provides admin endpoints for monitoring and management:

### GET /admin/ratelimit/tenants
List rate limiting status for all tenants.

### GET /admin/ratelimit/tenants/{tenant_id}
Get detailed status for a specific tenant.

### DELETE /admin/ratelimit/tenants/{tenant_id}/reset
Reset rate limiting state for a tenant.

### POST /admin/ratelimit/circuit-breaker/reset
Reset circuit breaker state.

### GET /admin/ratelimit/quotas
List quota configurations for all tenant tiers.

## Error Handling

The enhanced rate limiting provides structured error responses:

```json
{
  "error": {
    "type": "https://aurum.api/errors/rate_limit_exceeded",
    "title": "Rate Limit Exceeded",
    "detail": "Too many requests for tenant acme-corp",
    "retry_after": 60,
    "instance": "req-12345678"
  }
}
```

Response headers include:
- `Retry-After`: Seconds to wait before retrying
- `X-RateLimit-Tenant`: Tenant ID that was rate limited
- `X-RateLimit-Tier`: Tenant tier

## Circuit Breaker States

1. **CLOSED**: Normal operation, rate limiting working normally
2. **OPEN**: Cache failures detected, using in-memory fallback
3. **HALF_OPEN**: Testing recovery, limited operations allowed

The circuit breaker automatically transitions between states based on failure patterns.

## Graceful Degradation

When Redis or cache is unavailable, the system:

1. **Detects Failure**: Records cache failures and opens circuit breaker
2. **Falls Back**: Uses in-memory rate limiting as fallback
3. **Records Metrics**: Tracks cache failures for monitoring
4. **Attempts Recovery**: Periodically tests cache connectivity
5. **Recovers**: Automatically switches back to Redis when available

## Monitoring and Metrics

### Prometheus Metrics

- `aurum_api_tenant_ratelimit_total`: Rate limit decisions by result, path, tenant, and tier
- `aurum_api_tenant_ratelimit_active_windows`: Active rate limit windows per tenant
- `aurum_api_tenant_ratelimit_requests_per_window`: Request counts per window
- `aurum_api_tenant_ratelimit_block_duration_seconds`: Block durations
- `aurum_api_tenant_ratelimit_circuit_state`: Circuit breaker state
- `aurum_api_tenant_ratelimit_cache_failures_total`: Cache failure counts

### Health Checks

Monitor circuit breaker state and cache connectivity:

```python
from aurum.api.ratelimit_enhanced import get_rate_limit_manager

manager = get_rate_limit_manager()
status = {
    "circuit_breaker": manager._circuit_breaker_state.value,
    "cache_failures": manager._circuit_failure_count,
    "system_load": manager._system_load,
    "active_tenants": len(manager.get_all_tenant_status())
}
```

## Performance Considerations

### Memory Usage
- In-memory fallback uses bounded queues per tenant
- Default window size: 1000 requests per tenant/path
- Automatic cleanup of old entries

### Redis Load
- Minimal Redis operations per request
- Atomic operations using pipelines
- Automatic expiration of rate limit windows

### CPU Usage
- Efficient tenant ID extraction
- Cached quota calculations
- Minimal computation per request

## Security Considerations

- **Tenant Isolation**: Rate limits are strictly per-tenant
- **Header Validation**: Tenant ID headers are validated
- **Path Validation**: Request paths are validated before limiting
- **Admin Access**: Admin endpoints require proper authentication

## Troubleshooting

### Common Issues

1. **Circuit Breaker Opens Frequently**
   - Check Redis connectivity and performance
   - Monitor cache failure metrics
   - Consider increasing circuit breaker thresholds

2. **Rate Limits Too Restrictive**
   - Verify tenant tier detection
   - Check tenant-specific overrides
   - Monitor system load calculation

3. **High Memory Usage**
   - Check active tenant count
   - Verify old entries are cleaned up
   - Consider adjusting memory window size

4. **Redis Performance Issues**
   - Monitor Redis latency
   - Check connection pool size
   - Verify Redis memory usage

### Debug Mode

Enable debug logging for rate limiting:

```python
import logging
logging.getLogger('aurum.api.ratelimit_enhanced').setLevel(logging.DEBUG)
```

### Performance Profiling

Monitor rate limiting performance:

```python
from aurum.api.ratelimit_enhanced import get_rate_limit_manager

manager = get_rate_limit_manager()
stats = {
    "active_windows": len(manager._memory_windows),
    "circuit_state": manager._circuit_breaker_state.value,
    "cache_failures": manager._circuit_failure_count,
}
```

## Integration Examples

### FastAPI Integration

```python
from fastapi import FastAPI
from aurum.api.ratelimit_integration import create_rate_limit_middleware

app = FastAPI()
app.add_middleware(create_rate_limit_middleware(app, settings))

@app.get("/api/scenarios")
async def list_scenarios(request: Request):
    # Tenant ID is automatically extracted and rate limited
    return {"scenarios": []}
```

### Manual Rate Limiting

```python
from aurum.api.ratelimit_enhanced import get_rate_limit_manager

@app.post("/api/scenarios/{scenario_id}/run")
async def run_scenario(scenario_id: str, request: Request):
    tenant_id = get_tenant_id_from_request(request)

    manager = get_rate_limit_manager()
    allowed, retry_after, _ = await manager.check_rate_limit(
        tenant_id, request.url.path
    )

    if not allowed:
        raise HTTPException(429, "Rate limit exceeded")

    # Process request...
    return {"run_id": "run-123"}
```

## Migration Guide

### From Legacy Rate Limiting

1. **Replace Middleware**: Use `create_rate_limit_middleware` instead of `RateLimitMiddleware`
2. **Update Configuration**: Migrate to tenant-aware quota configuration
3. **Add Tenant Headers**: Include `X-Tenant-ID` in requests
4. **Monitor Circuit Breaker**: Set up alerts for circuit breaker state changes
5. **Test Fallback**: Verify in-memory fallback works during Redis outages

### Backward Compatibility

The enhanced system maintains backward compatibility with legacy rate limiting configuration. Existing applications will continue to work while gaining the benefits of enhanced features.
