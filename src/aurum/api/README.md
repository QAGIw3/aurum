# Aurum API Refactored Architecture

This document describes the refactored Aurum API architecture that provides better maintainability, performance, and testability.

## Overview

The refactored API introduces several key improvements:

- **Modular Architecture**: Split monolithic routes.py into focused modules
- **Dependency Injection**: Service provider pattern for better testability
- **Async Support**: Non-blocking operations throughout
- **Enhanced Caching**: Multi-level caching with Redis and in-memory backends
- **Comprehensive Error Handling**: Standardized exception hierarchy
- **Data Processing Pipeline**: Streaming, validation, and enrichment
- **Performance Monitoring**: Built-in metrics and optimization

## Architecture

### Core Components

#### 1. API Modules
- `health.py` - Health checks and monitoring
- `curves.py` - Curve data endpoints
- `metadata.py` - Metadata and reference data
- `scenarios.py` - Scenario management endpoints
- `handlers/external.py` - External data API handlers
- `ppa.py` - PPA valuation endpoints (planned)

#### 2. Service Layer
- `container.py` - Dependency injection container
- `async_service.py` - Async service wrappers
- `performance.py` - Query optimization and connection pooling
- `data_processing.py` - Data validation and enrichment pipeline

#### 3. Infrastructure
- `cache.py` - Enhanced caching system
- `exceptions.py` - Standardized error handling
- `config.py` - Simplified configuration management
- `testing.py` - Enhanced testing utilities

#### 4. HTTP Utilities
- `http/responses.py` - Response handling (ETags, CSV, errors)
- `http/pagination.py` - Cursor-based pagination utilities
- `http/__init__.py` - HTTP utilities package exports

## Usage

### Basic Setup

```python
from aurum.api import (
    create_app,
    configure_services,
    get_service,
    health_router,
    curves_router
)

# Configure services
configure_services()

# Create FastAPI app
app = create_app()

# Include routers
app.include_router(health_router)
app.include_router(curves_router)
```

### Dependency Injection

```python
from aurum.api.container import get_service_provider
from aurum.api.async_service import AsyncCurveService

# Get service provider
provider = get_service_provider()

# Get services
curve_service = provider.get(AsyncCurveService)
cache_manager = provider.get(CacheManager)

# Use services
curves, meta = await curve_service.fetch_curves(
    iso="PJM",
    market="DAY_AHEAD",
    location="HUB"
)
```

### Error Handling

```python
from aurum.api.exceptions import (
    AurumAPIException,
    ValidationException,
    NotFoundException
)

try:
    data = await api_call()
except ValidationException as e:
    # Handle validation errors
    print(f"Validation error: {e.detail}")
    print(f"Field errors: {e.context.get('field_errors', {})}")
except AurumAPIException as e:
    # Handle API errors
    print(f"API error: {e.detail}")
```

### HTTP Response Utilities

```python
from aurum.api.http import respond_with_etag, create_error_response

# Add ETag handling to your endpoint
return respond_with_etag(model, request, response)

# Create standardized error responses
error_response = create_error_response(
    400,
    "Invalid request parameters",
    request_id=get_request_id(),
    field_errors={"tenant_id": "Required field"}
)
```

### Pagination Utilities

```python
from aurum.api.http import encode_cursor, decode_cursor, normalize_cursor_input

# Generate next cursor
next_cursor = encode_cursor({"offset": offset + limit})

# Decode incoming cursor
payload = decode_cursor(cursor_token)
effective_offset, metadata = normalize_cursor_input(payload)
```

## Configuration

The API uses environment-based configuration through the AurumSettings class:

```python
from aurum.core import AurumSettings

# Environment-based configuration
settings = AurumSettings.from_env()

# Access settings
print(f"Cache TTL: {settings.api.cache.metadata_ttl}")
print(f"API timeout: {settings.api.request_timeout_seconds}")
print(f"CORS origins: {settings.api.cors_allow_origins}")
```

## Testing

The API is designed for easy testing with dependency injection. Use standard Python testing libraries like pytest and mock:

```python
from unittest.mock import AsyncMock, patch
from aurum.api.container import get_service

# Mock the service layer
with patch('aurum.api.container.get_service') as mock_get_service:
    mock_service = AsyncMock()
    mock_get_service.return_value = mock_service

    # Test your endpoint
    response = client.get("/v1/scenarios")
    assert response.status_code == 200
```

## Performance Optimizations

The API includes built-in performance optimizations:

- **Async Operations**: All I/O operations are async for better concurrency
- **Caching**: Multi-level caching with Redis and in-memory backends
- **Connection Pooling**: Database connections are pooled and reused
- **Query Optimization**: Large queries are automatically optimized
- **Streaming Responses**: Large datasets are streamed to avoid memory issues

## Migration Guide

### HTTP Utilities Migration

**Before:**
```python
from aurum.api.routes import _respond_with_etag, _encode_cursor, _decode_cursor

return _respond_with_etag(model, request, response)
next_cursor = _encode_cursor({"offset": offset + limit})
payload = _decode_cursor(cursor_token)
```

**After:**
```python
from aurum.api.http import respond_with_etag, encode_cursor, decode_cursor

return respond_with_etag(model, request, response)
next_cursor = encode_cursor({"offset": offset + limit})
payload = decode_cursor(cursor_token)
```

### Error Handling

The API provides structured error responses with RFC 7807 compliance:

```python
try:
    # API call
    pass
except ValidationException as e:
    # Handle validation errors with field-specific details
    print(f"Validation error: {e.detail}")
    print(f"Field errors: {e.context.get('field_errors', {})}")
except NotFoundException as e:
    # Handle not found errors
    print(f"Resource not found: {e.resource_type}")
except AurumAPIException as e:
    # Handle other API errors
    print(f"API error: {e.detail}")
```

## Benefits

### Performance
- **Async-first design** enables better concurrency and resource utilization
- **Multi-level caching** with Redis and in-memory backends reduces database load
- **Connection pooling** optimizes database connections
- **Streaming responses** handle large datasets efficiently
- **ETag support** reduces unnecessary data transfer

### Maintainability
- **Modular architecture** with clear separation of concerns
- **HTTP utilities package** centralizes common functionality
- **Consistent error handling** with structured error responses
- **Comprehensive documentation** with examples and migration guides

### Testability
- **Dependency injection** enables easy mocking and testing
- **Standardized HTTP utilities** make testing more predictable
- **Clear interfaces** between modules reduce coupling

### Reliability
- **Robust error handling** with RFC 7807 compliant responses
- **Cursor-based pagination** provides stable, performant pagination
- **Structured logging** with request correlation IDs
- **Graceful degradation** when external services are unavailable

## Best Practices

### Async/Await Usage
- Use `async/await` for all I/O operations
- Avoid blocking operations in async functions
- Use `asyncio.gather()` for concurrent operations

### Error Handling
- Always catch `AurumAPIException` for API-specific errors
- Use specific exception types for different error conditions
- Include request context in error responses

### Caching
- Use appropriate TTL values for different data types
- Implement cache warming for frequently accessed data
- Monitor cache hit rates and adjust strategies accordingly

### Testing
- Use `MockServiceProvider` for unit tests
- Create realistic test data with `TestDataFactory`
- Include performance tests for critical paths

### Configuration
- Use environment-based configuration
- Validate configuration at startup
- Provide sensible defaults for all settings

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are installed
2. **Service Not Found**: Check service registration in container
3. **Cache Connection Failed**: Verify Redis configuration
4. **Performance Issues**: Check query optimization and caching

### Debug Mode

Enable debug mode for detailed logging:

```python
import os
os.environ["AURUM_DEBUG"] = "true"
```

### Performance Monitoring

Check performance metrics:

```python
from aurum.api.performance import get_performance_monitor

monitor = get_performance_monitor()
stats = await monitor.get_stats()
print(f"Cache hit rate: {stats['cache_hit_rate']:.1%}")
```

## Contributing

When contributing to the refactored API:

1. **Follow the modular structure** - Add new endpoints to appropriate modules
2. **Use dependency injection** - Register services in the container
3. **Implement async patterns** - Use async/await for all operations
4. **Add comprehensive tests** - Include unit and integration tests
5. **Update documentation** - Add examples for new features

## Conclusion

The refactored Aurum API provides a solid foundation for scalable, maintainable, and performant market intelligence services. The modular architecture, dependency injection, and async patterns enable better development practices and improved system reliability.

For detailed examples, see `examples.py`. For testing utilities, see `testing.py`.
