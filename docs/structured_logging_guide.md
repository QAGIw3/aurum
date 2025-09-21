# Structured Logging and Correlation IDs Guide

This guide explains how to use Aurum's structured logging system with correlation IDs for end-to-end observability across the API and scenario worker.

## Overview

Aurum implements structured logging using JSON format with correlation IDs that are propagated end-to-end:

- **API Layer**: Request → Database queries → Response
- **Worker Layer**: Kafka message → Processing → Output
- **End-to-End**: API request → Worker processing → Database queries

## Key Features

- ✅ **Correlation IDs**: Automatic propagation of request IDs across all layers
- ✅ **Structured JSON Format**: Machine-readable logs for better observability
- ✅ **Context Variables**: Thread-safe context management using Python's `contextvars`
- ✅ **Multiple Context Types**: Request ID, Correlation ID, Tenant ID, User ID, Session ID
- ✅ **Performance**: <1ms per log call, suitable for production use
- ✅ **OpenTelemetry Integration**: Automatic span attributes and tracing context

## Quick Start

### Basic Usage

```python
from aurum.telemetry.context import log_structured

# Log a simple event
log_structured("info", "user_action", action="login", user_id="user123")

# Log with context (automatically includes correlation IDs)
with correlation_context(correlation_id="req-123", tenant_id="tenant-abc"):
    log_structured("info", "database_query", query="SELECT * FROM users", duration_ms=50)
```

### API Layer Integration

The API automatically sets up correlation context for each request:

```python
# In your API endpoint
@router.get("/api/example")
async def example_endpoint(request: Request):
    # Context is automatically set by access_log_middleware
    log_structured("info", "processing_request", endpoint="/api/example")

    # Business logic with structured logging
    result = await some_business_logic()
    log_structured("info", "business_logic_completed", result_count=len(result))

    return result
```

### Worker Layer Integration

The scenario worker automatically extracts correlation IDs from Kafka headers:

```python
# In the worker processing loop
with correlation_context(
    correlation_id=correlation_id,
    tenant_id=tenant_id,
    user_id=user_id
):
    log_structured("info", "scenario_processing_started", scenario_id=scenario_id)

    # Processing logic
    result = process_scenario(scenario_id)

    log_structured("info", "scenario_processing_completed",
                   scenario_id=scenario_id,
                   status="success",
                   processing_time_seconds=5.2)
```

## Context Management

### Context Variables

Aurum provides several context variables for tracking different aspects of requests:

- **`request_id`**: Unique identifier for the HTTP request
- **`correlation_id`**: ID that correlates across services (API → Worker → Database)
- **`tenant_id`**: Tenant/organization identifier
- **`user_id`**: User identifier
- **`session_id`**: Session identifier (usually same as request_id)

### Context Manager

Use the `correlation_context` context manager to set multiple context variables:

```python
from aurum.telemetry.context import correlation_context

with correlation_context(
    correlation_id="req-123-abc",
    tenant_id="tenant-456",
    user_id="user-789"
) as context:
    # All log calls within this block will include context
    log_structured("info", "processing_started", custom_field="value")
    # context dict is also available if needed
    print(f"Correlation ID: {context['correlation_id']}")
```

### Manual Context Management

For more control, manage context variables manually:

```python
from aurum.telemetry.context import set_correlation_id, get_correlation_id, reset_correlation_id

token = set_correlation_id("manual-corr-123")
try:
    log_structured("info", "manual_context_event")
finally:
    reset_correlation_id(token)
```

## Structured Logging Format

All structured logs are JSON formatted with these standard fields:

```json
{
  "timestamp": "2024-01-15T10:30:45.123456",
  "level": "info",
  "event": "scenario_processing_completed",
  "request_id": "req-123-abc-def",
  "correlation_id": "corr-456-ghi-jkl",
  "tenant_id": "tenant-789",
  "user_id": "user-101",
  "session_id": "session-123",
  "scenario_id": "scen-456",
  "status": "success",
  "processing_time_seconds": 5.2,
  "custom_field": "custom_value"
}
```

### Log Levels

- **`debug`**: Detailed debugging information
- **`info`**: General information (default)
- **`warning`**: Warning conditions
- **`error`**: Error conditions
- **`critical`**: Critical conditions

### Filtering None Values

None values are automatically filtered out to keep logs clean:

```python
log_structured("info", "event", key1="value1", key2=None, key3="value3")
# Results in: {"event": "event", "key1": "value1", "key3": "value3"}
```

## End-to-End Tracing Example

Here's how a request flows through the system with correlation IDs:

### 1. API Request

```bash
curl -H "X-Correlation-ID: req-123-abc" /api/scenarios/456/outputs
```

**API Log:**
```json
{
  "event": "http_request_completed",
  "method": "GET",
  "path": "/api/scenarios/456/outputs",
  "correlation_id": "req-123-abc",
  "tenant_id": "tenant-456",
  "status": 200,
  "duration_ms": 150
}
```

### 2. Worker Processing

**Kafka Headers:**
```
X-Correlation-ID: req-123-abc
X-Tenant-ID: tenant-456
```

**Worker Log:**
```json
{
  "event": "scenario_processing_completed",
  "correlation_id": "req-123-abc",
  "tenant_id": "tenant-456",
  "scenario_id": "456",
  "status": "success",
  "processing_time_seconds": 3.5
}
```

### 3. Database Query

**Database Log:**
```json
{
  "event": "database_query_executed",
  "correlation_id": "req-123-abc",
  "tenant_id": "tenant-456",
  "query": "SELECT * FROM scenario_outputs WHERE scenario_id = ?",
  "duration_ms": 25
}
```

## Best Practices

### 1. Always Use Structured Logging

Replace traditional logging with structured logging:

```python
# ❌ Bad - unstructured logging
logger.info(f"Processing scenario {scenario_id} for tenant {tenant_id}")

# ✅ Good - structured logging
log_structured("info", "scenario_processing_started",
               scenario_id=scenario_id,
               tenant_id=tenant_id)
```

### 2. Include Relevant Context

Always include business-relevant context in your logs:

```python
log_structured("info", "scenario_run_created",
               scenario_id=scenario_id,
               run_id=run_id,
               priority=priority,
               timeout_minutes=timeout,
               tenant_id=tenant_id)
```

### 3. Use Appropriate Log Levels

- **`debug`**: Detailed internal state, performance metrics
- **`info`**: Business events, successful operations
- **`warning`**: Non-critical issues, deprecation notices
- **`error`**: Failed operations, exceptions
- **`critical`**: System failures, security issues

### 4. Handle Exceptions Properly

```python
try:
    result = await process_scenario(scenario_id)
except Exception as exc:
    log_structured("error", "scenario_processing_failed",
                   scenario_id=scenario_id,
                   error_type=exc.__class__.__name__,
                   error_message=str(exc))
    raise
```

### 5. Performance Considerations

- Logs are designed to be fast (<1ms per call)
- Use appropriate log levels to control volume
- Consider sampling for high-volume operations

## Configuration

### Environment Variables

- `AURUM_OTEL_SERVICE_NAME`: Service name for tracing (e.g., "aurum-api", "aurum-scenario-worker")
- `AURUM_OTEL_SERVICE_NAMESPACE`: Service namespace (default: "aurum")
- `AURUM_OTEL_SERVICE_INSTANCE_ID`: Service instance ID (default: hostname)

### Logging Configuration

Structured logs are sent to the `aurum.structured` logger:

```python
import logging

# Configure structured logging
structured_logger = logging.getLogger("aurum.structured")
structured_logger.setLevel(logging.INFO)

# Add handlers as needed
handler = logging.StreamHandler()
structured_logger.addHandler(handler)
```

## Integration with Monitoring

### Prometheus Metrics

Structured logs work alongside Prometheus metrics for complete observability:

```python
from aurum.scenarios.worker import REQUESTS_TOTAL, PROCESS_DURATION

REQUESTS_TOTAL.inc()
PROCESS_DURATION.observe(processing_time)

log_structured("info", "scenario_processed",
               scenario_id=scenario_id,
               processing_time_seconds=processing_time)
```

### OpenTelemetry Tracing

Structured logs automatically include tracing context and set span attributes:

```python
with tracer.start_as_current_span("scenario.process") as span:
    span.set_attribute("aurum.scenario_id", scenario_id)
    span.set_attribute("aurum.tenant_id", tenant_id)

    log_structured("info", "scenario_processing_started", scenario_id=scenario_id)
    # Processing logic...
```

## Troubleshooting

### Debug Correlation ID Propagation

```python
# Check current context
from aurum.telemetry.context import get_context
context = get_context()
print(f"Current context: {context}")

# Manually set context for testing
from aurum.telemetry.context import correlation_context
with correlation_context(correlation_id="debug-123", tenant_id="debug-tenant"):
    log_structured("debug", "test_event", test_field="test_value")
```

### Common Issues

1. **Missing Context**: Ensure correlation context is properly set up in middleware
2. **Thread Safety**: Context variables are thread-safe but require proper setup in each thread
3. **Performance**: Monitor log volume in production; use appropriate log levels
4. **JSON Serialization**: Avoid non-serializable objects in log data

## Migration from Traditional Logging

### Before
```python
logger.info(f"Processing scenario {scenario_id} for tenant {tenant_id}")
logger.warning(f"Database query failed: {exc}")
```

### After
```python
log_structured("info", "scenario_processing_started",
               scenario_id=scenario_id,
               tenant_id=tenant_id)
log_structured("warning", "database_query_failed",
               scenario_id=scenario_id,
               error_type=exc.__class__.__name__,
               error_message=str(exc))
```

This provides better searchability, filtering, and analysis capabilities while maintaining the same performance characteristics.
