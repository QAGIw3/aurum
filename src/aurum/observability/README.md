# Enhanced Observability System

This directory contains the comprehensive observability infrastructure for the Aurum platform, providing end-to-end visibility into system performance, errors, and business metrics.

## Overview

The observability system is built on three pillars:

1. **Structured Logging** - JSON-formatted logs with rich context
2. **Distributed Tracing** - Request correlation and performance tracking
3. **Metrics Collection** - Prometheus-based metrics with SLO/SLI support

## Architecture

```
┌─────────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│   Applications  │───▶│ Observability API   │───▶│ Metrics Storage  │
│ (API, Workers)  │    │                     │    │ (Prometheus)     │
└─────────────────┘    │ - /v1/observability │    └──────────────────┘
                       │ - Structured Logs   │            │
                       │ - Tracing Spans     │    ┌──────────────────┐
                       │ - SLO Dashboard     │───▶│ Log Storage      │
                       └─────────────────────┘    │ (JSON/ELK)       │
                                                └──────────────────┘
```

## Components

### 📊 Metrics Collection (`metrics.py`)

**Features:**
- Prometheus metrics collection with automatic middleware
- Business metrics for scenarios and transactions
- Cache performance tracking
- Database connection pool monitoring
- Rate limiting metrics

**Key Metrics:**
- API request latency (P50, P95, P99)
- Error rates by endpoint and tenant
- Cache hit/miss rates
- Database query performance
- Business transaction success rates

### 🔍 Distributed Tracing (`tracing.py`)

**Features:**
- OpenTelemetry-compatible tracing
- Request correlation across services
- Database operation tracking
- Cache operation monitoring
- Error context preservation

**Tracing Structure:**
```
HTTP Request
├── Database Queries
├── Cache Operations
├── External API Calls
└── Business Logic
```

### 📋 Structured Logging (`enhanced_logging.py`)

**Features:**
- JSON-formatted logs with rich metadata
- Request correlation IDs
- Performance timing
- Error context and stack traces
- Component-specific loggers

**Log Format:**
```json
{
  "timestamp": "2024-01-01T12:00:00.000Z",
  "level": "INFO",
  "message": "Scenario creation completed",
  "request_id": "req-12345678",
  "tenant_id": "acme-corp",
  "component": "scenario",
  "operation": "create",
  "duration_ms": 45.2
}
```

### 🎯 SLO Dashboard (`slo_dashboard.py`)

**Service Level Objectives:**
- API Availability: 99.9% uptime
- API Latency: <500ms P95
- Scenario Creation: 99.5% success rate
- Scenario Runs: 95% success rate
- Data Freshness: <1 hour staleness
- Cache Hit Rate: >90%

**Features:**
- Real-time SLO status monitoring
- Grafana dashboard configuration
- Alert rules for SLO violations
- Performance baseline tracking

## API Endpoints

### Metrics Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/observability/metrics` | GET | Prometheus metrics |
| `/v1/observability/metrics/json` | GET | JSON metrics |
| `/v1/observability/performance` | GET | System performance |

### Tracing Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/observability/traces` | GET | List all traces |
| `/v1/observability/traces/{id}` | GET | Get trace detail |
| `/v1/observability/traces/{id}/export` | POST | Export trace |
| `/v1/observability/cleanup` | POST | Clean old traces |

### SLO Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/observability/slos` | GET | SLO status |
| `/v1/observability/slos/{name}` | GET | SLO detail |
| `/v1/observability/dashboard` | GET | Dashboard config |

### System Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/observability/health` | GET | System health |
| `/v1/observability/logs/schema` | GET | Log schema |

## Usage Examples

### Structured Logging

```python
from aurum.observability.enhanced_logging import get_logger, log_operation

logger = get_logger("aurum.api.scenarios")

async def create_scenario():
    async with log_operation(logger, "create_scenario", component="scenario"):
        # Your scenario creation logic
        pass

# Log with context
logger.info(
    "Scenario created successfully",
    scenario_id="scenario-123",
    tenant_id="acme-corp",
    duration_ms=45.2
)
```

### Tracing Operations

```python
from aurum.observability.tracing_enhanced import trace_scenario_operation, trace_database_query

async def create_scenario_with_tracing(scenario_id: str, tenant_id: str):
    async with trace_scenario_operation("create", scenario_id, tenant_id):
        async with trace_database_query("insert", table="scenario", tenant_id=tenant_id):
            # Database operations
            pass

        async with trace_cache_operation("set", cache_type="scenario", key=scenario_id):
            # Cache operations
            pass
```

### Business Metrics

```python
from aurum.observability.metrics import increment_business_transactions

# Track scenario creation
await increment_business_transactions("scenario_create", "scenario", "success")

# Track scenario run
await increment_business_transactions("scenario_run", "scenario", "success")
```

### Performance Monitoring

```python
from aurum.observability.slo_dashboard import check_slo_status

# Check current SLO status
slo_status = check_slo_status()

# Get SLI values
from aurum.observability.slo_dashboard import get_sli_values
slis = get_sli_values()

# Generate dashboard configuration
from aurum.observability.slo_dashboard import get_slo_dashboard_config
dashboard = get_slo_dashboard_config()
grafana_config = dashboard.generate_grafana_dashboard()
```

## Configuration

### Environment Variables

```bash
# Logging Configuration
AURUM_LOG_LEVEL=INFO
AURUM_LOG_FORMAT=json

# Tracing Configuration
AURUM_TRACING_ENABLED=true
AURUM_TRACING_SAMPLE_RATE=1.0

# Metrics Configuration
AURUM_METRICS_ENABLED=true
AURUM_METRICS_PREFIX=aurum_

# SLO Configuration
AURUM_SLO_API_AVAILABILITY_TARGET=0.999
AURUM_SLO_API_LATENCY_TARGET=500
AURUM_SLO_SCENARIO_SUCCESS_TARGET=0.95
```

### Settings Integration

```python
from aurum.core import AurumSettings

class ObservabilitySettings:
    """Observability configuration."""

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"
    log_retention_days: int = 30

    # Tracing
    tracing_enabled: bool = True
    tracing_sample_rate: float = 1.0
    tracing_max_spans_per_trace: int = 1000

    # Metrics
    metrics_enabled: bool = True
    metrics_collection_interval: int = 60  # seconds

    # SLO
    slo_check_interval: int = 300  # seconds
    slo_alert_threshold: float = 0.95

    # Performance
    performance_tracking_enabled: bool = True
    slow_query_threshold_ms: int = 1000
```

## Monitoring and Alerting

### Alert Rules

The system includes predefined alert rules for:

1. **API Performance**: High latency or error rates
2. **Business Metrics**: Scenario creation/run failures
3. **Infrastructure**: Database connection pool exhaustion
4. **Cache Performance**: Low hit rates
5. **SLO Violations**: Service level objective breaches

### Dashboard Configuration

Pre-built Grafana dashboards are available for:

- **SLO Overview**: High-level service status
- **API Performance**: Request latency and error rates
- **Business Metrics**: Scenario operations and success rates
- **Infrastructure**: Database, cache, and system metrics
- **Alert Status**: Active alerts and trends

### Health Checks

Health check endpoints provide:
- Observability system status
- Metrics collection health
- Tracing system status
- SLO compliance status
- Performance indicators

## Best Practices

### Logging

1. **Use Structured Logging**: Always use the enhanced logger
2. **Include Context**: Add request_id, tenant_id, and operation info
3. **Log Performance**: Track operation durations
4. **Error Context**: Include stack traces and error details
5. **Component Tags**: Use consistent component names

### Tracing

1. **Span Naming**: Use consistent naming conventions
2. **Attribute Standards**: Follow OpenTelemetry conventions
3. **Error Handling**: Always set error status on exceptions
4. **Context Propagation**: Ensure trace context flows correctly
5. **Performance Impact**: Monitor tracing overhead

### Metrics

1. **Business Focus**: Track key business outcomes
2. **SLO Alignment**: Metrics should support SLO definitions
3. **Cardinality Control**: Avoid high-cardinality labels
4. **Histogram Buckets**: Use appropriate bucket ranges
5. **Collection Frequency**: Balance detail vs. overhead

## Troubleshooting

### Common Issues

1. **High Tracing Overhead**
   - Reduce sample rate
   - Increase span limits
   - Optimize trace attributes

2. **Log Volume Too High**
   - Adjust log levels
   - Add filtering
   - Implement sampling

3. **Missing Metrics**
   - Check Prometheus configuration
   - Verify metric names and labels
   - Confirm collection intervals

4. **SLO Dashboard Errors**
   - Validate metric queries
   - Check data sources
   - Review time ranges

### Debug Mode

Enable debug logging:

```python
import logging
logging.getLogger('aurum.observability').setLevel(logging.DEBUG)
```

### Performance Profiling

Monitor observability overhead:

```python
from aurum.observability.metrics import REQUEST_LATENCY

# Monitor API request latency
async def api_endpoint():
    start_time = time.time()
    try:
        # Business logic
        return result
    finally:
        duration = time.time() - start_time
        REQUEST_LATENCY.labels(method="POST", path="/api/scenarios").observe(duration)
```

## Integration Examples

### FastAPI Application

```python
from fastapi import FastAPI
from aurum.observability.enhanced_logging import StructuredLoggingMiddleware
from aurum.observability.api import router as observability_router

app = FastAPI()
app.include_router(observability_router)
app.add_middleware(StructuredLoggingMiddleware)

@app.get("/api/scenarios")
async def list_scenarios():
    logger = get_logger("aurum.api.scenarios")

    async with log_operation(logger, "list_scenarios", component="api"):
        # Implementation
        pass
```

### Background Worker

```python
from aurum.observability.tracing_enhanced import trace_scenario_operation

async def process_scenario_run(scenario_id: str, run_id: str):
    async with trace_scenario_operation("process", scenario_id, run_id=run_id):
        # Worker logic
        pass
```

### Database Operations

```python
from aurum.observability.tracing_enhanced import trace_database_query

async def create_scenario_record(data: dict):
    async with trace_database_query("insert", table="scenario"):
        # Database operations
        pass
```

## Security Considerations

- **Log Sanitization**: Remove sensitive data from logs
- **Access Control**: Secure observability endpoints
- **Data Retention**: Implement log and trace retention policies
- **Performance Monitoring**: Monitor observability overhead
- **Alert Fatigue**: Configure appropriate alert thresholds

## Performance Impact

The observability system is designed to have minimal performance impact:

- **Logging**: <1ms per log entry
- **Tracing**: <0.1ms per span operation
- **Metrics**: <0.05ms per metric update
- **Memory Usage**: ~10MB baseline + ~1MB per 1000 active spans

Monitor system performance and adjust configuration if needed.
