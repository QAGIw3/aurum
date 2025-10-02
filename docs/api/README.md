# Aurum Market Intelligence Platform API

[![API Version](https://img.shields.io/badge/API-2.0.0-blue.svg)](https://api.aurum.local/docs)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green.svg)](https://fastapi.tiangolo.com)
[![OpenAPI](https://img.shields.io/badge/OpenAPI-3.1.0-yellow.svg)](https://swagger.io/specification/)

> ⚠️ **v1 Deprecation:** All `/v1/*` endpoints now emit `Deprecation` and `Sunset` headers and will be removed after **30 October 2025**. Migrate to the `/v2/*` surface—see [Migration Guide: v1 → v2](migration-v1-to-v2.md).

## Overview

The Aurum API provides comprehensive access to market data, curves, and analytics through a high-performance RESTful interface built with FastAPI. It offers enterprise-grade features including authentication, rate limiting, caching, and comprehensive monitoring.

**Recent Architecture Improvements:**
- **Unified Database Connection Management**: Standardized connection pooling across all databases (Trino, TimescaleDB, ClickHouse, PostgreSQL) with health monitoring and alerting
- **Enhanced Service Layer**: Decomposed monolithic services into focused, testable modules with proper dependency injection
- **Improved External Data Ingestion**: Unified collector framework for all data providers with rate limiting, retries, and error handling
- **Production Monitoring**: Enterprise-grade health monitoring with Slack/PagerDuty alerting

See also: ../pagination.md, ../runtime-config.md, and ../../src/aurum/database/README.md

## 🚀 Quick Start

### Installation

```bash
# Install the Aurum platform
pip install aurum-platform

# Or install with specific features
pip install aurum-platform[api,caching,monitoring]
```

### Basic Usage

```python
from aurum.api.client import AurumClient

# Initialize client
client = AurumClient(
    base_url="https://api.aurum.local",
    api_key="your-api-key"
)

# Get curve data
curves = client.get_curves(
    asof_date="2024-01-15",
    asset_class="NATURAL_GAS",
    iso="US"
)

# Compare curves
diffs = client.compare_curves(
    curve_key="NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY",
    asof_a="2024-01-10",
    asof_b="2024-01-15"
)
```

## 📚 Documentation

### API Reference
- Interactive Docs (server): https://api.aurum.local/docs
- OpenAPI Spec (YAML): openapi-spec.yaml
- Markdown Docs: api-docs.md
- Response Builders: ../api_usage_guide.md#builder-usage-maintainers
- Async Offload Runbook: offload.md
- HTML (Redoc) Viewer: index.html
- HTML (Swagger UI) Viewer: swagger.html

### Endpoints

| Endpoint | Method | Description | Lifecycle |
|----------|--------|-------------|-----------|
| `/health` | GET | Health check | Active |
| `/ready` | GET | Readiness check | Active |
| `/v2/curves` | GET | Curves API with RFC 7807 errors and cursor pagination | Active |
| `/v2/curves/{curve_id}/diff` | GET | Curve deltas between as-of dates | Active |
| `/v2/metadata/dimensions` | GET | Metadata catalogue | Active |
| `/v2/scenarios` | GET/POST | Scenario lifecycle | Active |
| `/v2/scenarios/{scenario_id}/runs` | GET | Scenario run listing | Active |
| `/v2/scenarios/{scenario_id}/outputs` | GET | Scenario outputs (JSON/CSV) | Active |
| `/v1/*` | — | Legacy v1 surface (curves, metadata, scenarios, admin) | Deprecated (Sunset 2025-12-31) |

## 🔧 Configuration

### Environment Variables

```bash
# API Configuration
export AURUM_API_HOST=0.0.0.0
export AURUM_API_PORT=8080
export AURUM_API_WORKERS=4

# Database Connection Management (New Unified System)
export AURUM_DB_TRINO_HOST=trino
export AURUM_DB_TRINO_PORT=8080
export AURUM_DB_TRINO_CATALOG=aurum
export AURUM_DB_TRINO_USER=aurum_user
export AURUM_DB_TRINO_PASSWORD=secure_password

export AURUM_DB_TIMESCALE_HOST=timescale
export AURUM_DB_TIMESCALE_PORT=5432
export AURUM_DB_TIMESCALE_DATABASE=timeseries
export AURUM_DB_TIMESCALE_USER=aurum_user
export AURUM_DB_TIMESCALE_PASSWORD=secure_password

# Database Pool Configuration
export AURUM_DB_POOL_MIN_SIZE=2
export AURUM_DB_POOL_MAX_SIZE=20
export AURUM_DB_POOL_IDLE_TIMEOUT=300
export AURUM_DB_ACQUIRE_TIMEOUT=10

# Legacy Database Connections (Deprecated - use unified system)
export AURUM_TIMESCALE_DSN="postgresql://user:pass@timescale:5432/timeseries"
export AURUM_TRINO_HOST=trino
export AURUM_TRINO_PORT=8080

# Caching
export AURUM_REDIS_MODE=standalone
export AURUM_CACHE_TTL=300

# Authentication
export AURUM_AUTH_DISABLED=false
export AURUM_JWT_SECRET=your-secret-key

# Rate Limiting
export AURUM_RATE_LIMIT_ENABLED=true
export AURUM_RATE_LIMIT_RPS=10

# Database Health Monitoring (New)
export AURUM_DB_MONITORING_ENABLED=true
export AURUM_DB_MONITORING_INTERVAL=30
export AURUM_DB_ALERT_SLACK_WEBHOOK=https://hooks.slack.com/services/...
export AURUM_DB_ALERT_EMAIL=admin@aurum.com

# External Data Collectors (New Unified Framework)
export AURUM_COLLECTORS_FRED_API_KEY=your-fred-key
export AURUM_COLLECTORS_NOAA_TOKEN=your-noaa-token
export AURUM_COLLECTORS_WORLD_BANK_ENABLED=false
```

## 🗄️ Database Connection Management

### Unified Connection Pooling

The Aurum platform now uses a unified connection pool management system that provides:

- **Standardized Pooling**: Consistent connection pooling across all database types
- **Health Monitoring**: Real-time monitoring of pool utilization and performance
- **Automatic Scaling**: Dynamic pool sizing based on load
- **Alerting**: Proactive alerts for pool exhaustion and performance issues

### Supported Databases

| Database | Purpose | Pool Manager | Health Monitoring |
|----------|---------|--------------|------------------|
| **Trino** | Analytics & OLAP | `TrinoPoolManager` | ✅ |
| **TimescaleDB** | Time-series data | `TimescalePoolManager` | ✅ |
| **ClickHouse** | OLAP queries | Planned | ✅ |
| **PostgreSQL** | Application data | Planned | ✅ |

### Monitoring & Alerting

Database health monitoring is automatically enabled and provides:

- **Pool Utilization Alerts**: Warning at 80%, Critical at 95%
- **Response Time Monitoring**: Alerts for slow connection acquisition
- **Connection Exhaustion**: Alerts when pools are fully utilized
- **Multi-channel Notifications**: Email, Slack, PagerDuty integration

### Configuration Example

```python
from aurum.database import configure_alerting

# Configure alerting
alert_config = configure_alerting(
    smtp_server="smtp.aurum.com",
    smtp_username="alerts@aurum.com",
    smtp_password="secure_password",
    slack_webhook="https://hooks.slack.com/services/...",
    pagerduty_routing_key="your-routing-key",
    to_emails=["admin@aurum.com", "dba@aurum.com"]
)

# Start monitoring
from aurum.database import start_production_monitoring
await start_production_monitoring(alert_config)
```

## 📊 External Data Collection

### Unified Collector Framework

The platform now uses a unified framework for external data collection that provides:

- **Standardized Patterns**: Common rate limiting, retry logic, and error handling
- **Provider Abstraction**: Easy to add new data providers
- **Automatic Checkpointing**: Incremental data collection with resume capability
- **Comprehensive Monitoring**: Built-in metrics and alerting for collection jobs

### Supported Providers

| Provider | API | Rate Limit | Data Types | Status |
|----------|-----|------------|------------|--------|
| **EIA** | ✅ | 1000/min | Energy data | ✅ Unified |
| **FRED** | ✅ | 120/min | Economic data | ✅ Unified |
| **NOAA** | ✅ | 1000/min | Weather data | ✅ Unified |
| **WorldBank** | ✅ | 1000/min | Global indicators | ✅ Unified |

### Usage Example

```python
from aurum.external.providers.eia_unified import create_eia_collector

# Create collector with unified framework
collector = create_eia_collector()

# Collect catalog (series metadata)
await collector.collect_catalog()

# Collect observations for specific datasets
await collector.collect_observations("electricity_series")

# Framework handles:
# - Rate limiting and quota management
# - Automatic retries with exponential backoff
# - Checkpoint management for incremental updates
# - Data transformation and validation
# - Kafka emission for downstream processing
```
```

### Docker Configuration

```yaml
version: '3.8'
services:
  aurum-api:
    image: ghcr.io/aurum/api:1.0.0
    ports:
      - "8080:8080"
    environment:
      - AURUM_TIMESCALE_DSN=postgresql://user:pass@timescale:5432/timeseries
      - AURUM_TRINO_HOST=trino
      - AURUM_REDIS_MODE=standalone
    depends_on:
      - timescale
      - trino
      - redis
```

## 📊 Performance Features

## 🔄 Regenerate OpenAPI Spec

To regenerate the OpenAPI schema and keep the HTML viewer in sync:

```bash
python3 aurum/scripts/docs/generate_openapi.py

# Outputs
# - docs/api/openapi-spec.yaml
# - docs/api/openapi-spec.json
```

### Caching
- **Multi-level caching**: In-memory + Redis backends
- **TTL-based expiration**: Configurable cache lifetimes
- **Cache warming**: Automatic pre-population of frequently accessed data
- **Cache analytics**: Hit/miss ratio monitoring

### Connection Pooling
- **Database pooling**: PostgreSQL and Trino connection pools
- **Redis pooling**: Optimized Redis connections
- **Pool monitoring**: Connection usage tracking
- **Automatic failover**: Pool health monitoring

### Batching
- **Query batching**: Efficient batch processing
- **Data ingestion**: Optimized data loading
- **Background processing**: Async task processing
- **Batch analytics**: Processing metrics

### Partitioning
- **Time-based partitioning**: Date-range optimized queries
- **Automatic partition management**: Creation and cleanup
- **Query optimization**: Partition-aware query planning
- **Storage optimization**: Compressed historical data

## 🔐 Security

### Authentication Methods
- **JWT Tokens**: Bearer token authentication
- **API Keys**: Service-to-service authentication
- **OIDC Integration**: Enterprise SSO support
- **RBAC**: Role-based access control

### Rate Limiting
- **Request throttling**: Per-endpoint limits
- **Burst handling**: Configurable burst limits
- **Rate limit headers**: Standard rate limit response headers
- **Monitoring**: Rate limit violation tracking

### Data Security
- **Encryption**: TLS/SSL encryption in transit
- **Data masking**: Sensitive data protection
- **Audit logging**: Complete request/response logging
- **Input validation**: Comprehensive input sanitization

## 📈 Monitoring

### Health Checks
- **Liveness probes**: Service health verification
- **Readiness probes**: Dependency health checks
- **Startup probes**: Service initialization verification

### Metrics Collection
- **Request metrics**: Response times, error rates
- **Cache metrics**: Hit rates, cache performance
- **Database metrics**: Query performance, connection stats
- **System metrics**: CPU, memory, disk usage

### Alerting
- **Performance alerts**: Slow query detection
- **Error alerts**: Service error monitoring
- **Resource alerts**: System resource monitoring
- **Integration alerts**: Dependency failure detection

## 🧪 Testing

### Unit Tests
```bash
# Run API unit tests
pytest tests/api/

# Run with coverage
pytest tests/api/ --cov=src/aurum/api --cov-report=html
```

### Integration Tests
```bash
# Run integration tests
pytest tests/integration/ -v

# Test with real services
pytest tests/integration/ --services=all
```

### Performance Tests
```bash
# Load testing
pytest tests/performance/ --load-test

# Stress testing
pytest tests/performance/ --stress-test
```

## 🚀 Deployment

### Kubernetes Deployment
```bash
# Deploy API service
kubectl apply -f k8s/api/

# Check deployment status
kubectl get pods -n aurum-dev -l app=api

# View logs
kubectl logs -f deployment/api -n aurum-dev
```

### Docker Deployment
```bash
# Build API image
docker build -f Dockerfile.api -t ghcr.io/aurum/api:1.0.0 .

# Run API service
docker run -p 8080:8080 ghcr.io/aurum/api:1.0.0
```

### Scaling
```bash
# Scale API deployment
kubectl scale deployment api --replicas=5 -n aurum-dev

# Enable horizontal pod autoscaling
kubectl autoscale deployment api --cpu-percent=70 --min=3 --max=10 -n aurum-dev
```

## 📋 API Examples

### Get Market Curves
```python
import requests

response = requests.get(
    "https://api.aurum.local/v1/curves",
    params={
        "asof_date": "2024-01-15",
        "asset_class": "NATURAL_GAS",
        "iso": "US",
        "limit": 100
    },
    headers={"Authorization": "Bearer your-jwt-token"}
)

curves = response.json()
print(f"Retrieved {len(curves['data'])} curve points")
```

### Compare Curve Prices
```python
import requests

response = requests.get(
    "https://api.aurum.local/v1/curves/diff",
    params={
        "curve_key": "NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY",
        "asof_a": "2024-01-10",
        "asof_b": "2024-01-15"
    },
    headers={"X-API-Key": "your-api-key"}
)

differences = response.json()
for diff in differences['data']:
    print(f"{diff['tenor_label']}: {diff['diff_abs']:+.3f} ({diff['diff_pct']:+.1f}%)")
```

### Get Metadata Dimensions
```python
import requests

response = requests.get(
    "https://api.aurum.local/v1/metadata/dimensions",
    params={"include_counts": "true"}
)

dimensions = response.json()
print("Available asset classes:", dimensions['data']['asset_class'])
print("Total curves:", sum(count['count'] for count in dimensions['counts']['asset_class']))
```

## 🔍 Troubleshooting

### Common Issues

**1. Authentication Errors**
```bash
# Check API key validity
curl -H "X-API-Key: your-api-key" https://api.aurum.local/health

# Check JWT token format
curl -H "Authorization: Bearer your-jwt-token" https://api.aurum.local/health
```

**2. Rate Limiting**
```bash
# Check rate limit headers
curl -v https://api.aurum.local/v1/curves 2>&1 | grep -i "x-ratelimit"

# Implement exponential backoff
sleep $((2 ** $retry_count))
```

**3. Timeout Errors**
```bash
# Increase client timeout
curl --max-time 30 https://api.aurum.local/v1/curves

# Check service health
curl https://api.aurum.local/ready
```

**4. Large Response Handling**
```python
# Use pagination
params = {"limit": 1000, "cursor": None}
while True:
    response = requests.get(url, params=params)
    data = response.json()
    # Process data
    if not data['meta']['next_cursor']:
        break
    params['cursor'] = data['meta']['next_cursor']
```

## 📞 Support

- **Documentation**: [https://docs.aurum.local](https://docs.aurum.local)
- **API Status**: [https://status.aurum.local](https://status.aurum.local)
- **Support Email**: api-support@aurum.local
- **GitHub Issues**: [github.com/aurum/platform/issues](https://github.com/aurum/platform/issues)

## 📄 License

This API is part of the Aurum platform and is licensed under the MIT License.

---

**API Version**: 1.0.0
**Last Updated**: 2024-01-15
**Contact**: api-support@aurum.local
