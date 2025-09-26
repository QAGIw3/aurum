# Aurum Market Intelligence Platform API

[![API Version](https://img.shields.io/badge/API-1.0.0-blue.svg)](https://api.aurum.local/docs)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green.svg)](https://fastapi.tiangolo.com)
[![OpenAPI](https://img.shields.io/badge/OpenAPI-3.0.3-yellow.svg)](https://swagger.io/specification/)

## Overview

The Aurum API provides comprehensive access to market data, curves, and analytics through a high-performance RESTful interface built with FastAPI. It offers enterprise-grade features including authentication, rate limiting, caching, and comprehensive monitoring.

See also: ../pagination.md and ../runtime-config.md

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
- Async Offload Runbook: offload.md
- HTML (Redoc) Viewer: index.html
- HTML (Swagger UI) Viewer: swagger.html

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/ready` | GET | Readiness check |
| `/v1/curves` | GET | Get curve data |
| `/v1/curves/diff` | GET | Compare curve data |
| `/v1/metadata/dimensions` | GET | Get metadata dimensions |
| `/v1/locations/iso` | GET | Get ISO locations |
| `/v1/locations/iso/{location_id}` | GET | Get location details |
| `/v1/scenarios` | GET/POST | List or create scenarios |
| `/v1/scenarios/{scenario_id}` | GET/DELETE | Get or delete scenario |
| `/v1/scenarios/{scenario_id}/run` | POST | Trigger a scenario run |
| `/v1/scenarios/{scenario_id}/runs` | GET | List runs for a scenario |
| `/v1/scenarios/{scenario_id}/runs/{run_id}` | GET | Get run detail |
| `/v1/scenarios/runs/{run_id}/state` | POST | Update run state |
| `/v1/scenarios/runs/{run_id}/cancel` | POST | Cancel a run |
| `/v1/scenarios/{scenario_id}/outputs` | GET | Latest outputs (JSON/CSV) |
| `/v1/scenarios/{scenario_id}/metrics/latest` | GET | Latest metrics |

## 🔧 Configuration

### Environment Variables

```bash
# API Configuration
export AURUM_API_HOST=0.0.0.0
export AURUM_API_PORT=8080
export AURUM_API_WORKERS=4

# Database Connections
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
# - docs/api/aurum.yaml (compat with Redoc index if needed)
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
