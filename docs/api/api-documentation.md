# Aurum Market Intelligence Platform API Documentation

## Overview

The Aurum API provides comprehensive access to market data, curves, and analytics through a RESTful interface. Built with FastAPI, it offers high performance, automatic documentation generation, and enterprise-grade features.

## Base URL

- **Development**: `http://api.aurum-dev.svc.cluster.local:8080`
- **Production**: `https://api.aurum.local`
- **Local**: `http://localhost:8080`

## Authentication

The API supports multiple authentication methods:

### 1. JWT Token Authentication
```bash
curl -H "Authorization: Bearer <jwt_token>" \
     http://api.aurum-dev.svc.cluster.local:8080/v1/curves
```

### 2. API Key Authentication
```bash
curl -H "X-API-Key: <api_key>" \
     http://api.aurum-dev.svc.cluster.local:8080/v1/curves
```

### 3. No Authentication (Health Checks)
```bash
curl http://api.aurum-dev.svc.cluster.local:8080/health
```

## Health and Readiness Endpoints

### Health Check
```bash
curl http://api.aurum-dev.svc.cluster.local:8080/health
```

**Response:**
```json
{
  "status": "ok"
}
```

### Readiness Check
```bash
curl http://api.aurum-dev.svc.cluster.local:8080/ready
```

**Response (Ready):**
```json
{
  "status": "ready",
  "checks": {
    "trino": "ready",
    "timescale": "ready",
    "redis": "ready"
  }
}
```

**Response (Unavailable):**
```json
{
  "status": "unavailable",
  "checks": {
    "trino": "unavailable",
    "timescale": "ready",
    "redis": "ready"
  }
}
```

## Market Curves API

### Get Curve Data

Retrieve market curve data with optional filtering.

**Endpoint:** `GET /v1/curves`

**Parameters:**
- `asof_date` (required): Date for which to retrieve data (YYYY-MM-DD)
- `curve_key` (optional): Specific curve identifier
- `asset_class` (optional): Filter by asset class
- `iso` (optional): Filter by ISO country code
- `market` (optional): Filter by market
- `location` (optional): Filter by location
- `product` (optional): Filter by product
- `block` (optional): Filter by trading block
- `tenor_type` (optional): Filter by tenor type
- `price_type` (optional): Filter by price type
- `limit` (optional): Maximum records (default: 100, max: 10000)
- `cursor` (optional): Pagination cursor

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/curves?asof_date=2024-01-15&asset_class=NATURAL_GAS&iso=US&limit=10"
```

**Response:**
```json
{
  "meta": {
    "request_id": "req-12345678-90ab-cdef-1234-567890abcdef",
    "query_time_ms": 145,
    "next_cursor": "eyJwYWdlIjoxLCJsaW1pdCI6MTAwfQ==",
    "prev_cursor": null
  },
  "data": [
    {
      "curve_key": "NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY",
      "tenor_label": "Jan24",
      "tenor_type": "MONTHLY",
      "contract_month": "2024-01-01",
      "asof_date": "2024-01-15",
      "mid": 2.85,
      "bid": 2.84,
      "ask": 2.86,
      "price_type": "MID"
    }
  ]
}
```

### Get Curve Differences

Compare curve data between two dates.

**Endpoint:** `GET /v1/curves/diff`

**Parameters:**
- `curve_key` (required): Curve identifier
- `asof_a` (required): First comparison date
- `asof_b` (required): Second comparison date
- `iso` (optional): Filter by ISO country code
- `market` (optional): Filter by market
- `location` (optional): Filter by location
- `limit` (optional): Maximum records
- `cursor` (optional): Pagination cursor

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/curves/diff?curve_key=NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY&asof_a=2024-01-10&asof_b=2024-01-15"
```

**Response:**
```json
{
  "meta": {
    "request_id": "req-12345678-90ab-cdef-1234-567890abcdef",
    "query_time_ms": 234,
    "next_cursor": null,
    "prev_cursor": null
  },
  "data": [
    {
      "curve_key": "NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY",
      "tenor_label": "Jan24",
      "tenor_type": "MONTHLY",
      "contract_month": "2024-01-01",
      "asof_a": "2024-01-10",
      "mid_a": 2.80,
      "asof_b": "2024-01-15",
      "mid_b": 2.85,
      "diff_abs": 0.05,
      "diff_pct": 1.79
    }
  ]
}
```

## Metadata API

### Get Dimensions

Retrieve available metadata dimensions for filtering.

**Endpoint:** `GET /v1/metadata/dimensions`

**Parameters:**
- `include_counts` (optional): Include count information (default: false)

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/metadata/dimensions?include_counts=true"
```

**Response:**
```json
{
  "meta": {
    "request_id": "req-12345678-90ab-cdef-1234-567890abcdef",
    "query_time_ms": 89,
    "next_cursor": null,
    "prev_cursor": null
  },
  "data": {
    "asset_class": ["NATURAL_GAS", "COAL", "OIL", "POWER"],
    "iso": ["US", "CA", "MX", "EU"],
    "location": ["HENRY_HUB", "ALGONQUIN", "CHICAGO"],
    "market": ["NYMEX", "ICE", "OTC"],
    "product": ["NATURAL_GAS", "POWER", "EMISSIONS"],
    "block": ["PEAK", "OFF_PEAK", "FLAT"],
    "tenor_type": ["MONTHLY", "QUARTERLY", "CALENDAR"]
  },
  "counts": {
    "asset_class": [
      {"value": "NATURAL_GAS", "count": 15420},
      {"value": "POWER", "count": 8900}
    ]
  }
}
```

## Location API

### Get ISO Locations

Retrieve mapping of ISO country codes to trading locations.

**Endpoint:** `GET /v1/locations/iso`

**Parameters:**
- `iso` (optional): Filter by ISO country code
- `location_type` (optional): Filter by location type (HUB, CITY, REGION, ZONE)
- `limit` (optional): Maximum records
- `cursor` (optional): Pagination cursor

**Example:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/locations/iso?iso=US&location_type=HUB"
```

**Response:**
```json
{
  "meta": {
    "request_id": "req-12345678-90ab-cdef-1234-567890abcdef",
    "query_time_ms": 67,
    "next_cursor": null,
    "prev_cursor": null
  },
  "data": [
    {
      "iso": "US",
      "location_id": "HENRY_HUB",
      "location_name": "Henry Hub",
      "location_type": "HUB",
      "zone": "America/Chicago",
      "hub": "HENRY_HUB",
      "timezone": "Central Time"
    }
  ]
}
```

### Get Location Details

Get detailed information about a specific location.

**Endpoint:** `GET /v1/locations/iso/{location_id}`

**Example:**
```bash
curl http://api.aurum-dev.svc.cluster.local:8080/v1/locations/iso/HENRY_HUB
```

## Error Handling

All endpoints return structured error responses:

**400 Bad Request:**
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid date format",
    "details": {
      "field": "asof_date",
      "provided": "2024/01/15",
      "expected": "YYYY-MM-DD"
    },
    "request_id": "req-12345678-90ab-cdef-1234-567890abcdef"
  }
}
```

**404 Not Found:**
```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Curve not found",
    "details": {
      "curve_key": "INVALID_CURVE_KEY"
    },
    "request_id": "req-12345678-90ab-cdef-1234-567890abcdef"
  }
}
```

**503 Service Unavailable:**
```json
{
  "error": {
    "code": "SERVICE_UNAVAILABLE",
    "message": "Database connection failed",
    "details": {
      "service": "timescale",
      "error": "Connection timeout"
    },
    "request_id": "req-12345678-90ab-cdef-1234-567890abcdef"
  }
}
```

## Rate Limiting

Rate limiting is applied per endpoint:
- **Default**: 10 requests/second, 20 burst
- **Health endpoints**: No rate limiting
- **Metadata endpoints**: 50 requests/second, 100 burst

**Rate limit exceeded response:**
```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Rate limit exceeded",
    "details": {
      "limit": 10,
      "remaining": 0,
      "reset_time": "2024-01-15T10:30:00Z"
    },
    "request_id": "req-12345678-90ab-cdef-1234-567890abcdef"
  }
}
```

## Pagination

Large datasets support cursor-based pagination:

**Initial Request:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/curves?asof_date=2024-01-15&limit=100"
```

**Paginated Response:**
```json
{
  "meta": {
    "request_id": "req-12345678-90ab-cdef-1234-567890abcdef",
    "query_time_ms": 145,
    "next_cursor": "eyJwYWdlIjoxLCJsaW1pdCI6MTAwfQ==",
    "prev_cursor": null
  },
  "data": [...]
}
```

**Next Page:**
```bash
curl "http://api.aurum-dev.svc.cluster.local:8080/v1/curves?asof_date=2024-01-15&limit=100&cursor=eyJwYWdlIjoxLCJsaW1pdCI6MTAwfQ=="
```

## Best Practices

### 1. Use Health Checks
```bash
# Check service health before making requests
curl http://api.aurum-dev.svc.cluster.local:8080/health
```

### 2. Implement Retry Logic
```python
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"],
    backoff_factor=1
)

adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)
```

### 3. Handle Pagination
```python
def get_all_pages(base_url, params):
    results = []
    while True:
        response = requests.get(base_url, params=params)
        data = response.json()
        results.extend(data['data'])

        if not data['meta']['next_cursor']:
            break

        params['cursor'] = data['meta']['next_cursor']

    return results
```

### 4. Use Appropriate Limits
```python
# Good: Reasonable page size
response = requests.get('/v1/curves', params={'limit': 1000})

# Bad: Too large page size may timeout
response = requests.get('/v1/curves', params={'limit': 100000})
```

### 5. Cache Frequently Used Data
```python
# Cache dimension data for filter dropdowns
dimensions = requests.get('/v1/metadata/dimensions').json()
# Cache for 1 hour
```

## SDK Usage

### Python Client
```python
from aurum_client import AurumClient

client = AurumClient(
    base_url="http://api.aurum-dev.svc.cluster.local:8080",
    api_key="your-api-key"
)

# Get curve data
curves = client.get_curves(
    asof_date="2024-01-15",
    asset_class="NATURAL_GAS",
    iso="US"
)

# Get curve differences
diffs = client.get_curve_diffs(
    curve_key="NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY",
    asof_a="2024-01-10",
    asof_b="2024-01-15"
)
```

### JavaScript/TypeScript Client
```typescript
import { AurumClient } from '@aurum/client';

const client = new AurumClient({
  baseURL: 'http://api.aurum-dev.svc.cluster.local:8080',
  apiKey: 'your-api-key'
});

const curves = await client.curves.get({
  asofDate: '2024-01-15',
  assetClass: 'NATURAL_GAS',
  iso: 'US'
});

const diffs = await client.curves.diff({
  curveKey: 'NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY',
  asofA: '2024-01-10',
  asofB: '2024-01-15'
});
```

## Performance Tips

### 1. Use Specific Filters
```bash
# Good: Specific filters reduce data transfer
curl "/v1/curves?asof_date=2024-01-15&curve_key=NATURAL_GAS_NYMEX_HENRY_HUB_MONTHLY"

# Bad: No filters may return large datasets
curl "/v1/curves?asof_date=2024-01-15"
```

### 2. Use Appropriate Pagination
```bash
# Start with small pages for testing
curl "/v1/curves?asof_date=2024-01-15&limit=100"

# Use larger pages for bulk processing
curl "/v1/curves?asof_date=2024-01-15&limit=1000"
```

### 3. Cache Dimension Data
```python
# Cache dimensions for filter UI
dimensions_cache = {}
def get_dimensions():
    if not dimensions_cache:
        response = requests.get('/v1/metadata/dimensions')
        dimensions_cache.update(response.json())
    return dimensions_cache
```

### 4. Use Connection Pooling
```python
import requests

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=20)
session.mount('http://', adapter)
session.mount('https://', adapter)
```

## Troubleshooting

### Common Issues

**1. Authentication Errors**
- Verify API key or JWT token is valid
- Check token expiration
- Ensure correct authentication header format

**2. Rate Limiting**
- Implement exponential backoff
- Use longer intervals between requests
- Consider upgrading rate limits for high-volume usage

**3. Timeout Errors**
- Increase client timeout settings
- Check network connectivity
- Verify service health with `/health` endpoint

**4. Large Response Handling**
- Use pagination for large datasets
- Process data in streaming fashion
- Consider data export for bulk processing

### Debug Mode
Enable debug logging to troubleshoot issues:

```bash
export AURUM_DEBUG=true
curl -v http://api.aurum-dev.svc.cluster.local:8080/v1/curves
```

## Support

For technical support:
- **Email**: platform@aurum.local
- **Documentation**: [API Reference](https://api.aurum.local/docs)
- **Status Page**: [status.aurum.local](https://status.aurum.local)

---

**API Version**: 1.0.0
**Last Updated**: 2024-01-15
**Contact**: platform@aurum.local
