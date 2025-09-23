# External Data API

The External Data API provides access to external market data providers, series, and observations with comprehensive caching, rate limiting, and observability features.

## Authentication

All external API endpoints require OIDC authentication with a valid Bearer token:

```bash
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
     "https://api.aurum.local/v1/external/providers"
```

## Endpoints

### List External Providers

Retrieve a paginated list of external data providers.

```bash
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
     "https://api.aurum.local/v1/external/providers?limit=50&offset=0"
```

**Response:**
```json
{
  "data": [
    {
      "id": "fred",
      "name": "Federal Reserve Economic Data",
      "description": "Economic data from the Federal Reserve Bank of St. Louis",
      "base_url": "https://api.stlouisfed.org",
      "last_updated": "2025-01-21T10:30:00Z",
      "series_count": 12345
    }
  ],
  "meta": {
    "request_id": "req-12345",
    "query_time_ms": 45,
    "has_more": false,
    "count": 1,
    "total": 1,
    "offset": 0,
    "limit": 50
  }
}
```

### List External Series

Retrieve external data series with optional filtering by provider and frequency.

```bash
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
     "https://api.aurum.local/v1/external/series?provider=fred&frequency=monthly&limit=100"
```

**Query Parameters:**
- `provider` (optional): Filter by provider
- `frequency` (optional): Filter by frequency (daily, weekly, monthly, quarterly, yearly)
- `asof` (optional): As-of date filter (YYYY-MM-DD)
- `limit` (optional): Maximum results (default: 100, max: 1000)
- `offset` (optional): Pagination offset
- `cursor` (optional): Opaque cursor for stable pagination

### Get Series Observations

Retrieve observations for a specific external series.

```bash
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
     "https://api.aurum.local/v1/external/series/FRED:GDP/observations?start_date=2020-01-01&end_date=2024-01-01&frequency=monthly"
```

**Query Parameters:**
- `start_date` (optional): Start date for observations (YYYY-MM-DD)
- `end_date` (optional): End date for observations (YYYY-MM-DD)
- `frequency` (optional): Output frequency (daily, weekly, monthly, quarterly, yearly)
- `asof` (optional): As-of date (YYYY-MM-DD)
- `limit` (optional): Maximum observations (default: 500, max: 10000)
- `offset` (optional): Pagination offset
- `cursor` (optional): Opaque cursor for stable pagination
- `format` (optional): Response format (json or csv, default: json)

**Response:**
```json
{
  "data": [
    {
      "series_id": "FRED:GDP",
      "date": "2020-01-01",
      "value": 21538.032,
      "metadata": {
        "curve_key": "GDP_MONTHLY",
        "tenor_label": "Jan-2020",
        "source": "external"
      }
    }
  ],
  "meta": {
    "request_id": "req-12346",
    "query_time_ms": 78,
    "has_more": false,
    "count": 1,
    "total": 1,
    "offset": 0,
    "limit": 500
  }
}
```

**CSV Response:**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
     "https://api.aurum.local/v1/external/series/FRED:GDP/observations?format=csv"
```

### Get External Metadata

Retrieve metadata about external data providers and their series.

```bash
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
     "https://api.aurum.local/v1/metadata/external?include_counts=true"
```

**Query Parameters:**
- `provider` (optional): Filter by provider
- `include_counts` (optional): Include series counts in response (default: false)

**Response:**
```json
{
  "providers": [
    {
      "id": "fred",
      "name": "Federal Reserve Economic Data",
      "description": "Economic data from the Federal Reserve Bank of St. Louis",
      "base_url": "https://api.stlouisfed.org",
      "last_updated": "2025-01-21T10:30:00Z",
      "series_count": 12345
    }
  ],
  "total_series": 54321,
  "last_updated": "2025-01-21T10:30:00Z"
}
```

## Error Handling

### Rate Limiting (429)

```json
{
  "error": "Rate limit exceeded",
  "message": "Too many requests",
  "retry_after": 60,
  "code": "RATE_LIMIT_EXCEEDED",
  "context": {
    "retry_after_seconds": 60
  },
  "request_id": "req-12347"
}
```

### Validation Error (400)

```json
{
  "error": "Validation Error",
  "message": "Invalid request parameters",
  "field_errors": [
    {
      "field": "frequency",
      "message": "Frequency must be one of: daily, weekly, monthly, quarterly, yearly",
      "value": "invalid"
    }
  ],
  "code": "VALIDATION_ERROR",
  "request_id": "req-12348"
}
```

### Not Found (404)

```json
{
  "error": "Not Found",
  "message": "Series not found",
  "code": "SERIES_NOT_FOUND",
  "context": {
    "series_id": "INVALID:SERIES"
  },
  "request_id": "req-12349"
}
```

### Authentication Error (401)

```json
{
  "error": "Authentication required",
  "message": "Valid Bearer token required",
  "code": "AUTH_MISSING_TOKEN",
  "field": "authorization",
  "context": {
    "header_name": "Authorization"
  },
  "request_id": "req-12350"
}
```

### Internal Server Error (500)

```json
{
  "error": "Internal Server Error",
  "message": "Failed to retrieve external providers",
  "code": "EXTERNAL_PROVIDERS_ERROR",
  "context": {
    "error_type": "ConnectionError"
  },
  "request_id": "req-12351"
}
```

## Performance Features

- **Redis Caching**: Automatic caching with configurable TTL
- **Rate Limiting**: Per-route limits with distinct buckets for heavy endpoints
- **Circuit Breaker**: Automatic failure detection and recovery
- **Connection Pooling**: Efficient Trino connection management
- **Pagination**: Cursor-based pagination for stable iteration
- **Compression**: Automatic response compression
- **Metrics**: Comprehensive Prometheus metrics and OpenTelemetry tracing

## Curve Mapping Passthrough

Series with curve mappings automatically proxy to the internal curves API, providing unified access to both external and internal data sources.

## Rate Limits

- **Providers**: 100 requests/minute
- **Series**: 200 requests/minute
- **Observations**: 50 requests/minute (higher cost endpoint)
- **Metadata**: 100 requests/minute

All limits include burst capacity and are enforced per user/tenant.

## Response Headers

```bash
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 60
X-RateLimit-Tier: premium
ETag: "abc123"
Content-Type: application/json
```

## OpenAPI Specification

The complete API specification is available at:
`/docs` (Swagger UI) or download the OpenAPI spec from `/openapi.json`

## Postman Collection

Download the Postman collection with pre-configured requests:
[external-api-collection.json](external-api-collection.json)

## SDK Support

The External API is supported by:
- Python SDK: `pip install aurum-client`
- JavaScript SDK: `npm install @aurum/external-api`
- Go SDK: `go get github.com/aurum/external-api`
