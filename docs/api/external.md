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

The External Data API emits structured RFC 7807 problem responses. Integrations should rely on the shared `ExternalAPIClient` located at `src/aurum/api/client.py` to benefit from standardized retry, circuit breaker, authentication, and caching behaviour. The client automatically records telemetry spans and events using `aurum.observability.tracing` and updates the Prometheus counters/histograms exported from `aurum.observability.metrics` (`aurum_external_api_requests_total`, `aurum_external_api_request_duration_seconds`).

### Consuming External APIs from services

All outbound calls from Aurum services should go through `ExternalAPIClient` to guarantee:

- Retries with exponential backoff and jitter for transient failures (e.g., 5xx, 429, 408)
- Circuit breaker protection to avoid cascading outages (`aurum.common.circuit_breaker`)
- Shared authentication helpers for API keys, bearer tokens, and pluggable OAuth token providers
- Optional TTL caching for idempotent GET requests via `cachetools.TTLCache`
- Structured errors (`ExternalAPIResponseError`, `ExternalAPIAuthError`, etc.) so callers can branch on failure types
- Observability integration: `tracing.start_span("external.http.request")`, `tracing.record_event`, and Prometheus metrics (`aurum_external_api_requests_total`, `aurum_external_api_request_duration_seconds`)

```python
from aurum.api.client import CacheConfig, ClientConfig, ExternalAPIClient, RetryConfig

client = ExternalAPIClient(
    ClientConfig(
        base_url="https://provider.example/v1",
        retry=RetryConfig(max_attempts=4, base_delay_seconds=0.5, max_delay_seconds=8.0),
        cache=CacheConfig(enabled=True, ttl_seconds=120),
    )
)

response = client.get("/series", params={"provider": "fred", "limit": 100})
payload = response.json()
```

Services inheriting legacy `