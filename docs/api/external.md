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

### Catalog Series

Expose the curated catalog of external datasets for a tenant. Supports filtering, cursor pagination, and conditional caching via ETag.

```bash
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
     "https://api.aurum.local/v1/catalog/series?tenant_id=tenant-123&provider=ercot&limit=25"
```

**Query Parameters:**
- `tenant_id` (required): Tenant scope; may also be provided via `X-Aurum-Tenant` header.
- `provider` (optional): Filter by source provider id (e.g. `ercot`).
- `dataset_code`, `status`, `category` (optional): Exact-match filters.
- `iso_code`, `iso_market`, `iso_product`, `iso_location_*` (optional): ISO metadata facets.
- `tags` (optional, repeatable): Tag array matched with `contains(tags, value)` semantics.
- `start_ts_from`, `start_ts_to`, `last_obs_from`, `last_obs_to` (optional): Timestamp ranges.
- `limit` (optional): Page size (default 50, max 200).
- `cursor` (optional): Opaque cursor returned in `meta.next_cursor`.

**Response:**
```json
{
  "data": [
    {
      "tenant_id": "tenant-123",
      "provider": "ercot",
      "series_id": "ERCOT:LZ_SOUTH:DA",
      "dataset_code": "LZ_SOUTH",
      "title": "ERCOT Day-Ahead South Load Zone",
      "status": "active",
      "tags": ["power", "load_zone"],
      "start_ts": "2017-01-01T00:00:00Z",
      "last_observation_ts": "2025-09-01T12:00:00Z"
    }
  ],
  "meta": {
    "request_id": "req-abc123",
    "query_time_ms": 42,
    "count": 25,
    "next_cursor": "eyJvZmZzZXQiOjI1fQ==",
    "has_more": true,
    "limit": 25,
    "offset": 0
  },
  "links": {
    "self": "https://api.aurum.local/v1/catalog/series?tenant_id=tenant-123&limit=25",
    "next": "https://api.aurum.local/v1/catalog/series?tenant_id=tenant-123&cursor=eyJvZmZzZXQiOjI1fQ=="
  }
}
```

**Headers:**
- `ETag`: Strong validator for conditional requests. Send `If-None-Match` with the returned value to receive `304 Not Modified` and avoid payload transfer when data is unchanged.
- `X-RateLimit-*`: Per-tenant rate limit headers. Hitting the policy returns `429` with `Retry-After` seconds.

### Coarse Search (Catalog + Curves)

Perform lightweight discovery across catalog entries and curve metadata. Useful for quick lookup experiences and auto-complete.

```bash
curl -H "Authorization: Bearer YOUR_TOKEN_HERE" \
     "https://api.aurum.local/v1/search?tenant_id=tenant-123&q=power%20houston&limit=10&facets=provider,doc_type"
```

**Query Parameters:**
- `tenant_id` (required)
- `q` (required): Space-delimited terms. All tokens must match (logical AND) across indexed fields.
- `filters` (optional): JSON object for structured constraints (e.g. `{ "doc_type": "series", "provider": ["ercot","pjm"] }`).
- `facets` (optional): Comma-separated list of facet fields (`doc_type`, `provider`, `iso_code`, `iso_market`, `iso_product`, `category`, `status`). Each facet returns top buckets with counts.
- `limit` (optional): Page size (default 20, max 100).
- `cursor` (optional): Opaque cursor for next page.

**Response:**
```json
{
  "data": [
    {
      "id": "ERCOT:LZ_HOUSTON:DA",
      "doc_type": "series",
      "title": "ERCOT Houston Load Zone Day-Ahead",
      "iso_market": "ERCOT",
      "provider": "ercot",
      "score": 2.0
    }
  ],
  "facets": {
    "provider": [{"value": "ercot", "count": 12}]
  },
  "meta": {
    "count": 10,
    "total": 240,
    "next_cursor": "eyJvZmZzZXQiOjEwfQ==",
    "offset": 0,
    "limit": 10,
    "request_id": "req-xyz456",
    "query_time_ms": 33
  },
  "links": {
    "self": "https://api.aurum.local/v1/search?tenant_id=tenant-123&q=power%20houston&limit=10",
    "next": "https://api.aurum.local/v1/search?tenant_id=tenant-123&q=power%20houston&cursor=eyJvZmZzZXQiOjEwfQ=="
  }
}
```

Search responses also surface `ETag` and rate-limit headers identical to catalog endpoints.

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