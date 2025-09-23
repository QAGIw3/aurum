# Aurum API Usage Guide

This guide provides practical examples and common pitfalls when using the Aurum API. It covers the core endpoints for curves, metadata, scenarios, and outputs.

## Quick Start

### Authentication
The API supports multiple authentication methods:

#### Option 1: Disabled Authentication (Development)
```bash
export AURUM_API_AUTH_DISABLED=1
curl -X GET "http://localhost:8000/v1/curves?iso=PJM&market=DAY_AHEAD&location=WEST&asof=2024-01-15"
```

#### Option 2: JWT Token Authentication
```bash
# Get JWT token (replace with your OIDC provider)
TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

curl -X GET "http://localhost:8000/v1/curves" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Aurum-Tenant: your-tenant-id"
```

#### Option 3: Traefik Forward-Auth
```bash
# Configure Traefik to forward auth headers
curl -X GET "http://localhost:8000/v1/curves" \
  -H "X-Forwarded-User: user@example.com" \
  -H "X-Forwarded-Claims: {\"email\":\"user@example.com\",\"groups\":[\"users\"]}"
```

### Base URL
- Development: `http://localhost:8000`
- Production: `https://api.aurum-platform.com`

---

## Core Endpoints

## 1. Curves API

### Get Curve Data
Retrieve curve observations for a specific date and market.

```bash
curl -X GET "http://localhost:8000/v1/curves?iso=PJM&market=DAY_AHEAD&location=WEST&asof=2024-01-15"
```

**Response:**
```json
{
  "data": [
    {
      "iso": "PJM",
      "market": "DAY_AHEAD",
      "location": "WEST",
      "asof": "2024-01-15",
      "observation_time": "2024-01-15T10:00:00Z",
      "price": 50.25,
      "volume": 1000.0
    }
  ],
  "meta": {
    "request_id": "req-abc123",
    "timestamp": "2024-01-15T12:00:00Z",
    "tenant_id": "tenant-123",
    "total": 24
  }
}
```

### Compare Curves (Diff)
Compare curve data between two dates.

```bash
curl -X GET "http://localhost:8000/v1/curves/diff?iso=PJM&market=DAY_AHEAD&location=WEST&asof_a=2024-01-15&asof_b=2024-01-14&dimension=price"
```

**Common Pitfalls:**
- Ensure `asof_a` and `asof_b` are different dates
- Date difference cannot exceed 365 days
- At least one dimension filter is required

### Curve Data with Pagination
Use cursor-based pagination for large datasets.

```bash
# First page
curl -X GET "http://localhost:8000/v1/curves?iso=PJM&market=DAY_AHEAD&asof=2024-01-15&limit=10"

# Next page
curl -X GET "http://localhost:8000/v1/curves?cursor=eyJsaW1pdCI6MTAsIm9mZnNldCI6MTAsInRpbWVzdGFtcCI6MTcwNTMxMjAwMH0"
```

**Common Pitfalls:**
- Cursor has a 1-hour expiration time
- Filters must match the original request
- Maximum limit is 500 records per request

---

## 2. Metadata API

### Get Dimensions
Retrieve available dimension values.

```bash
curl -X GET "http://localhost:8000/v1/metadata/dimensions"
```

**Response:**
```json
{
  "data": {
    "iso": ["PJM", "CAISO", "MISO", "ERCOT"],
    "market": ["DAY_AHEAD", "REAL_TIME"],
    "location": ["WEST", "EAST", "NORTH", "SOUTH"]
  },
  "meta": {
    "request_id": "req-def456",
    "timestamp": "2024-01-15T12:00:00Z",
    "tenant_id": "tenant-123"
  }
}
```

### Get Dimensions with Counts
Include frequency counts for each dimension value.

```bash
curl -X GET "http://localhost:8000/v1/metadata/dimensions?include_counts=true"
```

**Response:**
```json
{
  "data": {
    "iso": ["PJM", "CAISO", "MISO"],
    "market": ["DAY_AHEAD", "REAL_TIME"]
  },
  "counts": {
    "iso": [
      {"value": "PJM", "count": 1500},
      {"value": "CAISO", "count": 1200},
      {"value": "MISO", "count": 800}
    ],
    "market": [
      {"value": "DAY_AHEAD", "count": 2800},
      {"value": "REAL_TIME", "count": 1700}
    ]
  },
  "meta": {...}
}
```

**Common Pitfalls:**
- `include_counts=true` requires additional computation
- Only available if the feature flag is enabled for your tenant

---

## 3. Scenarios API

### List Scenarios
Get all scenarios for your tenant.

```bash
curl -X GET "http://localhost:8000/v1/scenarios"
```

**Response:**
```json
{
  "data": [
    {
      "id": "scenario-abc123",
      "name": "California RPS Analysis",
      "description": "Renewable Portfolio Standard impact analysis",
      "tenant_id": "tenant-123",
      "created_at": "2024-01-15T10:00:00Z",
      "updated_at": "2024-01-15T11:00:00Z"
    }
  ],
  "meta": {
    "request_id": "req-ghi789",
    "timestamp": "2024-01-15T12:00:00Z",
    "total": 1,
    "page": 1,
    "page_size": 20
  }
}
```

### Get Scenario Outputs
Retrieve scenario outputs with filtering and pagination.

```bash
curl -X GET "http://localhost:8000/v1/scenarios/scenario-abc123/outputs" \
  -G \
  -d "start_time=2024-01-15T00:00:00Z" \
  -d "end_time=2024-01-15T23:59:59Z" \
  -d "metric_name=power_output" \
  -d "min_value=0" \
  -d "max_value=1000" \
  -d "limit=100"
```

**Response:**
```json
{
  "outputs": [
    {
      "id": "output-def456",
      "scenario_run_id": "run-ghi789",
      "timestamp": "2024-01-15T10:00:00Z",
      "metric_name": "power_output",
      "value": 100.5,
      "unit": "MW",
      "tags": {"region": "west", "type": "renewable"}
    }
  ],
  "total": 1,
  "applied_filter": {
    "start_time": "2024-01-15T00:00:00Z",
    "end_time": "2024-01-15T23:59:59Z",
    "metric_name": "power_output",
    "min_value": 0,
    "max_value": 1000,
    "limit": 100,
    "offset": 0
  },
  "meta": {...}
}
```

### Bulk Scenario Runs
Create multiple scenario runs with deduplication.

```bash
curl -X POST "http://localhost:8000/v1/scenarios/scenario-abc123/runs:bulk" \
  -H "Content-Type: application/json" \
  -d '{
    "runs": [
      {
        "idempotency_key": "bulk-run-1",
        "parameters": {"policy": "rps_50", "year": 2025}
      },
      {
        "idempotency_key": "bulk-run-2",
        "parameters": {"policy": "rps_60", "year": 2025}
      }
    ]
  }'
```

**Response (202 Accepted):**
```json
{
  "results": {
    "bulk-run-1": {
      "scenario_run_id": "run-abc123",
      "status": "queued",
      "created_at": "2024-01-15T12:00:00Z"
    },
    "bulk-run-2": {
      "scenario_run_id": "run-def456",
      "status": "queued",
      "created_at": "2024-01-15T12:00:00Z"
    }
  },
  "total": 2,
  "successful": 2,
  "failed": 0
}
```

**Common Pitfalls:**
- Idempotency keys must be unique within the request
- Maximum 100 runs per bulk request
- Bulk runs require the `BULK_SCENARIO_RUNS` feature flag

---

## Health and Monitoring

### Health Check
Basic health check for load balancers.

```bash
curl -X GET "http://localhost:8000/health"
```

**Response:**
```json
{"status": "healthy"}
```

### Readiness Check
Deep health check with database connectivity tests.

```bash
curl -X GET "http://localhost:8000/ready"
```

**Response:**
```json
{
  "status": "ready",
  "checks": {
    "database": {"status": "healthy", "response_time_ms": 15},
    "trino": {"status": "healthy", "response_time_ms": 45},
    "schema_registry": {"status": "healthy", "response_time_ms": 23}
  }
}
```

### Metrics
Prometheus metrics for monitoring.

```bash
curl -X GET "http://localhost:8000/metrics"
```

**Common Pitfalls:**
- `/health` should be used for load balancer checks
- `/ready` performs deep checks and may be slower
- Metrics endpoint requires proper Prometheus scraping configuration

---

## Error Handling

### Common HTTP Status Codes

| Code | Description | Example |
|------|-------------|---------|
| 200 | Success | Normal response |
| 202 | Accepted | Bulk operations queued |
| 400 | Bad Request | Invalid parameters |
| 401 | Unauthorized | Missing/invalid authentication |
| 403 | Forbidden | Feature not enabled or insufficient permissions |
| 404 | Not Found | Resource doesn't exist |
| 429 | Rate Limited | Too many requests |
| 500 | Internal Error | Server error |

### Error Response Format
```json
{
  "error": {
    "type": "validation_error",
    "message": "Invalid request parameters",
    "field": "iso",
    "value": "",
    "detail": "ISO code cannot be empty"
  },
  "validation_errors": [
    {
      "field": "iso",
      "message": "ISO code is required",
      "code": "required_field"
    }
  ],
  "meta": {
    "request_id": "req-abc123",
    "timestamp": "2024-01-15T12:00:00Z",
    "tenant_id": "tenant-123"
  }
}
```

---

## Common Pitfalls and Solutions

### 1. Authentication Issues

**Problem:** 401 Unauthorized responses
```bash
# ❌ Wrong
curl -X GET "http://localhost:8000/v1/curves"

# ✅ Correct
curl -X GET "http://localhost:8000/v1/curves" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

**Solution:** Set `AURUM_API_AUTH_DISABLED=1` for development or provide valid JWT token.

### 2. Rate Limiting

**Problem:** 429 Too Many Requests
```bash
# ❌ Too many requests
for i in {1..1000}; do
  curl -X GET "http://localhost:8000/v1/curves"
done
```

**Solution:** Check rate limit headers and implement exponential backoff:
```bash
# Check rate limit status
curl -X GET "http://localhost:8000/v1/curves" \
  -v | grep -i "x-ratelimit"
```

### 3. Invalid Parameters

**Problem:** 400 Bad Request for invalid date formats
```bash
# ❌ Wrong date format
curl -X GET "http://localhost:8000/v1/curves?asof=01-15-2024"

# ✅ Correct ISO format
curl -X GET "http://localhost:8000/v1/curves?asof=2024-01-15"
```

**Common Issues:**
- Use ISO 8601 format: `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SSZ`
- ISO codes are uppercase 3-letter codes: `PJM`, `CAISO`
- Market values: `DAY_AHEAD`, `REAL_TIME`

### 4. Feature Flag Restrictions

**Problem:** 403 Forbidden for disabled features
```bash
# ❌ Feature not enabled
curl -X GET "http://localhost:8000/v1/scenarios/test/outputs?start_time=2024-01-01"
```

**Solution:** Contact your tenant administrator to enable required features:
- `SCENARIO_OUTPUTS_ENABLED`
- `SCENARIO_OUTPUTS_FILTERING`
- `SCENARIO_OUTPUTS_PAGINATION`

### 5. Cursor Expiration

**Problem:** Invalid cursor errors
```bash
# ❌ Using old cursor
curl -X GET "http://localhost:8000/v1/curves?cursor=expired-cursor"
```

**Solution:** Cursors expire after 1 hour. Always use cursors immediately after receiving them.

### 6. Large Response Handling

**Problem:** Memory issues with large datasets
```bash
# ❌ Requesting too much data
curl -X GET "http://localhost:8000/v1/curves?limit=10000"

# ✅ Use pagination
curl -X GET "http://localhost:8000/v1/curves?limit=100"
```

**Solution:** Use pagination with reasonable limits (max 500 per request).

### 7. Tenant Isolation

**Problem:** Accessing data from wrong tenant
```bash
# ❌ Missing tenant header
curl -X GET "http://localhost:8000/v1/scenarios"

# ✅ Include tenant ID
curl -X GET "http://localhost:8000/v1/scenarios" \
  -H "X-Aurum-Tenant: your-tenant-id"
```

**Solution:** Always include `X-Aurum-Tenant` header or ensure proper JWT claims.

### 8. Schema Validation

**Problem:** 400 errors for malformed requests
```json
// ❌ Invalid JSON
{
  "runs": [
    {
      "idempotency_key": "test-1",
      "parameters": "not-an-object"  // Should be object
    }
  ]
}
```

**Solution:** Validate JSON structure against API documentation.

### 9. Timeout Handling

**Problem:** Requests timing out
```bash
# ❌ No timeout
curl -X GET "http://localhost:8000/v1/curves?asof=2024-01-15"

# ✅ Set reasonable timeout
curl -X GET "http://localhost:8000/v1/curves?asof=2024-01-15" \
  --max-time 30
```

**Solution:** Set appropriate timeouts (30-60 seconds for most requests).

### 10. Compression

**Problem:** Large responses not compressed
```bash
# ❌ Missing compression header
curl -X GET "http://localhost:8000/v1/curves"

# ✅ Request compression
curl -X GET "http://localhost:8000/v1/curves" \
  -H "Accept-Encoding: gzip"
```

**Solution:** Include `Accept-Encoding: gzip` header for better performance.

---

## Best Practices

### 1. Use Appropriate HTTP Methods
- `GET` for retrieving data
- `POST` for creating resources
- `PUT` for updating resources
- `DELETE` for removing resources

### 2. Implement Pagination
```bash
# Always handle pagination for large datasets
first_page=$(curl -s "http://localhost:8000/v1/curves?limit=100")
next_cursor=$(echo $first_page | jq -r '.meta.next_cursor // empty')

if [ -n "$next_cursor" ]; then
  curl "http://localhost:8000/v1/curves?cursor=$next_cursor"
fi
```

### 3. Handle Errors Gracefully
```bash
# Check status code before processing response
response=$(curl -s -w "%{http_code}" -o response_body.txt "http://localhost:8000/v1/curves")
status_code=${response: -3}

if [ "$status_code" -eq 200 ]; then
  cat response_body.txt | jq '.data'
elif [ "$status_code" -eq 400 ]; then
  echo "Bad request: $(cat response_body.txt | jq '.error.message')"
elif [ "$status_code" -eq 429 ]; then
  echo "Rate limited - retry later"
fi
```

### 4. Use Structured Logging
```bash
# Log API calls with correlation IDs
curl -X GET "http://localhost:8000/v1/curves" \
  -H "X-Request-ID: $(uuidgen)" \
  -v 2>&1 | grep -E "(>|<)" | logger -t aurum-api
```

### 5. Monitor Performance
```bash
# Time API calls
time curl -s "http://localhost:8000/v1/curves"

# Monitor rate limits
curl -X GET "http://localhost:8000/v1/curves" \
  -I | grep -i ratelimit
```

### 6. Cache Responses
```bash
# Use ETags for caching
etag=$(curl -s "http://localhost:8000/v1/metadata/dimensions" \
  -H "If-None-Match: \"previous-etag\"" \
  -I | grep -i etag | cut -d'"' -f2)

curl -s "http://localhost:8000/v1/metadata/dimensions" \
  -H "If-None-Match: \"$etag\""
```

### 7. Handle Concurrent Requests
```bash
# Use connection pooling for multiple requests
curl --http1.1 --keepalive-time 30 \
  "http://localhost:8000/v1/curves"
```

### 8. Validate Data
```bash
# Check response structure
response=$(curl -s "http://localhost:8000/v1/curves")
echo $response | jq 'has("data") and has("meta")'

# Validate required fields
echo $response | jq '.data[0] | has("iso") and has("price")'
```

### 9. Use Environment Variables
```bash
# Set up environment for API calls
export AURUM_BASE_URL="http://localhost:8000"
export AURUM_TENANT_ID="your-tenant-id"
export AURUM_API_TOKEN="your-token"

curl -X GET "$AURUM_BASE_URL/v1/curves" \
  -H "X-Aurum-Tenant: $AURUM_TENANT_ID" \
  -H "Authorization: Bearer $AURUM_API_TOKEN"
```

### 10. Test in Development First
```bash
# Use development environment
export AURUM_API_AUTH_DISABLED=1
export AURUM_API_RATE_LIMIT_ENABLED=0

# Test basic functionality
curl "http://localhost:8000/health"
curl "http://localhost:8000/v1/metadata/dimensions"
```

---

## Troubleshooting Checklist

### 1. Check Authentication
- Is `AURUM_API_AUTH_DISABLED=1` set for development?
- Are JWT tokens valid and not expired?
- Are tenant claims properly configured?

### 2. Verify Parameters
- Are all required parameters provided?
- Are parameter formats correct (ISO dates, uppercase codes)?
- Are parameter values within allowed ranges?

### 3. Check Feature Flags
- Are required features enabled for your tenant?
- Do you have appropriate permissions?

### 4. Review Rate Limits
- Are you exceeding rate limits?
- Check `X-RateLimit-Remaining` header

### 5. Validate Network
- Can you reach the API server?
- Are there network timeouts or DNS issues?
- Check firewall and proxy settings

### 6. Monitor Performance
- Are response times acceptable?
- Is the API under load?
- Check `/ready` endpoint for service health

### 7. Examine Logs
- Check API logs for error details
- Look for request IDs in responses
- Review audit logs for access patterns

### 8. Test Environment
- Are you using the correct environment (dev/prod)?
- Are environment variables properly set?
- Is the API version compatible?

---

## API Client Libraries

### Python Client
```python
from aurum.client import AurumClient

client = AurumClient(
    base_url="http://localhost:8000",
    tenant_id="your-tenant-id",
    api_token="your-token"
)

# Get curve data
curves = client.get_curves(
    iso="PJM",
    market="DAY_AHEAD",
    location="WEST",
    asof="2024-01-15"
)

# List scenarios
scenarios = client.list_scenarios()

# Get scenario outputs
outputs = client.get_scenario_outputs(
    scenario_id="scenario-123",
    start_time="2024-01-15T00:00:00Z",
    end_time="2024-01-15T23:59:59Z"
)
```

### TypeScript/JavaScript Client
```typescript
import { AurumClient } from '@aurum-platform/client';

const client = new AurumClient({
  baseUrl: 'http://localhost:8000',
  tenantId: 'your-tenant-id',
  apiToken: 'your-token'
});

// Get curve data
const curves = await client.curves.get({
  iso: 'PJM',
  market: 'DAY_AHEAD',
  location: 'WEST',
  asof: '2024-01-15'
});

// List scenarios
const scenarios = await client.scenarios.list();

// Get scenario outputs
const outputs = await client.scenarios.getOutputs('scenario-123', {
  startTime: '2024-01-15T00:00:00Z',
  endTime: '2024-01-15T23:59:59Z'
});
```

---

## Support and Resources

### Documentation
- [API Reference](http://localhost:8000/docs) - Interactive API documentation
- [OpenAPI Spec](http://localhost:8000/openapi.json) - Machine-readable API specification
- [Developer Portal](https://developers.aurum-platform.com) - Additional resources

### Monitoring
- Health Check: `GET /health`
- Readiness Check: `GET /ready`
- Metrics: `GET /metrics`
- Logs: Check structured logs for detailed error information

### Contact
- **API Support**: api-support@aurum-platform.com
- **Documentation Issues**: docs@aurum-platform.com
- **Feature Requests**: features@aurum-platform.com

---

*This guide is maintained by the Aurum Platform team. Last updated: 2024-01-15*
