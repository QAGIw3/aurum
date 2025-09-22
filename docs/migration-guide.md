# API Migration Guide: v1 to v2

This guide provides comprehensive instructions for migrating from Aurum API v1 to v2, including breaking changes, new features, and migration strategies.

## Overview

### Why Migrate?

API v2 introduces significant improvements:

- **Enhanced Performance**: Cursor-based pagination for better scalability
- **Better Error Handling**: RFC 7807 compliant error responses
- **Improved Caching**: Enhanced ETag support for better cache efficiency
- **Better Observability**: Detailed request tracing and metrics
- **Consistent Responses**: Standardized response formats across all endpoints
- **Type Safety**: Better validation and error handling

### Migration Timeline

- **v1 Deprecation**: January 1, 2024
- **v1 Sunset**: December 31, 2025
- **v1 Removal**: v3.0.0 (estimated 2026)

## Breaking Changes

### 1. Pagination Changes

#### v1 (Offset-based)
```bash
GET /v1/scenarios?offset=0&limit=10
```

#### v2 (Cursor-based)
```bash
GET /v2/scenarios?cursor=eyJ0ZW5hbnRfaWQiOiAiYWNtZS1jb3JwIiwgIm9mZnNldCI6IDB9&limit=10
```

**Migration Required:**
- Replace `offset` parameter with `cursor`
- Update pagination logic to handle cursor-based responses
- Update frontend pagination components

### 2. Error Response Format

#### v1 (Inconsistent)
```json
{
  "detail": "Scenario not found",
  "error": "NotFoundError"
}
```

#### v2 (RFC 7807 Compliant)
```json
{
  "type": "scenario_not_found",
  "title": "Scenario Not Found",
  "detail": "The requested scenario 'scenario-123' was not found",
  "instance": "/v2/scenarios/scenario-123",
  "request_id": "req-12345678",
  "processing_time_ms": 45.2
}
```

**Migration Required:**
- Update error handling to use new error structure
- Map error types to appropriate HTTP status codes
- Update error messages and localization

### 3. Response Format Changes

#### v1 (Minimal metadata)
```json
{
  "id": "scenario-1",
  "name": "Test Scenario",
  "created_at": "2024-01-01T10:00:00Z"
}
```

#### v2 (Enhanced metadata)
```json
{
  "id": "scenario-1",
  "name": "Test Scenario",
  "created_at": "2024-01-01T10:00:00Z",
  "meta": {
    "request_id": "req-12345678",
    "processing_time_ms": 45.2,
    "version": "v2"
  }
}
```

**Migration Required:**
- Handle new `meta` field in responses
- Use `meta` information for debugging and monitoring
- Update response parsing logic

## New Features in v2

### 1. Enhanced ETag Support

v2 provides improved caching with ETags:

```python
# Automatic ETag generation
response = await get_scenario(scenario_id)
# ETag header automatically added to response

# Conditional requests
headers = {"If-None-Match": etag_value}
response = await get_scenario(scenario_id, headers=headers)
# Returns 304 Not Modified if unchanged
```

### 2. Cursor-based Pagination

v2 uses cursor-based pagination for better performance:

```python
# v2 pagination response
{
  "data": [...],
  "meta": {
    "cursor": "eyJ0ZW5hbnRfaWQiOiAiYWNtZS1jb3JwIiwgIm9mZnNldCI6IDEwfQ==",
    "next_cursor": "eyJ0ZW5hbnRfaWQiOiAiYWNtZS1jb3JwIiwgIm9mZnNldCI6IDIwfQ==",
    "has_more": true
  },
  "links": {
    "next": "/v2/scenarios?cursor=...&limit=10"
  }
}
```

### 3. Improved Error Handling

v2 provides structured error responses:

```python
# Error response example
{
  "type": "validation_error",
  "title": "Invalid Request Parameters",
  "detail": "The 'limit' parameter must be between 1 and 100",
  "instance": "/v2/scenarios",
  "errors": [
    {
      "field": "limit",
      "message": "must be between 1 and 100",
      "value": 0
    }
  ]
}
```

### 4. Better Observability

v2 includes comprehensive tracing and metrics:

```python
# Request tracing
# All requests include X-Request-ID header
# Detailed timing information in response meta
# Comprehensive error context
```

## Migration Strategy

### Phase 1: Assessment (1-2 weeks)

1. **API Inventory**: Catalog all v1 API usage
2. **Impact Analysis**: Identify affected components
3. **Dependency Mapping**: Find integration points
4. **Testing Setup**: Prepare test environments

### Phase 2: Parallel Implementation (4-8 weeks)

1. **Dual API Support**: Run v1 and v2 simultaneously
2. **Gradual Migration**: Migrate endpoints incrementally
3. **Backward Compatibility**: Maintain v1 functionality
4. **Testing**: Comprehensive test coverage for v2

### Phase 3: Transition (2-4 weeks)

1. **Feature Parity**: Ensure v2 matches v1 functionality
2. **Performance Testing**: Validate v2 improvements
3. **Client Migration**: Update all API consumers
4. **Monitoring**: Track migration progress

### Phase 4: Deprecation (Ongoing)

1. **Deprecation Notices**: Add deprecation headers to v1
2. **Migration Monitoring**: Track v1 usage decline
3. **Support Migration**: Help teams migrate to v2
4. **Sunset Planning**: Prepare for v1 removal

## Endpoint-by-Endpoint Migration

### Scenarios API

#### List Scenarios

**v1:**
```bash
GET /v1/scenarios?offset=0&limit=10
```

**v2:**
```bash
GET /v2/scenarios?cursor=eyJ0ZW5hbnRfaWQiOiAiYWNtZS1jb3JwIiwgIm9mZnNldCI6IDB9&limit=10
```

**Migration Steps:**
1. Replace `offset` parameter with `cursor`
2. Update response parsing for new format
3. Handle new `meta` and `links` fields

#### Create Scenario

**v1:**
```bash
POST /v1/scenarios
{
  "name": "Test Scenario",
  "tenant_id": "acme-corp"
}
```

**v2:**
```bash
POST /v2/scenarios
{
  "name": "Test Scenario",
  "tenant_id": "acme-corp"
}
# Response includes meta field with timing information
```

**Migration Steps:**
1. Update response parsing to handle `meta` field
2. Use timing information for performance monitoring
3. Handle new error response format

### Curves API

#### List Curves

**v1:**
```bash
GET /v1/curves?offset=0&limit=10
```

**v2:**
```bash
GET /v2/curves?cursor=eyJ0ZW5hbnRfaWQiOiAiYWNtZS1jb3JwIiwgIm9mZnNldCI6IDB9&limit=10
```

**Migration Steps:**
1. Replace `offset` parameter with `cursor`
2. Update response parsing for enhanced format
3. Handle new pagination metadata

## Testing Migration

### Unit Tests

```python
# Test v2 pagination
def test_v2_cursor_pagination():
    # Test cursor encoding/decoding
    # Test response format
    # Test edge cases

# Test v2 error handling
def test_v2_error_responses():
    # Test RFC 7807 compliance
    # Test error structure
    # Test error mapping
```

### Integration Tests

```python
# Test full API migration
async def test_api_migration():
    # Test v1 to v2 compatibility
    # Test data consistency
    # Test performance improvements
```

### Performance Tests

```python
# Test v2 performance improvements
def test_v2_performance():
    # Test cursor pagination performance
    # Test ETag caching effectiveness
    # Test error response overhead
```

## Client Migration Examples

### Python Client

```python
# Before (v1)
import requests

response = requests.get(
    "https://api.aurum.com/v1/scenarios",
    params={"offset": 0, "limit": 10}
)

if response.status_code == 200:
    scenarios = response.json()
    # Handle pagination manually
    next_offset = scenarios.get("next_offset")

# After (v2)
import requests

response = requests.get(
    "https://api.aurum.com/v2/scenarios",
    params={"limit": 10}
)

if response.status_code == 200:
    data = response.json()
    scenarios = data["data"]
    meta = data["meta"]
    links = data["links"]

    # Use cursor for next page
    next_url = links.get("next")
    if next_url:
        next_response = requests.get(next_url)
```

### JavaScript Client

```javascript
// Before (v1)
const response = await fetch('/v1/scenarios?offset=0&limit=10');
const scenarios = await response.json();

// After (v2)
const response = await fetch('/v2/scenarios?limit=10');
const data = await response.json();
const scenarios = data.data;
const hasMore = data.meta.has_more;
const nextCursor = data.meta.next_cursor;
```

### Go Client

```go
// Before (v1)
type V1ScenarioResponse struct {
    ID   string `json:"id"`
    Name string `json:"name"`
}

// After (v2)
type V2ScenarioResponse struct {
    ID   string                 `json:"id"`
    Name string                 `json:"name"`
    Meta V2Metadata            `json:"meta"`
}

type V2Metadata struct {
    RequestID        string  `json:"request_id"`
    ProcessingTimeMS float64 `json:"processing_time_ms"`
    Version         string  `json:"version"`
}
```

## Monitoring Migration Progress

### Metrics to Track

1. **API Usage by Version**
   ```sql
   -- Query to track v1 vs v2 usage
   SELECT
     version,
     COUNT(*) as requests,
     DATE(timestamp) as date
   FROM api_requests
   GROUP BY version, DATE(timestamp)
   ```

2. **Migration Completion Rate**
   ```sql
   -- Track which clients have migrated
   SELECT
     client_id,
     api_version,
     last_request_time
   FROM client_api_usage
   WHERE api_version IN ('v1', 'v2')
   ```

3. **Error Rate by Version**
   ```sql
   -- Monitor error rates during migration
   SELECT
     api_version,
     error_rate,
     DATE(timestamp) as date
   FROM api_error_rates
   ```

### Dashboard Setup

1. **Create Grafana Dashboard** with panels for:
   - API usage by version over time
   - Migration progress by endpoint
   - Error rate comparison v1 vs v2
   - Performance metrics comparison

2. **Set Up Alerts** for:
   - Sudden drop in v2 usage
   - Increased error rates in v2
   - v1 usage after sunset date

## Rollback Strategy

### Immediate Rollback

```bash
# Disable v2 endpoints temporarily
kubectl patch deployment aurum-api -p '{"spec":{"replicas":0}}'
kubectl patch deployment aurum-api-v1 -p '{"spec":{"replicas":3}}'

# Update load balancer
kubectl patch service aurum-api -p '{"spec":{"selector":{"version":"v1"}}}'
```

### Gradual Rollback

```bash
# Rollback specific endpoints
kubectl patch configmap api-routing -p '{"data":{"v2_enabled":"false"}}'

# Monitor rollback impact
kubectl logs -f deployment/aurum-api -n aurum-dev | grep -E "(ERROR|WARN)"
```

### Data Rollback

```bash
# If data corruption occurred
kubectl exec -it postgresql-0 -n aurum-dev -- \
  psql -c "SELECT * FROM schema_migrations WHERE version = 'v2_rollback'"
```

## Support and Communication

### Developer Communication

1. **Migration Announcements**
   - Email to all API consumers
   - Blog post with migration guide
   - Slack channel for support

2. **Documentation Updates**
   - Update API documentation
   - Add migration examples
   - Provide code samples

3. **Support Channels**
   - Dedicated Slack channel: #api-migration
   - Email support: api-migration@aurum-corp.com
   - Office hours for migration assistance

### Support Resources

- **Migration Guide**: This document
- **API Reference**: Updated v2 documentation
- **Code Samples**: GitHub repository with examples
- **Testing Tools**: Automated migration testing scripts

## Best Practices

### During Migration

1. **Test Thoroughly**: Validate all changes in staging
2. **Monitor Closely**: Watch metrics during rollout
3. **Communicate Clearly**: Keep stakeholders informed
4. **Rollback Ready**: Have rollback plan prepared
5. **Gradual Approach**: Migrate incrementally, not all at once

### After Migration

1. **Performance Validation**: Confirm v2 improvements
2. **Error Monitoring**: Watch for new error patterns
3. **Client Feedback**: Gather feedback from API consumers
4. **Documentation Updates**: Keep docs current
5. **Retrospective**: Review what went well and what to improve

## Troubleshooting

### Common Migration Issues

#### 1. Cursor Decoding Errors

**Problem:** Invalid cursor format errors
**Solution:**
```python
import base64
import json

def decode_cursor(cursor: str) -> dict:
    try:
        return json.loads(base64.urlsafe_b64decode(cursor.encode()))
    except Exception:
        raise ValueError("Invalid cursor format")
```

#### 2. Response Format Changes

**Problem:** Client code fails with new response format
**Solution:**
```python
def handle_v2_response(response_data: dict) -> dict:
    # Extract data from v2 format
    if "data" in response_data:
        return response_data["data"]
    return response_data
```

#### 3. Error Response Changes

**Problem:** Error handling code breaks with new format
**Solution:**
```python
def handle_v2_error(response: requests.Response) -> dict:
    if response.status_code >= 400:
        error_data = response.json()
        # Map to legacy format if needed
        return {
            "error": error_data.get("type", "unknown_error"),
            "detail": error_data.get("detail", "Unknown error")
        }
    return {}
```

### Performance Issues

#### 1. Slow Cursor Queries

**Problem:** Cursor pagination slower than expected
**Solution:**
- Add database indexes for cursor columns
- Optimize cursor generation queries
- Consider pagination table for large datasets

#### 2. High Memory Usage

**Problem:** v2 endpoints using more memory
**Solution:**
- Implement streaming responses for large datasets
- Add response size limits
- Optimize JSON serialization

## Success Metrics

### Migration Success Criteria

1. **Zero Downtime**: No service interruption during migration
2. **Performance Improvement**: v2 endpoints faster than v1
3. **Error Reduction**: Fewer errors in v2 than v1
4. **Client Adoption**: All major clients migrated within timeline
5. **Monitoring Coverage**: Full observability of v2 endpoints

### Post-Migration Metrics

1. **API Response Times**: Average, P95, P99
2. **Error Rates**: By endpoint and error type
3. **Cache Hit Rates**: ETag effectiveness
4. **Client Migration Progress**: Percentage of clients on v2
5. **User Satisfaction**: Feedback from API consumers

## Resources

### Documentation
- [API v2 Reference](https://api.aurum.com/docs/v2/)
- [Migration Examples](https://github.com/supernova-corp/aurum/tree/main/examples/migration)
- [Breaking Changes List](https://github.com/supernova-corp/aurum/blob/main/CHANGELOG.md)

### Support
- **Email**: api-migration@aurum-corp.com
- **Slack**: #api-migration
- **GitHub Issues**: [Migration Label](https://github.com/supernova-corp/aurum/labels/migration)

### Tools
- **Migration Script**: Automated code migration tool
- **Testing Framework**: Comprehensive test suite
- **Monitoring Dashboard**: Real-time migration tracking

---

*This migration guide ensures a smooth transition from v1 to v2 while maximizing the benefits of the enhanced API features.*
