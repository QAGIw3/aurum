# Migration Guide: API v1 → v2

This guide helps clients migrate from `/v1/*` endpoints to `/v2/*`.

## Key changes
- Versioned path prefix: `/v2` replaces `/v1`
- Consistent cursor pagination replaces offset pagination
- Typed, consistent error responses (RFC7807 style)
- Tenant scoping required via `X-Aurum-Tenant`

## Endpoint mapping (examples)
- GET `/v1/scenarios` → GET `/v2/scenarios`
- GET `/v1/catalog/series` → GET `/v2/catalog/series`
- GET `/v1/curves/{series_id}` → GET `/v2/curves/{series_id}`

## Pagination
- v1: `limit`, `offset` (deprecated)
- v2: `limit`, `cursor`; pass prior `next_cursor` as `cursor` to continue.

## Headers
- Send `X-Aurum-Tenant: <tenant_id>` on all requests.

## Responses
- v2 responses include `next_cursor` when more results are available.

## Timeline
- Deprecation window: 30 days from announcement
- After the window, `/v1/*` is removed

## Help
- Contact support with example requests/responses if issues arise during migration.
