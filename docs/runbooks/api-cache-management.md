## API Cache Management

This runbook describes how to inspect and clear API caches (Redis) by scope and tenant.

### Clear Cache via Admin Endpoint

- Endpoint: `POST /v2/admin/cache/clear`
- Query/body params:
  - `scope`: one of `all`, `market`, `catalog`, `metadata` (default: `all`)
  - `tenant_id`: optional tenant filter; clears only keys containing the tenant id

Example:

```bash
curl -X POST "http://localhost:8000/v2/admin/cache/clear?scope=market&tenant_id=acme" -H "Authorization: Bearer <token>"
```

Response:

```json
{
  "message": "Cache clear executed",
  "scope": "market",
  "tenant_id": "acme",
  "invalidated": 128
}
```

### Notes

- Market endpoints cache keys use the pattern: `aurum:*market:*` and include tenant id.
- Catalog and metadata caches use `aurum:*catalog:*` and `aurum:*metadata:*` respectively.
- Prefer targeted clears (`scope` + `tenant_id`) to minimize cache churn.


