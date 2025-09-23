# Runtime Configuration API

This document explains the admin runtime configuration endpoints for rate limits and feature flags, and how to audit changes. These endpoints require administrator access and are mounted under `/v1/admin/config/*`.

## Authentication and Access

- Requires an authenticated principal in an admin group. Configure with `AURUM_API_ADMIN_GROUP` and OIDC/JWT settings. See `README.md:API` and `docs/auth/oidc-forward-auth.md`.
- All endpoints return a standard envelope with `meta.request_id` for traceability.

## Rate Limits

- Get all rate-limit overrides for a tenant:
  `GET /v1/admin/config/ratelimit/{tenant_id}`

- Update a rate limit for an endpoint path for a tenant:
  `PUT /v1/admin/config/ratelimit/{tenant_id}/{path:path}`

  Request body:
  ```json
  {
    "requests_per_second": 20,
    "burst": 40,
    "enabled": true
  }
  ```

  Notes:
  - Changes apply immediately in-memory.
  - The current implementation stores tenant overrides in `AurumSettings` during process lifetime and logs an audit entry. In production, persist to durable storage (DB/config store) before rolling.

## Feature Flags

- Get all feature flags for a tenant:
  `GET /v1/admin/config/feature-flags/{tenant_id}`

  Note: returns an empty list until storage exposure is implemented for listing. Individual reads/writes are supported via the store.

- Update a feature flag for a tenant:
  `PUT /v1/admin/config/feature-flags/{tenant_id}/{feature_name}`

  Request body:
  ```json
  {
    "enabled": true,
    "configuration": {
      "beta_threshold": 0.25,
      "notes": "Allow scenario outputs API"
    }
  }
  ```

  Notes:
  - Persists via `ScenarioStore.set_feature_flag` and logs an audit entry.
  - Flags are tenant-scoped; use consistent `feature_name` keys across services.

## Audit Log

- Query audit log (filterable) for config changes:
  `GET /v1/admin/config/audit-log?tenant_id=...&user_id=...&limit=100`

  Response includes entries of the form:
  ```json
  {
    "id": "uuid",
    "tenant_id": "tenant-123",
    "user_id": "user@example.com",
    "action": "update_rate_limit",
    "resource_type": "rate_limit",
    "resource_id": "/v1/curves",
    "old_value": {"rps": 10, "burst": 20},
    "new_value": {"rps": 20, "burst": 40, "enabled": true},
    "timestamp": "2025-09-12T12:34:56Z",
    "request_id": "req-abc"
  }
  ```

Implementation: see `src/aurum/api/runtime_config.py` (service and router) and `AurumSettings.api.rate_limit` in `src/aurum/core/settings.py`.

## Operational Guidance

- Rollout: When moving from in-memory to persistent storage, seed overrides and feature flags from DB on startup; expose read-only endpoints to verify state before writes.
- RBAC: Lock these endpoints to admin groups only. Verify group claims are mapped correctly in your IdP.
- Telemetry: All changes are logged with `tenant_id`, `user_id`, and `request_id`. Forward logs to your SIEM and alert on high-frequency changes.

