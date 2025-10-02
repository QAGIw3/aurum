# API Authentication & RBAC

This guide explains how to authenticate to the Aurum API and how roles and groups are enforced for authorization.

## Modes

- Development: set `AURUM_API_AUTH_DISABLED=1` to disable auth for local testing.
- OIDC/JWT (recommended): validate JWT access tokens on requests, map claims to tenants and groups.
- Forward-auth (Traefik): see docs/auth/oidc-forward-auth.md for gateway enforcement.

## Configuration (environment)

- `AURUM_API_AUTH_DISABLED`: `0|1` (default `0`)
- `AURUM_API_OIDC_ISSUER`: OIDC issuer (e.g., `https://login.example.com/realms/aurum`)
- `AURUM_API_OIDC_AUDIENCE`: expected audience claim
- `AURUM_API_OIDC_JWKS_URL`: JWKS endpoint
- `AURUM_API_ADMIN_GROUP`: group name(s) that confer admin (comma‑separated)
- `AURUM_API_CORS_ORIGINS`: optional comma‑separated origins (e.g., `https://app.example.com`)

## Tenant Resolution

Priority order per request:
1) Tenant claim in JWT (if present)
2) `X-Tenant-ID` header
3) `X-Aurum-Tenant` header (legacy)

Responses echo the resolved tenant for observability.

## Curl examples

Bearer token:

```
TOKEN=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
TENANT=my-tenant
curl -s \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Tenant-ID: $TENANT" \
  "http://localhost:8095/v1/scenarios?limit=10"
```

Admin endpoint (rate limit update):

```
curl -s -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "http://localhost:8095/v1/admin/config/ratelimit/$TENANT/v1/curves" \
  -d '{"requests_per_second":20, "burst":40, "enabled": true}'
```

## RBAC

- Permissions are evaluated from JWT groups mapped to application roles.
- Set `AURUM_API_ADMIN_GROUP` to one or more groups that unlock admin routes (comma‑separated).
- Non‑admin clients can access tenant‑scoped routes based on their assigned roles; forbidden responses return 403.

## Troubleshooting

- 401: token missing/invalid; check issuer/audience/JWKS URL, clock skew.
- 403: insufficient permissions or wrong tenant; verify group membership and tenant header/claim.
- Inspect logs for `aurum.api.access` and use `X-Request-Id` for correlation.

