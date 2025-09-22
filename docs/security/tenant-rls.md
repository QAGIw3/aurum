# Tenant Isolation & Row-Level Security (RLS)

This guide explains how Aurum enforces tenant isolation at the database layer and how the API propagates tenant context.

## Overview

- Postgres tables are protected by Row-Level Security (RLS) policies keyed on `tenant_id`.
- The application sets `SET LOCAL app.current_tenant = '<tenant-uuid>'` per transaction/session.
- RLS policies reference `current_setting('app.current_tenant')::UUID` to filter rows transparently.

## Where it’s defined

- DDL and RLS policies: `postgres/ddl/app.sql`
- Migration to ensure coverage: `db/migrations/versions/20240921_05_tenant_rls_enforcement.py`
- API tenant resolution: `src/aurum/api/routes.py` (looks at principal, then `X-Tenant-ID` / `X-Aurum-Tenant` headers)
- DB context setting:
  - Async (asyncpg): `src/aurum/scenarios/storage.py:64`
  - Psycopg/cursors: `src/aurum/api/scenario_service.py:772`, `src/aurum/api/scenario_service_hardened.py:317`

## Client headers

- Preferred header: `X-Tenant-ID`
- Backward-compatible header: `X-Aurum-Tenant`
- The API resolves tenant in this priority order:
  1) Auth principal claim (if OIDC/JWT configured)
  2) `X-Tenant-ID`
  3) `X-Aurum-Tenant`

Responses include `X-Tenant-ID` (or `X-Aurum-Tenant`) for observability.

## Example: RLS policy

From `postgres/ddl/app.sql`:

```
CREATE POLICY tenant_isolation_scenario_output ON scenario_output
    USING (tenant_id = current_setting('app.current_tenant')::UUID);
```

Within request handling, the API sets:

```
SET LOCAL app.current_tenant = '<TENANT_UUID>'
```

All subsequent queries in the transaction are restricted by RLS.

## Applying and verifying migrations

- Apply DDL for a fresh database: run the statements in `postgres/ddl/app.sql` (or use your migration tool to bootstrap).
- Alembic migration (adds missing `tenant_id`, FKs, enables RLS, and creates policies):
  - File: `db/migrations/versions/20240921_05_tenant_rls_enforcement.py`
  - Upgrade: executes `SCHEMA_UPGRADE_STATEMENTS`
  - Downgrade: reverts indices, policies, RLS, and tenant columns

Verify in psql:

```
\d+ scenario_output  -- should show RLS enabled
SELECT policyname, qual FROM pg_policies WHERE tablename = 'scenario_output';
```

## Testing

- E2E header propagation: `tests/e2e/test_auth_rls_e2e.py`
- Scenario store RLS usage: `tests/scenarios/test_tenant_rls.py`

## Operational guidance

- Enforce tenant context in all DB interactions touching tenant-scoped tables.
- Drop direct SQL access for tenants; expose data via the API only, or provision per-tenant database roles/views if necessary.
- Audit/monitor for queries executed without `SET LOCAL app.current_tenant`.

