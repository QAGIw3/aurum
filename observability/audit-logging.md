# Audit Logging

This document summarizes Aurum’s audit, compliance, and security logging for external data and admin operations.

## Overview

- Every API request receives a correlation ID (header `x-correlation-id`), generated if missing, and echoed in responses.
- Audit events include user ID (when available), tenant context, resource, operation, status, and details.

## Loggers and Files

- `audit.external_data`: API calls and data access (info/warn)
- `compliance.external_data`: compliance‑focused events (info)
- `security.external_data`: security events and violations (warning)

Set `AURUM_AUDIT_LOG_DIR` to a writable directory (e.g., `/var/log/aurum`). If not writable, logging falls back to stdout.

## Event Types

- api_call: method, path, status, request/response metadata
- data_access: provider/dataset resource, record counts, operation (read/write)
- admin_action: runtime config, mapping changes, feature flag updates
- security_event: isolation violations, privilege escalation attempts, classification access audit

## Runtime Configuration Changes

Admin endpoints under `/v1/admin/config/*` record audit entries for:
- Rate limit updates: tenant, path, before/after values
- Feature flag updates: tenant, feature, before/after configuration

See docs/runtime-config.md for details and examples.

## Best Practices

- Forward the three logger streams to your SIEM with appropriate retention.
- Alert on high‑volume changes to runtime config and on security events.
- Include correlation IDs in user support tickets and dashboards to join API and backend logs.

