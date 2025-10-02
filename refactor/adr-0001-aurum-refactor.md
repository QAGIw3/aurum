# ADR-0001: Incremental Refactor of Aurum API

Status: Accepted

Date: 2025-09-24

## Context

The Aurum codebase has grown a large monolithic v1 router (`src/aurum/api/routes.py`) and a dense service layer (`src/aurum/api/service.py`). This leads to:

- Mixed concerns (routing, validation, business, infra) in single modules
- Mixed sync/async I/O on request paths (e.g., `asyncio.run`)
- Duplication across health checks and HTTP utilities
- Dependency version drift between base and optional groups

There is a v2 surface (under `src/aurum/api/v2/*`) with better patterns (cursor-only pagination, RFC7807 errors, ETags), but v1 remains the default.

## Decision

Refactor incrementally with a strangler pattern:

1. Stabilize shared utilities (HTTP, pagination, error handling) and remove obvious duplication.
2. Consolidate health checks and convert heavy readiness checks to async.
3. Split per-domain services (curves, metadata, EIA, ISO, PPA, drought) out of the monolithic service file.
4. Standardize on v2 conventions for new and moved endpoints; freeze v1.
5. Align dependencies using a single constraints source.

## Rationale

Incremental changes avoid large regressions and make it possible to validate parity domain-by-domain. Async correctness on hot paths reduces tail latency and prevents event-loop misuse.

## Consequences

- Short term: new utility modules and docs; updates to health endpoints and small behavior improvements (robust readiness status aggregation).
- Medium term: domain services/DAOs become clearer to test and evolve; v1 remains available with deprecation headers.
- Long term: monolith router can be retired; simpler performance tuning and observability across modular code.

## Implementation Notes (initial changes)

- Deduplicated `src/aurum/api/http/__init__.py` exports.
- Converted `/ready` to async, removed `asyncio.run` in readiness checks, and used `httpx.AsyncClient` for schema registry checks.
- Made readiness aggregation robust (treat `healthy`/`disabled` as OK; not truthiness of dicts).
- Fixed `src/aurum/api/health.py` to use the routes’ Trino readiness helper correctly and asynchronously.

## Rollout & Validation

- Contract tests via `schemathesis` against `/openapi.json`.
- Smoke readiness checks under load and verify no event-loop warnings.
- Track p95 on hot endpoints during later phases before/after async migrations.

