# Phase 1 — PR 1: Async API conversion + DI

## Summary
Convert hot endpoints to `async def`, inject settings/cache/service via dependencies, and preserve behavior (OpenAPI, headers, pagination, ETag).

## Scope
- src/aurum/api/v2/curves.py
- src/aurum/api/v2/metadata.py
- src/aurum/api/v2/eia.py
- src/aurum/api/app.py (wire deps)
- src/aurum/api/deps.py (new)

## Checklist
- [ ] Add `deps.py` (settings, cache_manager, principal, tenant)
- [ ] Switch handlers to `async def`
- [ ] Replace global settings accessor with DI
- [ ] Use CacheManager from request/app.state
- [ ] Tests updated; behavior unchanged

## Verification
- curl examples from README return identical payloads/headers
- schemathesis/contract tests green
- p95 not worse in smoke tests

## Risks
- Incorrect DI wiring; mitigate with targeted tests
- Latency regressions; mitigate with simple bench run

