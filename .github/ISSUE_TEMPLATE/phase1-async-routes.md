---
name: "Phase 1 – Async API + DI (PR 1)"
about: Convert hot endpoints to async and inject dependencies
labels: ["phase:1", "area:api", "type:refactor", "perf", "priority:P0"]
---

## Summary
Convert v2 hot endpoints to `async def`, remove `asyncio.run` bridges, and inject `settings`/`cache_manager`/services via dependencies. Maintain behavior and OpenAPI.

## Scope
- src/aurum/api/v2/curves.py
- src/aurum/api/v2/metadata.py
- src/aurum/api/v2/eia.py
- src/aurum/api/app.py (dependency wiring)
- src/aurum/api/deps.py (new) — dependency helpers

## Tasks
- [ ] Add dependency helpers (settings, cache_manager, principal)
- [ ] Convert handlers to `async def`
- [ ] Replace global settings with DI
- [ ] Use CacheManager from DI in handlers
- [ ] Ensure ETag/pagination headers remain identical

## Acceptance Criteria
- [ ] No direct blocking I/O in handlers
- [ ] Tests pass; behavior unchanged
- [ ] p95 latency not worse than baseline in smoke test

## Notes
Non-functional change; no API contract changes expected.

