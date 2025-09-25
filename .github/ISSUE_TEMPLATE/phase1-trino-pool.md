---
name: "Phase 1 – Trino Pooling Unification (PR 2)"
about: Standardize on Trino client pool; instrument; remove duplicates
labels: ["phase:1", "area:db", "type:refactor", "reliability", "perf", "priority:P0"]
---

## Summary
Route all Trino queries through `TrinoClient` async API, expose pool stats, and eliminate duplicate pooling usage in API.

## Scope
- src/aurum/api/database/trino_client.py
- src/aurum/api/service.py
- src/aurum/performance/connection_pool.py (deprecate for API)

## Tasks
- [ ] Ensure `TrinoClient.execute_query` is awaited (no `asyncio.run`)
- [ ] Instrument pool stats and expose via metrics
- [ ] Remove API dependencies on `performance/connection_pool`

## Acceptance Criteria
- [ ] Single pool implementation used by API
- [ ] Pool utilization/queue depth metrics visible
- [ ] No regression in error handling/retries

## Notes
Keep behavior; add metrics and remove dead paths.

