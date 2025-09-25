# Phase 1 — PR 2: Trino Pooling Unification

## Summary
Standardize on `TrinoClient` pooled execution, drop duplicate pools in API code, and expose pool/queue metrics.

## Scope
- src/aurum/api/database/trino_client.py
- src/aurum/api/service.py
- src/aurum/performance/connection_pool.py (deprecation for API usage)

## Checklist
- [ ] Ensure `execute_query` is `await`ed; remove `asyncio.run`
- [ ] Record pool utilization and queue depth
- [ ] Remove API imports of `performance/connection_pool`

## Verification
- Unit tests for pool stats and queue timeouts
- Smoke: run a query; check metrics endpoints

## Risks
- Concurrency tuning; keep configuration compatible and conservative

