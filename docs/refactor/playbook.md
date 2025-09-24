# Aurum Refactor Playbook

This playbook outlines the phased approach to refactoring the Aurum API.

## Phases

1) Baseline & Quick Wins
- Remove duplicated exports in `api/http`.
- Unify health checks: async `/ready`, use `httpx` for external probes, robust status aggregation.
- Avoid `asyncio.run` on request paths for health checks; keep heavy refactors for later phases.

2) Router Modularization
- Extract remaining v1 domains from `api/routes.py` into domain routers behind feature flags.
- Keep small, declarative handlers that call services and shared HTTP utilities.

3) Service & DAO Split
- Create per-domain services with orchestration only.
- Push DB access (Trino/Timescale/ClickHouse) into thin async DAOs.
- Standardize cache access through `AsyncCache/CacheManager`.

4) v2 Hardening
- Enforce v2 pagination utils and RFC7807 errors everywhere.
- Add stable ETag + Link header behavior to all list/export endpoints.

5) Dependencies & Tooling
- Align dependency versions across groups via constraints.
- Strengthen CI with lint/typing gates and contract testing.

6) Cleanup
- Retire monolith router and legacy sync code once parity is validated.

## Validation

- Contract tests (Schemathesis) and response snapshots for v1 vs split routers.
- Perf baselines (p95 latency) on curves/EIA flows before/after migrations.
- Coverage threshold unchanged; grow type coverage gradually.

