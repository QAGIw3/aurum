# Aurum Refactor Plan

## Purpose
Establish a shared roadmap for untangling the Aurum API/ingestion monolith into modular, testable components while preserving service availability. The plan balances short-term hygiene with staged architectural change so we can iterate safely and avoid big-bang rewrites.

## Guiding Principles
- Ship incremental, reversible changes guarded by feature flags where possible.
- Preserve API contracts (especially `/v1` until sunset) and behavior while refactoring internals.
- Prefer composition over global state; use explicit dependency injection and lifespan hooks.
- Keep observability (metrics, logs, tracing) first-class during every change.
- Require tests and documentation updates for each phase.

## Phases

### Phase 0 – Foundations (In Flight)
**Objectives**
- Unify diverging dependency pins across `pyproject.toml` to avoid resolver conflicts.
- Enable stricter linting/typing defaults (ruff, mypy) and clear quick hygiene wins.
- Record architecture intent via ADR and refactor plan.

**Deliverables**
- Dependency alignment MR with changelog.
- ADR: `docs/adr/000x-architecture-boundaries.md`.
- This plan committed under `docs/refactor-plan.md`.

**Exit Criteria**
- `poetry lock`/`pip-compile` converges without manual overrides.
- Static analysis runs clean with tightened config.
- Team reviews and signs off on the ADR + plan.

### Phase 1 – API Surface & Versioning (Queued)
**Objectives**
- Finish extracting `/v1` endpoints from `src/aurum/api/routes.py` into domain-specific routers (`src/aurum/api/v1/*`).
- Introduce router registry mapping for both versions; expose `configure_routes(settings)` without global mutations.
- Keep `/v1` behind a feature flag and emit enhanced deprecation headers.

**Deliverables**
- Router registry module with tests.
- Updated FastAPI app factory wiring v1/v2 via registry.
- Docs update (migration guide + changelog entry).

**Exit Criteria**
- `routes.py` only contains shared middleware/utilities, no endpoint definitions.
- `create_app` registers routers through registry + feature flags.
- Schemathesis & k6 smoke runs green across both versions.

### Phase 2 – Services & Data Access Separation
**Objectives**
- Split business logic from transport layer: routers call service interfaces, services depend on adapters/DAOs.
- Formalize adapter contracts for Trino/Timescale/Redis (leveraging `src/aurum/api/database/*`).
- Introduce domain-level error types mapped to RFC7807 handler.

**Deliverables**
- `services/` package per domain with interfaces + implementations.
- DAO layer with unit tests (sync + async fakes).
- Updated container/lifespan wiring for DI.

**Exit Criteria**
- Routers contain no direct DB/cache calls.
- Coverage for services/adapters ≥ 85%.
- RFC7807 responses observed in integration smoke tests.

### Phase 3 – Caching & Rate Limiting Unification
**Objectives**
- Consolidate redundant cache implementations into unified interface with governance (TTL, keys, metrics).
- Merge rate limiting modules into single policy engine + admin router.
- Document cache/ratelimit behavior and metrics.

**Deliverables**
- Unified cache module with configuration schema + tests.
- Single rate limiting package + fixtures.
- Observability dashboards updated for cache/ratelimit metrics.

**Exit Criteria**
- Only one cache manager entry point used by API.
- Rate limit middleware + admin endpoints sourced from unified package.
- Prometheus metrics verified via integration tests.

### Phase 4 – Application Lifecycle & State
**Objectives**
- Remove module-level singletons (`state.py`, global settings) in favour of lifespan context + DI.
- Centralize startup/shutdown logic for Trino pools, Redis, warehouse maintenance.
- Harden optional dependency handling with clear fallbacks.

**Deliverables**
- Lifespan manager module with tests.
- Updated app factory using dependency overrides for tests.
- Docs covering new lifecycle hooks.

**Exit Criteria**
- No reliance on global `_SETTINGS`; tests use explicit fixtures.
- Startup/shutdown idempotent and observable via metrics/logs.
- Chaos testing validates graceful degradation when optional deps missing.

### Phase 5 – Observability & SLOs
**Objectives**
- Introduce minimal façade over metrics/tracing/logging API to reduce coupling.
- Define and monitor core SLOs (latency, error rate, cache hit rate).
- Ensure structured logging and request ID propagation are consistent.

**Deliverables**
- Observability façade module + tests.
- SLO definitions + automated checks (k6 dashboards, alerts).
- Runbook updates.

**Exit Criteria**
- Services depend on façade rather than deep observability modules.
- Alerting rules rolled out; dry-run dashboards validated.
- Access logs + traces verified end-to-end in staging.

### Phase 6 – Scenarios & Worker Streamlining
**Objectives**
- Unify scenario services (`scenario_service.py` and `_hardened`) behind configuration-driven policies.
- Extract side-effects (Kafka, Iceberg) into strategy interfaces with retries/outbox pattern.
- Reuse shared service abstractions from Phases 2–3.

**Deliverables**
- Scenario domain package with cohesive services/adapters.
- Worker wiring aligned with new abstractions.
- Extended integration tests & fixtures.

**Exit Criteria**
- Scenario API and worker share codepaths via services.
- Outbox/retry semantics covered by tests + docs.
- Operational metrics demonstrate parity with prior behaviour.

### Phase 7 – Packaging, CLI & Documentation
**Objectives**
- Slim `src/aurum/api/__init__.py`; rely on explicit imports + lazy loading.
- Align CLI entrypoints under unified `aurum` command tree.
- Automate docs generation from running app (OpenAPI + Spectral checks).

**Deliverables**
- Lightweight API package initializer with lazy exports.
- CLI module structure + docs.
- CI pipeline verifying docs drift + lint.

**Exit Criteria**
- Importing `aurum.api` triggers no heavy side effects (validated in tests).
- CLI smoke tests pass (`aurum --help`).
- Docs pipeline green and diff-free per release.

### Phase 8 – CI/Test Strategy
**Objectives**
- Expand unit/integration/perf coverage; enforce module-level coverage budgets.
- Add contract tests for DAOs and Schemathesis regression suite.
- Integrate performance regression alerting (k6 + Prometheus).

**Deliverables**
- Test matrix documentation and tooling scripts.
- CI enhancements (parallel jobs, caching, fail-fast settings).
- Performance baselines stored for comparison.

**Exit Criteria**
- CI runs within agreed SLA with failing gates for coverage/perf regression.
- On-call playbooks reference new test tooling.
- Release checklist updated.

## Milestones & Tracking
- Track phases as GitHub Projects columns with acceptance criteria checklists.
- Tag issues `refactor-phase-X` for filtering.
- Target cadence: 2–3 week iterations per phase, overlapping where safe.

## Risks & Mitigations
- **API regressions**: Mitigate via parallel v1/v2 testing, Schemathesis, canary scripts.
- **Scope creep**: Guard with ADR sign-off and per-phase acceptance criteria.
- **Optional dependency drift**: Add health checks + fallback code paths in lifecycle phase.
- **Team bandwidth**: Rotate ownership per domain to avoid burnout; keep changes small.

## Immediate Backlog
1. Land dependency alignment (Phase 0).
2. Finish v1 router extraction + registry (Phase 1).
3. Slim `aurum.api.__init__` with lazy exports (Phase 7 dependency but started now for quick win).
4. Author ADR for architecture boundaries.
5. Add automation to diff OpenAPI spec (`make docs-openapi-check-drift` in CI).

