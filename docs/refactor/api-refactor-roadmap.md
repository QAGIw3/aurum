# Aurum API Refactor Roadmap

## Context Snapshot (Jan 2025)
- v1 FastAPI surface still backed by `routes.py` (2.6k LOC) and `service.py` (2.7k LOC); concerns span routing, caching, validation, IO, persistence.
- v2 surface (`src/aurum/api/v2/*`) follows better conventions (cursor pagination, RFC7807, modular routers) but coexists with v1 in production.
- Shared infrastructure (cache, rate limiting, feature flags, telemetry) lives in globals under `state.py` and is configured via side-effects; testing relies on those globals.
- Recent work introduced a router registry (`router_registry.py`) to decouple app wiring and started extracting PPA utilities into `ppa_utils.py`.

## Guiding Principles
1. **Parity First:** Protect existing contracts (especially v1) until v2 usage reaches agreed thresholds. Use integration/regression tests to guard parity.
2. **Strangler Pattern:** Move functionality into modular packages (v2-style) while leaving compatibility shims that route legacy entry points.
3. **Event-Loop Safety:** Migrate remaining synchronous IO in request paths to async or run in executors; add tests to prevent regressions.
4. **Configuration Clarity:** Replace implicit globals with explicit dependency injection (DI) or context objects passed through routers/services.
5. **Observability Everywhere:** Standardize structured logging, metrics, and tracing hooks before expanding surfaces.

## Phase 0 – Baseline & Tooling (In Flight)
- Finalize router registry to load v1/v2 routers declaratively (ensure no double registration, add env gating tests).
- Record key module ownership docs (module READMEs) outlining responsibilities and dependencies.
- Capture performance and error baselines (p95 latencies, cache hit rate, error budgets) for regression tracking.

## Phase 1 – API Composition & Lifecycle (Target: Feb 2025)
- Replace the `routes.py` megafile with per-domain v1 routers living beside v2 modules (curves, metadata, ISO/EIA, drought, scenarios, PPA).
- Introduce middleware registry/loader mirroring the router registry, making toggling (audit, rate limit, telemetry) configuration-driven.
- Convert `create_app` into a pure factory (no global side-effects) and expose a startup hook that receives resolved dependencies (cache, feature flags, telemetry).
- Decouple admin/observability routers from `routes.py`; move to dedicated packages with explicit auth dependencies.
- Document lifecycle in `docs/refactor/playbook.md` (startup ordering, required env vars, migration strategy).

## Phase 2 – Service Layer Decomposition (Target: Mar–Apr 2025)
- Split `service.py` into domain services matching router packages (curves_service, metadata_service, scenario_service, ppa_service, drought_service).
- Extract data-access into DAO modules (Trino, Redis, Postgres) with typed responses and retry/backoff policies.
- Ensure every domain service exposes async APIs; wrap blocking DB/CSV operations inside `run_in_executor` helpers.
- Align DTOs with Pydantic v2 models, removing ad-hoc dict munging scattered across routes.
- Add contract/unit tests per service module with fixtures defined in `tests/api/test_fixtures.py`.

## Phase 3 – Cross-Cutting Concerns (Target: May 2025)
- Centralize cache configuration: replace `_SimpleCache` globals with cache manager instances injected per request or per domain.
- Standardize error handling using custom exception hierarchy -> RFC7807 payload generator.
- Migrate rate limiting and feature flag initializers to async factories; provide no-op implementations for tests.
- Replace direct env var inspection (`os.getenv`) with `AurumSettings` feature flags for all toggles (v1 split flags, metrics, audit, etc.).
- Introduce unified tracing decorators for high-value endpoints; ensure instrumentation attaches request IDs consistently.

## Phase 4 – Contract Modernization & Cleanup (Target: Jun–Jul 2025)
- Promote v2 routers to default `/latest` path; gate v1 behind explicit `AURUM_API_ENABLE_V1` flag with sunset messaging.
- Backfill OpenAPI metadata: tags, examples, error schemas; auto-generate docs via `OpenAPIGenerator` once per release.
- Remove deprecated v1 helpers (`_scenario_outputs_enabled`, legacy pagination caches) after sunset date.
- Publish migration guide updates and SDK/client adjustments to consume v2 semantics exclusively.

## Testing & Validation Strategy
- Maintain dual-surface integration suite (v1 & v2) until v1 sunset; use `schemathesis` contract fuzzing on both.
- Add router registry unit tests (toggle coverage) and smoke tests verifying middleware ordering.
- For each phase, run load tests against docker-compose stack and compare metrics to Phase 0 baselines.
- Extend CI to run `pytest -m "not slow"` by default and nightly slow suite (Trino/Redis dependent tests).

## Dependencies & Risks
- Requires coordination with ingestion/worker teams when service APIs change (e.g., scenario persistence interfaces).
- Async migrations depend on availability of async clients (Trino, Redis); verify driver compatibility before refactoring.
- Cache revamps must account for golden query cache warm-up scripts and admin UI expectations.
- V1 sunset timeline depends on customer readiness; maintain feature flags for rollback.

## Immediate Next Actions
1. Fix router registry duplication for v1 PPA router and add regression tests.
2. Draft module-level READMEs (starting with `src/aurum/api/README.md`) summarizing new architecture.
3. Schedule pairing sessions with data ingest team to map DAO boundaries and shared schemas.
