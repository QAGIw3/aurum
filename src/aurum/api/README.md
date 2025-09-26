# Aurum API Module Guide

This package hosts the FastAPI surface for Aurum as well as the supporting infrastructure (caching, rate limiting, telemetry, feature flags). The codebase is in the middle of an incremental refactor from a v1 monolith to modular v2 routers; this guide documents the current layout so contributors can navigate the transition safely.

## High-Level Architecture

- **Application factory** (`app.py`)
  - Builds the FastAPI app, wires middleware, and includes routers through the registry.
  - Exposes legacy shims so callers that imported symbols from `routes.py` continue to work.
- **Router registry** (`router_registry.py`)
  - Declares which routers belong to v1 and v2 and loads them dynamically.
  - Handles feature-flag toggles for split v1 domains and prevents double registration.
- **Global configuration/state**
  - `state.py` initialises settings and shared singletons (cache manager, feature flags, idempotency manager).
  - `config.py` converts `AurumSettings` into specialised configs (cache, Trino, rate limiting).
- **Domain surfaces**
  - v1: legacy handlers still concentrated in `routes.py`; new extractions live under `v1/` (e.g. `v1/ppa.py`).
  - v2: modular routers under `v2/`, each paired with service/DAO helpers (e.g. `v2/curves.py`, `curves_v2_service.py`).
- **Shared infrastructure**
  - `http/` for reusable response/pagination helpers, `exceptions.py` for RFC7807 errors, `rate_limiting/`, `features/`, etc.

## Directory Tour

| Area | Key Modules | Responsibilities |
| ---- | ----------- | ---------------- |
| Application lifecycle | `app.py`, `router_registry.py`, `state.py` | Build the app, include routers, configure middleware, manage shared state. |
| Legacy surface | `routes.py`, `service.py` | Remaining v1 handlers and imperative service layer (scheduled for decomposition). |
| v1 split routers | `v1/*.py` | Incrementally extracted v1 endpoints, gated by `AURUM_API_V1_SPLIT_*` flags. |
| v2 surface | `v2/*.py`, `*_v2_service.py`, `*_v2_dao.py` | Modern API contract with cursor pagination, async I/O, typed models. |
| Cross-cutting | `cache/`, `features/`, `rate_limiting/`, `performance/`, `idempotency.py` | Infrastructure concerns shared across routers. |
| Observability | `telemetry/` (root package), `observability/metrics.py`, middleware declared in `routes.py` | Structured logging, tracing, metrics wiring. |

## Working With Routers

1. **Fetch settings** – call `AurumSettings.from_env()` or reuse the instance stored on `app.state.settings`.
2. **Configure globals** – `configure_routes(settings)` in `routes.py` initialises cache TTLs, admin group membership, and metrics toggles.
3. **Register routers** – use `get_v1_router_specs(settings)` / `get_v2_router_specs(settings)` to discover routers to include; the application factory already does this.
4. **Add a new router**
   - Place the implementation under `src/aurum/api/v2/your_domain.py` (or `v1/` if backporting).
   - Export an `APIRouter` named `router`.
   - Update `router_registry.py` with the module path. Use environment flags for optional v1 extractions.
   - Write targeted tests in `tests/api/` covering both contract and feature-flag behaviour.

## Lifecycle Hooks

- `create_app(settings)` – entry point for building a FastAPI instance. It handles middleware and registers lifecycle events (startup/shutdown) for cache, feature flags, maintenance coordinators, etc.
- `configure_state(settings)` – prepares global singletons used across handlers.
- Middleware order is manually controlled inside `app.py`; keep new middleware additions here to maintain a predictable stack.

## Testing Guidance

- Unit / module tests: `pytest` under `tests/api/` (e.g. `tests/api/test_router_registry.py`).
- Integration tests: Compose-based environments exercise real dependencies (Trino, Redis, etc.). Use `docker-compose.sandbox.yml` for local parity.
- When adding v1 split routers or refactoring `routes.py`, run both v1 and v2 suites to ensure parity (`tests/api/test_scenarios_api.py`, `tests/api/test_v2_*`).
- The repository currently has no test runner wrappers; use `python -m pytest` once dependencies are installed.

## Ongoing Refactor Checklist

- [x] Router registry abstraction in place and covered by unit tests.
- [x] Extract remaining v1 domains (curves, metadata, ISO, drought, admin) into dedicated modules.
- [ ] Decompose `service.py` into per-domain services/DAOs (tracked in `docs/refactor/api-refactor-roadmap.md`).
- [ ] Introduce middleware registry to complement router registry.
- [ ] Replace implicit globals with dependency-injected collaborators.

See `docs/refactor/api-refactor-roadmap.md` for the phased plan and `docs/refactor/playbook.md` for rollout tactics.
