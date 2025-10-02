Codebase Map (Aurum)

Overview
- API: FastAPI application exposing curve, scenario, external data endpoints.
- Workers: Long‑running processes for scenario execution and ingestion helpers.
- Observability: Tracing/metrics/logging utilities and middleware.
- Ingestion: Airflow DAGs, SeaTunnel templates, and external collectors.
- Data access: DAO/clients for Trino/Timescale/Redis and schema registry.

Key Directories
- src/aurum/api: HTTP models, routes, auth, caching, pagination, and services.
- src/aurum/scenarios: Worker and models for scenario execution.
- src/aurum/observability: Tracing and metrics helpers.
- src/aurum/airflow_utils + airflow_factory: Utilities/builders for DAGs.
- src/aurum/seatunnel: Template rendering + linter for SeaTunnel jobs.
- src/aurum/reference: Static reference data (calendars, units, ISO nodes).
- src/aurum/external: Collectors and runners for EIA/FRED/NOAA/WorldBank.
- src/aurum/performance: Orchestrator, caching, batching, connection pooling.
- src/aurum/data: External DAO and backend abstractions.

High‑Traffic Modules
- api/routes.py: Defines most endpoints; uses service + cache + pagination.
  Notes: ETag helpers, request/tenant resolution, Redis caching guards, and
  Prometheus metrics integration. Prefer extending via small handler functions
  and shared helpers for stability.
- api/scenario_service.py: Storage backends for scenarios and runs.
  Notes: InMemory store for tests; Postgres store uses app.current_tenant and
  reuses existing runs by version hash for idempotency.
- observability/tracing.py: Span/trace context with optional OpenTelemetry.
  Notes: Safe to call without OTEL installed; spans record attributes/tags.

Ingestion / Orchestration
- airflow/dags/*: TaskFlow/PythonOperator DAGs for ISO/MISO helpers.
- seatunnel/jobs/templates/*: Rendered into executable jobs; linter validates
  placeholders and documentation. See src/aurum/seatunnel/linter.py.
- external/runner.py: Command‑line utility to run collectors; emits to Kafka.

Testing
- tests/: Unit/integration tests for API pagination, cache, providers, etc.
  Patterns: direct module load via importlib to avoid heavy app init; TestClient
  for FastAPI routes; monkeypatch for optional dependencies.

Extending the API
- Add models to src/aurum/api/models.py; reuse AurumBaseModel.
- Add service methods (pure functions or store interactions) in scenario_service
  or data/external_dao as needed.
- Add routes in api/routes.py. Prefer small functions and shared validation
  helpers; use _resolve_tenant and hardened cursor utilities.

Observability Conventions
- Use aurum.telemetry.context.get_request_id for correlation.
- Use trace_span()/get_tracer() for spans; metrics via aurum.observability.metrics.

Caching Guidelines
- Prefer AsyncCache/CacheManager for cross‑process; _SimpleCache for hot, local
  metadata paths. Respect TTLs in settings.api.cache.*.

Security/Auth
- AuthMiddleware supports OIDC JWT or forward‑auth headers. Tenant is inferred
  from claims/email domain/groups when not explicitly provided.

Notes
- Many optional deps: Prometheus, OpenTelemetry, Redis, psycopg, requests.
- All SQL queries use parameter binding; Postgres store scopes by tenant.

