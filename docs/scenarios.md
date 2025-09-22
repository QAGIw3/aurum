# Scenarios Guide

This guide explains how to define scenarios, run them asynchronously, retrieve outputs and metrics, and operate the pipeline end to end.

- Audience: API consumers, data engineers, platform operators
- Surfaces: REST API, Kafka topics, Iceberg tables, Postgres metadata, Timescale ops metrics

## Concepts

- Scenario: Named definition with assumptions, parameters, tags, and version.
- Run: Execution of a scenario with options (code version, priority), tracked by status (QUEUED → RUNNING → SUCCEEDED/FAILED/CANCELLED).
- Output: Time‑series and metric values produced by runs, persisted to Iceberg (`iceberg.market.scenario_output`) and exposed through API.

## Authentication & Tenancy

- Auth: OIDC/JWT (configurable). Requests must include a bearer token unless `AURUM_API_AUTH_DISABLED=1`.
- Tenant scoping: All scenario CRUD and reads are tenant‑scoped. The API enforces RBAC (see `Permission.SCENARIOS_*`).

## Endpoints

- List scenarios: `GET /v1/scenarios?limit=20&status=active`
  - Filters: `name` (substring match), `tag`, `created_after`, `created_before`
- Create scenario: `POST /v1/scenarios`
- Get / Delete: `GET|DELETE /v1/scenarios/{id}`
- Trigger run: `POST /v1/scenarios/{id}/run` (202 Accepted)
- List runs: `GET /v1/scenarios/{id}/runs?limit=20`
  - Filters: `state`, `created_after`, `created_before`
- Get run: `GET /v1/scenarios/{id}/runs/{run_id}`
- Cancel run: `POST /v1/scenarios/runs/{run_id}/cancel`
- Update run state (internal/admin): `POST /v1/scenarios/runs/{run_id}/state`
- List outputs: `GET /v1/scenarios/{id}/outputs?limit=100&format=json|csv`
- Latest metrics: `GET /v1/scenarios/{id}/metrics/latest`
- Bulk runs: `POST /v1/scenarios/{id}/runs:bulk`

OpenAPI contract with schemas and examples: `docs/api/openapi-spec.yaml`.

## Examples

Create a scenario:

```bash
curl -s -X POST http://localhost:8095/v1/scenarios \
  -H 'Content-Type: application/json' \
  -d '{
        "tenant_id": "demo-tenant",
        "name": "RPS sensitivity",
        "assumptions": [
          {"driver_type": "policy", "payload": {"policy_name": "RPS", "start_year": 2030, "target": 0.6}}
        ],
        "parameters": {"shock_bp": 25},
        "tags": ["demo", "sensitivity"]
      }'
```

Trigger a run:

```bash
curl -s -X POST http://localhost:8095/v1/scenarios/{scenario_id}/run \
  -H 'Content-Type: application/json' \
  -d '{"code_version": "v1", "priority": "NORMAL"}'
```

Fetch outputs (JSON):

```bash
curl -s "http://localhost:8095/v1/scenarios/{scenario_id}/outputs?limit=100&metric_name=NPV"
```

CSV streaming (set `format=csv`):

```bash
curl -s "http://localhost:8095/v1/scenarios/{scenario_id}/outputs?format=csv" -o outputs.csv
```

Latest metrics rollup:

```bash
curl -s "http://localhost:8095/v1/scenarios/{scenario_id}/metrics/latest"
```

## Caching and invalidation

- Scenario output queries are cached per-tenant with Redis (if configured). Cache keys incorporate a tenant-level version that is bumped whenever new outputs are written—this prevents stale reads without needing to flush entire namespaces.
- Concurrent cache misses use a singleflight guard so only one query hits Trino at a time for a given key.
- Cache hit/miss metrics are exported via Prometheus (`aurum_scenario_cache_*`).
- Worker writes and API mutations publish Kafka invalidation events when `AURUM_SCENARIO_CACHE_INVALIDATION_TOPIC` is set, allowing external consumers to invalidate their own caches.

## Benchmarking

The repository ships with a lightweight benchmarking harness that exercises the output query path:

```bash
python scripts/bench/scenario_benchmark.py \\
  --tenant-id TENANT_UUID \\
  --scenario-id SCENARIO_UUID \\
  --iterations 10 --limit 200 --use-cache
```

The script relies on `AurumSettings` environment variables for Trino/Redis connection details and prints latency/throughput summaries for quick regression checks.

## Async Flow

1. API validates and enqueues a run (202) with `ScenarioRunOptions`.
2. Worker consumes run requests from Kafka, executes model, writes outputs to Iceberg (`iceberg.market.scenario_output`) and updates run status in Postgres.
3. API `/outputs` reads from Iceberg via Trino; `/metrics/latest` aggregates by metric.

Idempotency: Repeated `run` requests with identical `scenario_id` and options may be de‑duplicated by the worker; cancellation is idempotent.

## Worker & K8s

Local (docker compose):

```
COMPOSE_PROFILES=core,worker docker compose -f compose/docker-compose.dev.yml up -d scenario-worker
```

Environment:
- `AURUM_APP_DB_DSN` (optional): Postgres DSN for scenario metadata. If unset, the API/worker fall back to `AURUM_TIMESCALE_DSN`.
- `AURUM_SCENARIO_METRICS_PORT` (default 9500): Prometheus metrics port.
- `AURUM_SCENARIO_METRICS_ADDR` (optional): Bind address for metrics.

Kubernetes:
- Reference deployment: k8s/scenario-worker/deployment.yaml
- Ensure the pod env provides a Postgres/Timescale DSN and any required feature flags.
- Expose metrics via a Service/ServiceMonitor targeting the configured port.

Operational tips:
- The worker publishes outputs into Iceberg (via Trino/Spark) or Postgres depending on your deployment; API output endpoints read from Iceberg by default.
- Use the runtime config API (docs/runtime-config.md) to toggle feature flags and tune rate limits if needed.

## Storage & Models

- Postgres operational tables (see `postgres/ddl/app.sql`): `scenario`, `scenario_run`, `scenario_event`.
- Iceberg (Trino DDL in `trino/ddl/iceberg_market.sql`): `iceberg.market.scenario_output` partitioned by `scenario_id`, `metric`, and `days(asof_date)`.
- dbt marts: use `mart_scenario_output` views/tables where present; ensure tests for uniqueness and not‑null.

## Feature Flags & Limits

- Enable outputs: `AURUM_API_SCENARIO_OUTPUTS_ENABLED=1` or set feature on via `ScenarioOutputFeature`.
- Output limit enforcement and validation are applied server‑side; 400 is returned on invalid filters.
- Rate limits apply (`X-RateLimit-*` headers); 429 conveys `Retry-After`.

## Errors

- 400 – validation error (missing/invalid fields)
- 401/403 – unauthorized/forbidden
- 404 – unknown scenario or run
- 409 – conflict (e.g., duplicate name, concurrent mutation)
- 500 – internal error

Responses include `meta.request_id` for support.

## Observability

- API logs structured events (`trino_query_metrics`, `scenario_*`) with request/tenant context; scrape Prometheus metrics at `/metrics`.
- Ops metrics table (`timescale.public.ops_metrics`) can store query p95s via `make trino-harness-metrics`.

## Operations

- Cancel a stuck run: `POST /v1/scenarios/runs/{run_id}/cancel`
- Clean up test scenarios: `DELETE /v1/scenarios/{id}` (also deletes runs)
- Backfill outputs to Iceberg with a one‑off worker job; keep dbt marts current.

## Versioning

- API follows semantic versioning through `docs/api/openapi-spec.yaml` (`info.version`).
- Scenario schema evolution is additive where possible; breaking changes require a version bump and migration notes.
