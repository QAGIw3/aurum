# API Concurrency & Trino Runbook

## When to use this runbook

Reach for this guide when API responses begin returning `429`/`503`, queue wait
times spike, or Trino-backed endpoints slow down. The companion Grafana
dashboards live in `docs/observability/grafana`:

- `api_concurrency_overview.json`
- `trino_pool_health.json`

Import them into Grafana (or use jsonnet tooling) to get the panels referenced
below.

## Symptoms & Triage

| Symptom | Likely Cause | First Checks |
| --- | --- | --- |
| Rising `429 TooManyRequests` with short `Retry-After` | Tenant queue saturated | Review **Tenant Queue Depth** panel; look for single tenant pinned at high depth |
| Rising `503 queue_timeout` and long waits | Global capacity exhausted or Trino blocked | Inspect **Global Queue Depth**; correlate with **Trino Pool Utilization** |
| Requests timing out with `504` and `X-Timeout-Seconds` header | Downstream work not finishing before request deadline | Cross-check **Queue Wait p95** vs application latency in APM; consider increasing `AURUM_API_REQUEST_TIMEOUT_SECONDS` if justified |
| Trino dashboards show utilization >= 0.85 and breaker state > 0 | Connection pool or Trino cluster saturated | See **Pool Utilization**, **Connection Acquire p95**, **Circuit Breaker State** panels |

## Diagnostics

1. **Identify the tenant** – `X-Tenant-ID` header (FastAPI dependency) and
   middleware logs include tenant IDs. The rejection payload also includes it.
2. **Verify queue depth** using the `aurum_api_global_queue_depth` gauge and per
   tenant `aurum_api_tenant_queue_depth` metric. Persistent non-zero values mean
   the queue is never draining.
3. **Inspect queue wait distribution** via
   `aurum_api_tenant_queue_wait_seconds_bucket`. Long p95 waits with low depth
   usually indicate Trino IO rather than queue backpressure.
4. **Compare with Trino health** in the Trino dashboard. High utilization,
   longer connection acquire p95, or an open breaker mean the database is the
   bottleneck.

## Mitigation Steps

### Tenant-level tuning

Per-tenant caps and queue sizes now come from settings/env:

- `AURUM_API_CONCURRENCY_MAX_CONCURRENT_REQUESTS`
- `AURUM_API_CONCURRENCY_MAX_REQUESTS_PER_TENANT`
- `AURUM_API_CONCURRENCY_TENANT_QUEUE_LIMIT`
- `AURUM_API_CONCURRENCY_QUEUE_TIMEOUT_SECONDS`
- Slow-start knobs: `AURUM_API_CONCURRENCY_SLOW_START_*`
- Burst refill: `AURUM_API_CONCURRENCY_BURST_REFILL_PER_SECOND`

Use `AURUM_API_CONCURRENCY_TENANT_OVERRIDES` (JSON map) for targeted tweaks, e.g.:

```bash
export AURUM_API_CONCURRENCY_TENANT_OVERRIDES='{"tenant-a": {"tenant_queue_limit": 12, "slow_start_initial_limit": 4}}'
```

Redeploy (or update runtime config if wired) to apply.

### Short-term throttling

For runaway tenants causing queue starvation, temporarily reduce their queue or
slow-start cap via the override above and notify the owner. `Retry-After` plus
`X-Queue-Depth` headers now show the queue length that triggered the `429`/`503`.

### Trino pool relief

If dashboards show pool utilization > 0.85, take one or more actions:

1. Scale Trino workers or bump `trino_connection_pool_max_size` in service
   configuration.
2. Verify circuit breaker state via `aurum_trino_circuit_breaker_state`. If it is
   open, expect retries to short-circuit; resolve upstream outage before raising
   pool sizes.
3. Reduce query concurrency by tightening API limits (above) or by adding caching.

### Timeout adjustments

- For long but valid operations, increase `AURUM_API_REQUEST_TIMEOUT_SECONDS`
  and matching tenant queue timeout. Remember to update client expectations.
- If `Queue Wait p95` is <1s but `Trino Connection Acquire p95` is high, the fix
  is almost certainly in the database tier.

## Validation & Follow-up

- Ensure queue depth returns to oscillating around zero and rejection counters
  stabilise.
- Keep overrides in source control using the new settings wiring; avoid ad-hoc
  patching.
- When the incident is resolved, document the change in the runbook or service
  README and back out temporary overrides if they are no longer required.
