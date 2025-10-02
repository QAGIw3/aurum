# Trino On-Call Runbook

## Overview
Procedures for incidents affecting Trino availability, query latency, and pool saturation.

## Contacts
- Primary On-call: oncall-trino@aurum.com
- Slack: #oncall-ops
- PagerDuty Service: Trino

## Key Signals & Paging
- Coordinator unavailable, 5xx from `/v1/info` (P0)
- Query queue depth > 75% for 5+ minutes (P1)
- Worker failures > 10% or continuous rescheduling (P1)
- p95 query latency outside SLO for critical dashboards/jobs (P1)

## First 5 Minutes
1. Ack and declare IC; set next update time
2. Scope: which catalogs, tenants, or workloads affected (interactive vs batch)
3. Stabilize: cancel runaway queries; cap concurrency; scale workers

```bash
# Pods and component health
kubectl -n aurum-dev get po -l app.kubernetes.io/name=trino
curl -s http://trino.aurum-dev.svc.cluster.local:8080/v1/info | jq .

# Query inventory and states
trino --execute "SELECT state, count(*) FROM system.runtime.queries GROUP BY 1 ORDER BY 1" || true
trino --execute "SELECT query_id, state, user, source, queued_time, elapsed_time, cpu_time FROM system.runtime.queries ORDER BY cpu_time DESC LIMIT 20" || true

# Cancel a problematic query (replace <id>)
trino --execute "CALL system.runtime.kill_query(query_id => '<id>', message => 'on-call mitigation')" || true
# Alternatively (HTTP):
curl -s -X DELETE http://trino.aurum-dev.svc.cluster.local:8080/v1/query/<id>

# Pool and memory signals (via metrics endpoint if enabled)
curl -s http://trino.aurum-dev.svc.cluster.local:8080/v1/metrics | head -50
```

## Dashboards & Signals
- Grafana: Trino Pool Health (`docs/observability/grafana/trino_pool_health.json`)
- Queue depth, queued time, split backlog, worker CPU/memory
- Catalog errors (Iceberg/Timescale), transaction/commit failures

## Common Failure Modes
- Hot queries scanning large tables; insufficient stats → queue saturation
- Spill to disk pressure; memory overcommit → worker OOM/restarts
- Catalog connectivity (Iceberg/MinIO/Timescale) causing timeouts

## Remediation Actions
- Cancel/top offenders; add/refresh table stats where applicable
```bash
# Example: analyze to refresh stats (adjust for catalog/schema)
trino --execute "ANALYZE iceberg.market.curve_observation"
```
- Reduce concurrency and queue pressure
```bash
# Runtime config via ConfigMap or env; temporary scale workers
kubectl -n aurum-dev scale deploy trino-worker --replicas=8
```
- Route heavy reads to cached endpoints or ClickHouse where available
- Increase query max memory per node cautiously; verify spill configuration
- Verify Iceberg maintenance (compaction, vacuum) is healthy; run maintenance if needed

## Escalation
- If unable to restore service SLO in 30 minutes, escalate per paging policy
- Engage data platform owners for dataset-specific tuning

## Verification & Closure
- Queue depth < 25% sustained; p95 latency back to baseline for 30m
- Worker fleet stable; no continuous restarts
- Postmortem for P0/P1 within 24–48h
