# API On-Call Runbook

## Overview
Procedures for responding to API incidents affecting availability, latency, and correctness.

## Contacts
- Primary On-call: oncall-api@aurum.com
- Slack War Room: #oncall-ops
- PagerDuty Service: API

## Key SLOs & Paging
- Availability ≥ 99.9%
- p95 latency ≤ 1000ms (public), ≤ 500ms (internal)
- Error rate (5xx) ≤ 1% sustained 5 minutes
- Burn-rate multi-window (2h/6h) per paging policy

## First 5 Minutes
1. Acknowledge page; declare IC and set next update time (15m for P0, 30m for P1)
2. Verify blast radius: check status page, synthetic checks, tenant scope
3. Stabilize: scale up temporarily, enable graceful-degradation, reduce expensive endpoints

```bash
# Pods and health
kubectl -n aurum-dev get deploy,po,ingress -l app.kubernetes.io/name=aurum-api
kubectl -n aurum-dev logs deploy/aurum-api --tail=200 | grep -i "error\|timeout\|circuit"

# Health & metrics
curl -s http://aurum-api.aurum-dev.svc.cluster.local:8080/health
curl -s http://aurum-api.aurum-dev.svc.cluster.local:8080/metrics | head -50

# Scale temporarily
kubectl -n aurum-dev scale deploy aurum-api --replicas=6
```

## Dashboards & Signals
- Grafana: API Concurrency Overview (`docs/observability/grafana/api_concurrency_overview.json`)
- Golden Signals: availability, latency p95/p99, error rate, saturation
- Downstream: DB connection pool, Trino pool queue depth, Kafka producer acks

## Common Failure Modes
- Hot endpoints causing CPU/memory pressure → enable offload strategies; cache busts
- Database connection pool exhaustion → reduce pool size per pod, limit concurrency, retry with backoff
- External dependency slowness → open circuit breakers, increase timeouts, degrade gracefully
- Rate limiting misconfig → adjust runtime config to restore headroom

```bash
# Runtime config toggles
kubectl -n aurum-dev get configmap aurum-api-config -o yaml | sed -n '1,200p'

# Feature flags / degradation toggles (see docs/runtime-config.md)
# Example: reduce per-request fanout or disable heavy joins
```

## Triage Tree
1. Errors increase?
   - 5xx dominated by upstream timeouts → check Trino/Kafka/Timescale
   - 429s increase → quotas and concurrency policy in effect
2. Latency spikes with low errors?
   - CPU/memory throttling, GC pauses, connection pool wait time
3. Availability drops?
   - Pod crashes, OOM, ingress/LB issues

## Remediation Actions
- Temporarily scale API and workers
- Reduce endpoint concurrency via quotas (see `docs/quotas_and_concurrency.md`)
- Enable cached responses for heavy endpoints; extend TTLs
- Roll back to last known good release

```bash
# Rollout status and rollback
kubectl -n aurum-dev rollout status deploy/aurum-api
kubectl -n aurum-dev rollout history deploy/aurum-api
kubectl -n aurum-dev rollout undo deploy/aurum-api --to-revision=PREV
```

## Escalation
- If unacked > 10m or mitigation not found in 30m → escalate per paging policy
- Notify service owner and platform secondary

## Verification & Closure
- Error rate and latency back to baseline for 30 minutes
- Postmortem for P0/P1 within 24–48h; track actions

