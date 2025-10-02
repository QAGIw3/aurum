# Airflow On-Call Runbook

## Overview
Procedures for incidents affecting DAG scheduling, execution latency, and data SLAs.

## Contacts
- Primary On-call: oncall-airflow@aurum.com
- Slack: #oncall-ops
- PagerDuty Service: Airflow

## Key Signals & Paging
- Scheduler heartbeat missing > 5 minutes (P1/P0 if broad)
- Backlog growth: queued/running tasks exceed thresholds
- Task failure rate > 5% for 10 minutes
- SLAs: freshness SLO breach on critical DAGs

## First 5 Minutes
1. Ack and declare IC; set next update time
2. Identify scope: single DAG vs platform-wide
3. Stabilize: pause non-critical DAGs; increase worker replicas

```bash
# Scheduler & workers
kubectl -n aurum-dev get deploy,po -l app.kubernetes.io/name=airflow
kubectl -n aurum-dev logs deploy/airflow-scheduler --tail=200 | tail -n +1

# Airflow CLI quick checks
airflow dags list | head -50
airflow tasks failed <dag_id> $(date -d '1 hour ago' +%Y-%m-%d) | head -50

# Pause noisy DAGs
airflow dags pause <dag_id>

# Scale workers
kubectl -n aurum-dev scale deploy airflow-worker --replicas=6
```

## Dashboards & Signals
- Scheduler heartbeats, DAG run duration, task success rate
- Queue depth and worker CPU/memory saturation
- External systems: Kafka, Trino, Timescale connectivity

## Common Failure Modes
- Scheduler stuck on heavy DagBag parse → reduce DAG import cost, validate env vars
- Worker image mismatch / dependency drift → redeploy stable image
- External API rate limits → enable backoff; reduce parallelism

```bash
# Pools, concurrency, and variables
airflow pools list
airflow variables list | head -100

# Executor backlog
kubectl -n aurum-dev top pods -l app.kubernetes.io/name=airflow
```

## Remediation Actions
- Pause/disable heavy DAGs temporarily
- Lower `max_active_runs` / `concurrency` per critical DAG
- Re-sync connections and variables; restart scheduler

```bash
kubectl -n aurum-dev rollout restart deploy/airflow-scheduler
kubectl -n aurum-dev rollout restart deploy/airflow-worker
```

## Escalation
- DAG-level logic issues → escalate to data engineering owner
- Platform degradation > 30m → escalate per paging policy

## Verification & Closure
- Backlog drained; SLAs restored
- Postmortem for P0/P1 within 24–48h

