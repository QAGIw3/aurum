# Kafka On-Call Runbook

## Overview
Procedures for incidents affecting Kafka availability, throughput, consumer lag, and schema compatibility.

## Contacts
- Primary On-call: oncall-kafka@aurum.com
- Slack: #oncall-ops
- PagerDuty Service: Kafka

## Key Signals & Paging
- Under-replicated partitions > 0 (P0 if sustained > 5m)
- Offline partitions > 0 (P0)
- Consumer lag growing on critical groups (P1)
- Broker unavailability or ISR shrinkage (P0/P1)

## First 5 Minutes
1. Ack and declare IC; set next update time
2. Scope impact: which topics/tenants/regions affected
3. Stabilize: throttle producers, pause non-critical consumers, ensure ISR health

```bash
# Brokers and pods
kubectl -n aurum-dev get po -l app.kubernetes.io/name=kafka
kubectl -n aurum-dev logs $(kubectl -n aurum-dev get po -l app.kubernetes.io/name=kafka -o name | head -1) --tail=200 | grep -i "UnderReplicated\|OutOfSync\|OOM"

# Under-replicated and offline partitions
kubectl -n aurum-dev exec -it $(kubectl -n aurum-dev get po -l app.kubernetes.io/name=kafka -o name | head -1 | cut -d/ -f2) -- \
  kafka-topics --bootstrap-server localhost:9092 --describe --under-replicated-partitions | head -50
kubectl -n aurum-dev exec -it $(kubectl -n aurum-dev get po -l app.kubernetes.io/name=kafka -o name | head -1 | cut -d/ -f2) -- \
  kafka-topics --bootstrap-server localhost:9092 --describe --unavailable-partitions | head -50

# Consumer group lag (all groups)
kubectl -n aurum-dev exec -it $(kubectl -n aurum-dev get po -l app.kubernetes.io/name=kafka -o name | head -1 | cut -d/ -f2) -- \
  kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe | head -100

# Schema Registry quick check
curl -s ${SCHEMA_REGISTRY:-http://schema-registry:8081}/subjects | head -20
```

## Dashboards & Signals
- Kafka Brokers: CPU/memory, network, request handler idle
- Topic health: under-replicated partitions, ISR size, leader imbalance
- Consumer lag: per critical consumer group; DLQ volume

## Common Failure Modes
- Broker crash/GC pauses → OOM or heap pressure; ISR shrink
- Disk pressure / IO latency → under-replicated partitions
- Network partitions → controller election flaps
- Schema evolution issues → consumer failures, DLQ growth

## Remediation Actions
- Rebalance leadership and partitions
```bash
# Prefer a stable controller, then rebalance leadership
kubectl -n aurum-dev exec -it <broker-pod> -- kafka-preferred-replica-election --bootstrap-server localhost:9092
```
- Scale brokers or increase pod resources for short-term relief
```bash
kubectl -n aurum-dev scale statefulset kafka --replicas=4
```
- Throttle producers or pause high-traffic topics
```bash
# Example: reduce linger / batch size via env or config rollout
# Coordinate with producers; consider backpressure via quotas
```
- Fix schema compatibility
```bash
# Inspect latest schema and compatibility
echo '{"compatibility":"BACKWARD"}' | \
  curl -s -X PUT -H 'Content-Type: application/json' --data-binary @- \
  ${SCHEMA_REGISTRY:-http://schema-registry:8081}/config
```
- Reassign partitions to balance load (use with care)
```bash
# Generate and execute a reassignment plan (tooling dependent)
```

## Escalation
- P0: sustained offline/under-replicated partitions > 5m, data loss risk → escalate per paging policy
- Involve data engineering if consumer logic or schema failures dominate

## Verification & Closure
- Under-replicated=0, offline=0, lag stable/declining
- DLQ rates back to baseline; successful consumption for 30m
- Postmortem for P0/P1 within 24–48h
