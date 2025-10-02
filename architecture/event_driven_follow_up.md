# Event-Driven Platform – Next Steps

The scaffolding introduced for the event store, streaming layer, and saga orchestration covers Phases 0–4 of the roadmap. The remaining phases require operational hardening and organisation-wide rollout. The checklist below captures the critical follow-up work.

## Phase 5 – Replay and Time Travel
- Implement CLI utilities in `aurum.cli` for aggregate replay (consume `TimescaleEventStore.load_stream` and `TimescaleEventStore.get_snapshot`).
- Add projection bootstrapper service that reads from Kafka topics via `KafkaEventConsumer` and rebuilds materialized views.
- Persist replay audit events to `aurum.ops.replay.v1` topic using the outbox dispatcher.

## Phase 6 – Schema Governance
- Extend `SchemaValidator` to perform Avro structural validation using generated models.
- Update CI pipeline to call `SubjectContracts.validate_schema_payload` before merging schema changes.
- Document evolution policy in `docs/schema_registry.md` (backward-compatible defaults, exception workflow).

## Phase 7 – Reliability (DLQ, Retries, Idempotency)
- Wire `KafkaEventConsumer` DLQ publisher to environment-specific topics and expose metrics.
- Configure per-topic retry budget (attempt count, delay strategy) in service settings.
- Enable periodic cleanup job for `IdempotencyTracker` data and integrate with observability alerts.

## Phase 8 – Service Integration
- Produce an integration guide for internal services describing command vs. event channels, required headers, and tenancy propagation.
- Update API layer (e.g., drought endpoints) to read from projection caches fed by Kafka consumers.
- Introduce dual-write feature flags so services can shadow produce events while still writing to legacy stores.

## Phase 9 – Observability & Ops
- Add Grafana dashboards for event store throughput, outbox backlog, saga success rate, DLQ volumes, and consumer lag.
- Instrument saga execution with tracing spans (`trace_span`) and Prometheus counters for step latency.
- Create runbooks for replay, schema rollout, DLQ reprocessing, and saga incident response.

## Phase 10 – Rollout Strategy
- Pilot event sourcing on a low-risk domain (scenario lifecycle) with strict monitoring.
- Execute load tests targeting the 300% scalability uplift; adjust Kafka partitions and producer batching.
- After burn-in, migrate additional domains and decommission legacy request-response flows.

## Operational To-Dos
- Provision Kafka/Schema Registry infrastructure updates (topics, ACLs, retention policies).
- Author Alembic migrations mirroring the Timescale DDL (events, snapshots, metadata, outbox, saga, idempotency tables).
- Define configuration schema for outbox dispatcher (batch size, poll interval, DLQ topic).

