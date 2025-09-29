# Event-Driven Architecture Platform

## Phase 0 – Scoping Summary

### Bounded Contexts and Aggregates
| Context | Aggregate | Primary Events | Owning Team |
| --- | --- | --- | --- |
| Curves | `CurveSeries`, `CurveSnapshot` | `curve.series.created`, `curve.series.repriced`, `curve.snapshot.published` | Curves & Analytics |
| Drought Analytics | `DroughtIndex`, `DroughtVectorLayer` | `drought.index.ingested`, `drought.vector.event.recorded` | Climate Intelligence |
| Scenarios | `Scenario`, `ScenarioRun` | `scenario.created`, `scenario.updated`, `scenario.run.started`, `scenario.run.completed`, `scenario.run.failed`, `scenario.run.cancelled` | Scenario Orchestration |
| External Ingestion | `ExternalFeedJob` | `ingest.job.started`, `ingest.job.completed`, `ingest.job.failed` | Data Ingestion |
| Governance | `SchemaContract`, `PolicyDecision` | `schema.contract.updated`, `policy.decision.issued` | Governance |
| Operations | `ReplayJob`, `BackfillJob` | `replay.job.started`, `replay.job.completed`, `backfill.job.started`, `backfill.job.completed` | Platform Ops |

### Event Envelope (Canonical Contract)
```json
{
  "event_id": "uuid",
  "event_type": "scenario.run.started",
  "aggregate_id": "scenario-run-123",
  "aggregate_type": "ScenarioRun",
  "sequence": 42,
  "schema_version": 1,
  "occurred_at": "2024-09-28T19:16:00.000Z",
  "recorded_at": "2024-09-28T19:16:05.000Z",
  "correlation_id": "req-abc",
  "causation_id": "event-xyz",
  "trace_id": "trace-123",
  "tenant_id": "aurum-default",
  "source": "aurum.scenarios",
  "payload": {},
  "metadata": {
    "producer_service": "aurum-scenarios-api",
    "request_id": "req-abc"
  }
}
```

### Topic Strategy
| Topic | Purpose | Partitions | Retention | DLQ |
| --- | --- | --- | --- | --- |
| `aurum.curves.series.v1` | Curve series lifecycle | 12 | 90 days | `aurum.curves.series.v1.dlq` |
| `aurum.curves.snapshots.v1` | Curve snapshots | 6 | 30 days | `aurum.curves.snapshots.v1.dlq` |
| `aurum.drought.index.v1` | Drought index events | 8 | 120 days | `aurum.drought.index.v1.dlq` |
| `aurum.drought.vector.v1` | Vector layers | 6 | 120 days | `aurum.drought.vector.v1.dlq` |
| `aurum.scenario.lifecycle.v1` | Scenario aggregate events | 12 | 180 days | `aurum.scenario.lifecycle.v1.dlq` |
| `aurum.ingest.jobs.v1` | Ingestion job state | 6 | 30 days | `aurum.ingest.jobs.v1.dlq` |
| `aurum.ops.replay.v1` | Replay orchestration | 3 | 7 days | `aurum.ops.replay.v1.dlq` |
| `aurum.ops.backfill.v1` | Backfill operations | 3 | 30 days | `aurum.ops.backfill.v1.dlq` |

Retention values align with operational requirements: long-lived business aggregates keep 6–12 months, operational topics keep shorter windows. DLQ topics adopt `*.dlq` suffix and mirror partition counts for routing simplicity.

### Schema Registry Governance
- All subjects conform to `aurum.<context>.<entity>.v<major>-value` naming.
- Compatibility mode: **BACKWARD** by default, **FULL** for mission-critical topics (curves, scenarios).
- Each event payload Avro schema references the canonical envelope; envelope stored separately in `aurum.events.EventRecord`.

### Ownership Matrix
- **Curves & Analytics**: Own curve topics, projections, schemas, and consumer groups.
- **Climate Intelligence**: Own drought topics, ingestion pipelines, projections.
- **Scenario Orchestration**: Own scenario topics, saga orchestrator, replay tooling for scenario runs.
- **Platform Ops**: Own operational topics, replay CLI, event store infrastructure.
- **Governance**: Own schema registry policy enforcement and CI checks.

### Backlog for Phase 0
1. Finalize Avro schemas for each event type and register via CI pipeline.
2. Update `kafka/schemas/contracts.yml` with new subjects and retention metadata.
3. Provision Kafka ACLs per context (produce/consume) and coordinate with security team.
4. Prepare Grafana dashboards for partition utilization and DLQ metrics.

