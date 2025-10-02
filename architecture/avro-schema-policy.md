## Avro and Schema Registry Policy

This document defines naming, compatibility, and governance for streaming contracts.

### Naming
- Subjects: `aurum.<domain>.<entity>.<version>-(key|value)`; e.g., `aurum.ref.eia.series.v1-value`.
- Topics mirror subject base: `aurum.<domain>.<entity>.<version>`.
- DLQ topics are versioned: append `.dlq.v<version>`; e.g., `aurum.curve.observation.dlq.v1`.

### Compatibility
- Default: BACKWARD for all `*-value` subjects.
- Transitive modes may be used for append-only schemas; declare in `kafka/schemas/contracts.yml`.
- Breaking changes require new major version (`v2`) and coordinated topic/subject introduction.

### Contracts
- Source of truth lives in `kafka/schemas/contracts.yml` with subject → schema mapping.
- CI enforces contracts on PR via schema syntax, compatibility, and contract validation.
- Required fields are specified per contract and must not be removed without a major version.

### Registration
- Use CI step or run: `python scripts/ci/register_schemas.py kafka/schemas --registry-url $SCHEMA_REGISTRY_URL --compatibility BACKWARD --fail-on-error`.
- Compatibility is set per-subject to match contract; CI fails on violations.

### DLQ Envelope
- DLQ records use `ingest.error.v1.avsc` with `source`, `error_message`, `severity`, `ingest_ts`, and optional `original_payload`.
- Keep DLQ retention ≥ 30d; access restricted to ops principals.

### Replay
- Prefer targeted re-processing over raw replay. If replaying, validate payloads against current `*-value` schema and rate-limit producers.

### Observability
- Emit policy events to `aurum.schema_registry.policy`; registration events to `aurum.schema_registry.events`.


