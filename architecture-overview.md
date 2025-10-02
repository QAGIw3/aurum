# Architecture Overview

Aurum is a data and API platform for market intelligence. This page summarizes the system at a glance; see docs/aurum-developer-documentation.md for the deep dive.

## Core Components

- API (FastAPI): serves curves, metadata, scenarios; rate limiting, caching, auth (OIDC/JWT optional), observability.
- Storage: Iceberg (Nessie) on S3/MinIO for versioned data; Postgres/Timescale for ops and scenario metadata; ClickHouse for logs/OLAP.
- Compute: Trino (federated SQL), Spark (batch/backfills), dbt (SQL transforms), Airflow (orchestration).
- Stream: Kafka + Schema Registry (Avro contracts), SeaTunnel / Connect (sources & sinks).
- Governance/Obs: lakeFS (object versioning), OpenMetadata/Marquez (lineage), Vector→ClickHouse (logs), Prometheus (metrics).

## Data Flow (Simplified)

1) Ingest: Files/feeds → Kafka (Avro) or S3 raw → Iceberg staging
2) Validate: Great Expectations → quarantine/DLQ when failing
3) Transform: dbt/Spark → conformed facts/dimensions and marts
4) Serve: API and Trino (SQL) with Redis-backed caches, plus file exports
5) Govern/Observe: Versioning, lineage, logs/metrics, alerts

## Scenarios

- Metadata and runs tracked in Postgres (RLS per tenant)
- Worker writes outputs to Iceberg (default) or Postgres depending on deployment
- API reads outputs via Trino; golden query cache accelerates hot endpoints

## Environments

- Dev: docker compose (profiles: core, worker, ui)
- Kind/Kubernetes: one‑node cluster with Strimzi, ClickHouse, Vector, etc. (see docs/k8s-dev.md)

## Contracts

- OpenAPI (docs/api/openapi-spec.yaml): API contract
- Avro (kafka/schemas): topic schemas
- dbt: SQL models and tests

For hands-on setup, start with README.md → Dev stack; for details on each area, follow links in docs/README.md.
