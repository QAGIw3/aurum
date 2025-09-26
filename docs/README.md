# Aurum Documentation

This directory contains developer and operator documentation for the Aurum platform.

## Getting Started

- Architecture overview: architecture-overview.md
- Architecture & deep dive: aurum-developer-documentation.md
- API usage guide: api_usage_guide.md
- Scenarios overview: scenarios.md
- Kubernetes developer flow: k8s-dev.md
- Onboarding checklist: onboarding.md

## Development Roadmap & Planning

- **Development Roadmap 2024-2025**: [development-roadmap-2024.md](development-roadmap-2024.md) - Strategic development plan and phased approach
- **Implementation Guide**: [roadmap-implementation-guide.md](roadmap-implementation-guide.md) - Practical templates and patterns for execution  
- **Metrics Dashboard**: [roadmap-metrics-dashboard.md](roadmap-metrics-dashboard.md) - KPIs, tracking, and success criteria
- **Current Refactor Plan**: [refactor-plan.md](refactor-plan.md) - Existing architectural refactoring initiative
- **Ingestion & Scenarios Roadmap**: [roadmap-ingestion-scenarios.md](roadmap-ingestion-scenarios.md) - Data pipeline and scenario engine evolution

## API

- API index and examples: api/README.md
- OpenAPI spec (source of truth): api/openapi-spec.yaml
- Generated reference (Markdown): api/api-docs.md
- Generated reference (HTML): api/api-docs.html
- Pagination model: pagination.md
- Runtime configuration (admin): runtime-config.md

## Scenarios

- Scenarios overview: scenarios.md
- Scenario outputs, metrics, limits: scenarios.md#outputs-and-metrics
- Golden query cache (scenarios): golden_query_cache.md

## External Data & Ingestion

- External data architecture: external-data.md
- Incremental processor: external/incremental.md
- EIA dataset configuration: ingestion/eia_dataset_config.md
- External contracts and topics: external_contracts.md
- EIA/FRED/NOAA API notes: api/eia.md, external.md
- SeaTunnel jobs and usage: ../seatunnel/README.md
- NOAA deployment guide: ../NOAA_DEPLOYMENT_GUIDE.md
- NOAA expansion summary: ../NOAA_EXPANSION_SUMMARY.md

## Caching & Performance

- Golden query cache: golden_query_cache.md
- Structured logging: structured_logging_guide.md
- Quotas and concurrency: quotas_and_concurrency.md
- Canary monitoring: canary_monitoring.md

## Security

- Tenant isolation and RLS: security/tenant-rls.md
- API auth & RBAC: auth/api-auth.md
- OIDC forward-auth setup: auth/oidc-forward-auth.md

## Observability

- Observability guide: observability/observability-guide.md
- Probe health checks: runbooks/probe-health-checks-playbook.md
- Airflow overview: airflow.md
- Audit logging: observability/audit-logging.md

## Configuration

- Full configuration reference: configuration.md
- Runtime configuration API: runtime-config.md
- Storage plan: storage/plan.md

## Data, Metrics, and Dictionary

- Data dictionary: data_dictionary.md
- Metric catalog: metric_catalog.md

## Runbooks

- Operations runbooks: runbooks/
- On-call (platform): runbooks/oncall-platform-runbook.md
- External data operations: runbooks/external-data.md
- Production external-data runbook: runbooks/external-data-production-runbook.md
- Data operations: runbooks/data_operations_runbook.md
- Multi‑region failover: runbooks/multi-region-failover-runbook.md
- Vendor ingestion: runbooks/vendor_ingestion.md

## Migrations & Admin

- API version migration: migration-guide.md
- Database migrations: ../db/migrations/README.md

## Building Docs

- Serve statically: `make docs-serve` (http://localhost:8000)
- Regenerate OpenAPI and API docs from the FastAPI app:
  - `make docs-openapi` (writes api/openapi-spec.yaml and api/openapi-spec.json)
  - `make docs-openapi-validate` (validates the spec)
  - `make docs-build` (writes api/api-docs.md and api/openapi.json)

Looking for contribution guidelines? See ../CONTRIBUTING.md
