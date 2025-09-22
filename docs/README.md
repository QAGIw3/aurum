# Aurum Documentation

This directory contains developer and operator documentation for the Aurum platform.

## Getting Started

- Architecture & deep dive: `aurum-developer-documentation.md`
- API usage guide: `api_usage_guide.md`
- Scenarios overview: `scenarios.md`
- Kubernetes developer flow: `k8s-dev.md`

## API

- API index and examples: `api/README.md`
- OpenAPI spec: `../openapi/aurum.yaml`
- Generated reference: `api/api-docs.md` (see build below)
- Pagination model: `pagination.md`
- Runtime configuration (admin): `runtime-config.md`

## Caching & Performance

- Golden query cache: `golden_query_cache.md`
- Structured logging: `structured_logging_guide.md`
- Quotas and concurrency: `quotas_and_concurrency.md`
- Canary monitoring: `canary_monitoring.md`

## Data & Contracts

- Data contracts: `data-contracts.md`
- Schema registry and Avro: `schema_registry.md`
- External data: `external-data.md`

## Runbooks

- Runbooks index: `runbooks/`

## Configuration

- Full configuration reference: `configuration.md`

## Security

- Tenant isolation and RLS: `security/tenant-rls.md`

## Build Docs

- Serve statically: `make docs-serve` (http://localhost:8000)
- Build API docs from the FastAPI app and export OpenAPI:
  - `make docs-build` (writes `docs/api/api-docs.md` and `docs/api/openapi.json`, plus `openapi/aurum.generated.yaml`)
