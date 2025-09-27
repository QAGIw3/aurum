# Aurum Documentation

Welcome to the Aurum platform documentation! This is your central hub for all technical documentation, guides, and operational resources.

## 🚀 Getting Started

New to Aurum? Start here:

- **[Onboarding Checklist](onboarding.md)** - Complete setup guide for new developers
- **[Architecture Overview](architecture-overview.md)** - High-level system design and components
- **[Local Development Setup](k8s-dev.md)** - Get your development environment running
- **[API Usage Guide](api_usage_guide.md)** - Learn to use the Aurum APIs

## 🏗️ Architecture & Development

### Core Architecture
- **[Architecture Deep Dive](aurum-developer-documentation.md)** - Comprehensive technical documentation
- **[Code Structure Map](CODEMAP.md)** - Navigate the codebase efficiently
- **[Development Roadmap](development-roadmap-2024.md)** - Strategic development priorities
- **[Next Development Steps](next-development-steps.md)** - Immediate development priorities

### Implementation Patterns
- **[Async DAO Pattern](async_dao_pattern.md)** - Database access patterns
- **[Configuration Management](configuration.md)** - System configuration reference
- **[Feature Flags](feature-flags.md)** - Feature flag system usage
- **[Data Contracts](data-contracts.md)** - API and data contracts

## 📚 API & Integration

### API Documentation
- **[API Reference](api/README.md)** - Complete API documentation and examples
- **[OpenAPI Specification](api/openapi-spec.yaml)** - Machine-readable API contract
- **[Generated API Docs](api/api-docs.md)** - Auto-generated API reference
- **[API Authentication](auth/api-auth.md)** - Authentication and authorization setup
- **[Pagination Guide](pagination.md)** - Working with paginated APIs

### External Data Integration
- **[External Data Architecture](external-data.md)** - Data ingestion system design
- **[EIA Integration](api/eia.md)** - Energy Information Administration APIs
- **[NOAA Weather Data](external.md)** - Weather data integration
- **[Data Incremental Processing](external/incremental.md)** - Incremental data processing

## 🎯 Scenarios & Data Processing

### Scenario System
- **[Scenarios Overview](scenarios.md)** - Scenario modeling and execution
- **[Scenario Outputs & Metrics](scenarios.md#outputs-and-metrics)** - Understanding scenario results
- **[Golden Query Cache](golden_query_cache.md)** - Caching strategy for scenarios

### Data Pipeline & Processing
- **[Airflow Operations](airflow.md)** - Workflow orchestration
- **[Airflow Variables](airflow-variables.md)** - Configuration management
- **[dbt Transformations](../dbt/)** - Data transformation models
- **[Backfill Processing](backfill_driver.md)** - Historical data processing

## ⚡ Performance & Operations

### Performance Optimization
- **[Caching Best Practices](cache/caching-best-practices.md)** - Caching strategies and patterns
- **[Cost Profiling](cost_profiler.md)** - Monitor and optimize resource usage
- **[Quotas & Concurrency](quotas_and_concurrency.md)** - Resource management
- **[Performance Monitoring](canary_monitoring.md)** - System monitoring and alerting

### Operational Runbooks
- **[Platform Operations](runbooks/oncall-platform-runbook.md)** - On-call procedures
- **[External Data Operations](runbooks/external-data.md)** - Data pipeline operations
- **[Multi-region Failover](runbooks/multi-region-failover-runbook.md)** - Disaster recovery
- **[API Concurrency Issues](runbooks/api_concurrency_runbook.md)** - API troubleshooting
- **[All Runbooks →](runbooks/)** - Complete operational guides

## 🔒 Security & Configuration

### Security
- **[Tenant Isolation & RLS](security/tenant-rls.md)** - Multi-tenant security model
- **[OIDC Forward Auth](auth/oidc-forward-auth.md)** - Authentication setup
- **[Secrets Management](security/secrets-policy.md)** - Secret handling policies
- **[Vault Rotation](vault-rotation.md)** - Key rotation procedures

### Configuration & Management
- **[Runtime Configuration](runtime-config.md)** - Dynamic configuration management
- **[Migration Guide](migration-guide.md)** - Version migration procedures
- **[Test Fixtures](test_fixtures.md)** - Testing data and fixtures
- **[Fast Local Testing](testing-fast-local.md)** - Optimized testing workflows

## 📊 Observability & Data

### Monitoring & Observability  
- **[Observability Guide](observability/observability-guide.md)** - Comprehensive monitoring setup
- **[Structured Logging](structured_logging_guide.md)** - Logging best practices
- **[Audit Logging](observability/audit-logging.md)** - Security and compliance logging
- **[Health Checks](runbooks/probe-health-checks-playbook.md)** - System health monitoring

### Data & Analytics
- **[Data Dictionary](data_dictionary.md)** - Complete data catalog
- **[Metric Catalog](metric_catalog.md)** - Available metrics and KPIs
- **[Schema Registry](schema_registry.md)** - Data schema management
- **[Storage Planning](storage/plan.md)** - Data storage architecture

## 🛠️ Implementation Guides

### Data Source Implementations
- **[NOAA Deployment](implementations/noaa-deployment-guide.md)** - Weather data pipeline setup
- **[NOAA Expansion](implementations/noaa-expansion-summary.md)** - Coverage expansion details
- **[ISO-NE Implementation](implementations/isone-implementation.md)** - New England ISO integration
- **[MISO Implementation](implementations/miso-implementation.md)** - Midcontinent ISO integration
- **[External Data Ingestion](implementations/external-data-ingestion.md)** - General ingestion patterns

### Project Management & Planning
- **[Roadmap Implementation Guide](roadmap-implementation-guide.md)** - Execution templates and patterns
- **[Roadmap Metrics Dashboard](roadmap-metrics-dashboard.md)** - KPIs and success tracking  
- **[Ingestion & Scenarios Roadmap](roadmap-ingestion-scenarios.md)** - Data pipeline evolution
- **[Refactor Implementation Guide](refactor-implementation-guide.md)** - Architecture refactoring
- **[Refactor Plan](refactor-plan.md)** - Current refactoring initiatives

## 📋 Documentation Maintenance

### Building Documentation
- **Serve Locally**: `make docs-serve` → [http://localhost:8000](http://localhost:8000)
- **Generate API Docs**: `make docs-build` (creates api/api-docs.md and api/openapi.json)
- **Validate OpenAPI**: `make docs-openapi-validate`
- **Update OpenAPI**: `make docs-openapi` (regenerates api/openapi-spec.yaml)

### Reference Links
- **[Contributing Guidelines](../CONTRIBUTING.md)** - Development and contribution process
- **[Database Migrations](../db/migrations/README.md)** - Database schema management
- **[SeaTunnel Jobs](../seatunnel/README.md)** - Data pipeline job configuration

---

💡 **Tip**: Use the search function (Ctrl/Cmd + F) to quickly find specific topics in this documentation hub.
