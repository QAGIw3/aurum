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
- **[External Data Architecture](external-data.md)** - Economic data ingestion system design
- **[External Data Contracts](external_contracts.md)** - API contracts and data formats
- **[EIA Integration](api/eia.md)** - Energy Information Administration APIs
- **[EIA Dataset Configuration](ingestion/eia_dataset_config.md)** - EIA data source configuration
- **[External API Documentation](api/external.md)** - External provider API reference
- **[Data Incremental Processing](external/incremental.md)** - Incremental data processing
- **[ISO Canonical Contracts](external/iso_canonical_contract.md)** - Standardized energy market data formats

## 🎯 Scenarios & Data Processing

### Scenario System
- **[Scenarios Overview](scenarios.md)** - Scenario modeling and execution
- **[Scenario Outputs & Metrics](scenarios.md#outputs-and-metrics)** - Understanding scenario results
- **[Scenario Smoke Tests](scenarios/smoke.md)** - Testing and validation procedures
- **[Golden Query Cache](golden_query_cache.md)** - Caching strategy for scenarios

### Data Pipeline & Processing
- **[Airflow Operations](airflow.md)** - Workflow orchestration
- **[Airflow Setup Guide](airflow-setup.md)** - Complete Airflow configuration and deployment
- **[Airflow Variables](airflow-variables.md)** - Configuration management
- **[dbt Transformations](../dbt/)** - Data transformation models
- **[Backfill Processing](backfill_driver.md)** - Historical data processing
- **[Data Quality Assertions](data_quality_assertions.md)** - Data validation and testing framework

## ⚡ Performance & Operations

### Performance Optimization
- **[Caching Best Practices](cache/caching-best-practices.md)** - Caching strategies and patterns
- **[Cost Profiling](cost_profiler.md)** - Monitor and optimize resource usage
- **[Quotas & Concurrency](quotas_and_concurrency.md)** - Resource management
- **[Performance Monitoring](canary_monitoring.md)** - System monitoring and alerting
- **[API Offload Strategies](api/offload.md)** - Async processing and performance optimization

### Operational Runbooks
- **[SRE Index](sre/README.md)** - Paging policy, on-call, chaos drills
- **[Paging Policy](sre/paging-policy.md)** - When/what/how we page
- **[API On-call](runbooks/api-oncall-runbook.md)** - API incident response
- **[Airflow On-call](runbooks/airflow-oncall-runbook.md)** - Scheduler/workers incidents
- **[Kafka On-call](runbooks/kafka-oncall-runbook.md)** - Broker/consumer/lag incidents
- **[Trino On-call](runbooks/trino-oncall-runbook.md)** - Query pool/worker incidents
- **[Chaos Drills](runbooks/chaos-drills-runbook.md)** - Monthly resilience exercises
- **[Platform Operations](runbooks/oncall-platform-runbook.md)** - On-call procedures
- **[External Data Operations](runbooks/external-data.md)** - Data pipeline operations
- **[Multi-region Failover](runbooks/multi-region-failover-runbook.md)** - Disaster recovery
- **[API Concurrency Issues](runbooks/api_concurrency_runbook.md)** - API troubleshooting
- **[Advanced Workflow Orchestration](workflows/advanced_orchestration.md)** - Dynamic DAG promotion, monitoring, and recovery
- **[All Runbooks →](runbooks/)** - Complete operational guides

## 🔒 Security & Configuration

### Security
- **[Tenant Isolation & RLS](security/tenant-rls.md)** - Multi-tenant security model
- **[OIDC Forward Auth](auth/oidc-forward-auth.md)** - Authentication setup
- **[Secrets Management](security/secrets-policy.md)** - Secret handling policies
- **[Platform Security & Reliability](platform-reliability-security-cost-optimization.md)** - Comprehensive security features and cost optimization
- **[Vault Rotation](vault-rotation.md)** - Key rotation procedures

### Configuration & Management
- **[Runtime Configuration](runtime-config.md)** - Dynamic configuration management
- **[Migration Guide](migration-guide.md)** - Version migration procedures
- **[System Maintenance](maintenance/refactored_system.md)** - Maintenance operations and system management
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
- **[Canary Deployment](implementations/canary-deployment.md)** - Progressive deployment strategies

### Development Planning & Architecture
- **[Development Roadmap 2024-2025](development-roadmap-2024.md)** - Strategic development priorities and phases
- **[Next 10 Development Steps](next-development-steps.md)** - Immediate tactical development priorities
- **[Architecture Refactor Plan](refactor-plan.md)** - Current refactoring initiatives and technical debt reduction
- **[Refactor Implementation Guide](refactor-implementation-guide.md)** - Practical refactoring patterns and examples
- **[Refactor Documentation →](refactor/)** - Detailed refactor ADRs, playbooks, and roadmaps

### Project Execution & Roadmaps
- **[Roadmap Implementation Guide](roadmap-implementation-guide.md)** - Execution templates and patterns
- **[Roadmap Metrics Dashboard](roadmap-metrics-dashboard.md)** - KPIs and success tracking  
- **[Ingestion & Scenarios Roadmap](roadmap-ingestion-scenarios.md)** - Data pipeline evolution

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
