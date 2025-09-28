# Aurum

Aurum is a comprehensive market intelligence platform for energy trading, providing curve data, scenario modeling, and external data ingestion capabilities.

## Quick Start

1. **Setup**: Copy `.env.example` to `.env` and populate required secrets
2. **Start Platform**: `COMPOSE_PROFILES=core docker compose -f compose/docker-compose.dev.yml up -d`
3. **Bootstrap**: `COMPOSE_PROFILES=core,bootstrap docker compose -f compose/docker-compose.dev.yml up bootstrap --exit-code-from bootstrap`
4. **Access Services**: 
   - API: `http://localhost:8095`
   - Airflow: `http://localhost:8088`
   - MinIO: `http://localhost:9001`
   - Trino: `http://localhost:8080`

## Documentation

📖 **[Complete Documentation →](docs/README.md)**

### Essential Links
- 🚀 [Onboarding Guide](docs/onboarding.md) - Get started quickly
- 🏗️ [Architecture Overview](docs/architecture-overview.md) - System design
- 📚 [API Documentation](docs/api/README.md) - API usage and examples
- ☸️ [Kubernetes Development](docs/k8s-dev.md) - K8s workflow
- 🔒 [Security & Auth](docs/security/tenant-rls.md) - Authentication setup
- �� [Runbooks](docs/runbooks/) - Operations guides
- 💡 [Contributing](CONTRIBUTING.md) - Development guidelines

## Repository Structure

```
aurum/
├── airflow/          # Airflow DAGs for data orchestration
├── dbt/              # dbt models for data transformation
├── docs/             # Documentation (architecture, guides, runbooks)
├── kafka/schemas/    # Avro schemas for Kafka topics
├── src/aurum/        # Core application code
│   ├── api/          # FastAPI web service
│   ├── scenarios/    # Scenario modeling engine
│   ├── parsers/      # Vendor data parsers
│   └── external/     # External data collectors
├── trino/ddl/        # Iceberg table definitions
├── k8s/              # Kubernetes manifests
└── scripts/          # Utility scripts and tools
```

## Key Features

- **🔄 Data Ingestion**: Automated pipelines for EIA, NOAA, FRED, and ISO data
- **📊 Curve Analytics**: Market curve analysis and forecasting
- **🎯 Scenario Modeling**: What-if analysis and scenario planning
- **🔌 REST API**: Comprehensive API for data access and management
- **⚡ Real-time Processing**: Kafka-based streaming architecture
- **🗄️ Multi-store Architecture**: Trino, TimescaleDB, ClickHouse backends

## API Examples

```bash
# Get curve data
curl "http://localhost:8095/v1/curves?iso=PJM&market=DA&limit=10"

# List scenarios
curl "http://localhost:8095/v1/scenarios?limit=20"

# Create scenario
curl -X POST "http://localhost:8095/v1/scenarios" \
  -H "Content-Type: application/json" \
  -d '{"name": "Test Scenario", "assumptions": [...]}'
```

## Development

### Local Development
```bash
# Install dependencies
pip install -e .

# Start core services
COMPOSE_PROFILES=core docker compose -f compose/docker-compose.dev.yml up -d

# Run tests
pytest tests/

# Lint code
make lint

### CI/CD

- Pre-commit: `pre-commit install` then `make git-pre-commit` to run all hooks.
- Conventional Commits: follow `feat:`, `fix:`, `chore:`, etc. `cz commit` is supported.
- Coverage gate: pytest enforces `--cov-fail-under=85` via `pyproject.toml`.
- Image scanning: local `make image-scan`; CI uses Trivy and uploads SARIF.
- Releases: automatic SemVer + changelog via GitHub Actions `Release` workflow.
- E2E: `make e2e-up && make e2e-seed && make e2e-test && make e2e-down` or run the `E2E Pipeline` workflow.
```

### Kubernetes Development
See [K8s Development Guide](docs/k8s-dev.md) for complete instructions on:
- Setting up kind clusters
- Deploying to Kubernetes
- Managing secrets and configuration

## Support

- 📖 [Documentation](docs/README.md) - Complete guides and references
- 🐛 [Issues](https://github.com/QAGIw3/aurum/issues) - Bug reports and feature requests
- 💬 [Discussions](https://github.com/QAGIw3/aurum/discussions) - Questions and community
