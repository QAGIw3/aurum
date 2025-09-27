
.PHONY: help build test deploy lint clean docker-build docker-push k8s-deploy db-migrate security-scan security-pip-audit security-bandit trino-harness trino-harness-metrics reconcile-kafka-lake iceberg-maintenance ci-trino-smoke cache-warm \
	kind-create kind-apply kind-bootstrap kind-up kind-down kind-apply-ui kind-delete-ui \
	kafka-bootstrap kafka-register-schemas kafka-set-compat kafka-apply-topics kafka-apply-topics-kind kafka-apply-topics-dry-run \
	compose-bootstrap perf-k6 \
	airflow-check-vars airflow-print-vars airflow-apply-vars

TRINO_SERVER ?= http://localhost:8080
TRINO_USER ?= aurum
TRINO_CATALOG ?= iceberg
TRINO_SCHEMA ?= mart
TRINO_SERVER_HOST ?= localhost
TRINO_SERVER_PORT ?= 8080
KAFKA_BOOTSTRAP ?= localhost:9092

# Default target
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Development
build: ## Build the application
	docker build -f Dockerfile.api -t aurum-api .
	docker build -f Dockerfile.worker -t aurum-worker .

test: ## Run tests
	pytest tests/ -v

unit-test: ## Run unit test subset
	pytest -m "unit" -q

test-services: ## Run service layer tests
	pytest tests/api/test_services_*.py -v

test-services-coverage: ## Run service layer tests with coverage
	pytest tests/api/test_services_*.py --cov=src/aurum/api/services --cov-report=term-missing --cov-report=html -v

# Phase 1.4 Development Experience Enhancement
dev-health-check: ## Run Phase 1 development health checks
	python scripts/dev/health_checks.py

dev-quick-setup: ## Quick development environment setup
	pip install -e .
	@echo "✅ Development environment ready"

docker-dev-fast: ## Fast Docker development stack startup (Phase 1.4 target: <3 minutes)
	@echo "🚀 Starting Docker development stack (optimized for speed)..."
	@start_time=$$(date +%s); \
	COMPOSE_PROFILES=core docker compose -f compose/docker-compose.dev.yml up -d --build; \
	end_time=$$(date +%s); \
	elapsed_time=$$((end_time - start_time)); \
	minutes=$$((elapsed_time / 60)); \
	seconds=$$((elapsed_time % 60)); \
	echo "⏱️  Docker stack started in $${minutes}m $${seconds}s"; \
	if [ $$elapsed_time -gt 180 ]; then \
		echo "⚠️  Warning: Startup time exceeded 3-minute target"; \
	else \
		echo "✅ Startup time within 3-minute target"; \
	fi

lint: ## Run linting
	black --check src/ tests/
	isort --check-only src/ tests/
	flake8 src/ tests/

format: ## Format code
	black src/ tests/
	isort src/ tests/

clean: ## Clean up build artifacts
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	docker system prune -f

perf-k6: ## Run k6 smoke tests against v2 endpoints
	k6 run perf/k6/curves.js

compose-bootstrap: ## Run one-shot bootstrap against local Compose stack
	COMPOSE_PROFILES=core,bootstrap docker compose -f compose/docker-compose.dev.yml up bootstrap --exit-code-from bootstrap

# Docker operations
docker-build: ## Build Docker images
	docker build -f Dockerfile.api -t ghcr.io/aurum/api:latest .
	docker build -f Dockerfile.worker -t ghcr.io/aurum/aurum-worker:latest .

docker-push: ## Push Docker images to registry
	docker push ghcr.io/aurum/api:latest
	docker push ghcr.io/aurum/aurum-worker:latest

# Kubernetes operations
k8s-deploy: ## Deploy to Kubernetes
	kubectl apply -f k8s/api/
	kubectl apply -f k8s/scenario-worker/
	kubectl rollout status deployment/api -n aurum-dev --timeout=300s
	kubectl rollout status deployment/scenario-worker -n aurum-dev --timeout=300s

k8s-validate: ## Validate Kubernetes deployment
	@echo "Checking deployment status..."
	@kubectl get pods -n aurum-dev -l app=api
	@kubectl get pods -n aurum-dev -l app=scenario-worker
	@kubectl get svc -n aurum-dev
	@echo "✅ Deployment validation completed"

# Kind helpers
kind-create: ## Create the local kind cluster with required port mappings
	scripts/k8s/create_kind_cluster.sh ${KIND_FLAGS}

kind-apply: ## Apply core manifests (Strimzi, Schema Registry, data services)
	scripts/k8s/apply.sh

kind-bootstrap: ## Run bootstrap jobs for lakeFS, Nessie, Schema Registry
	scripts/k8s/bootstrap.sh

kind-up: ## Create, apply, and bootstrap the kind stack
	$(MAKE) kind-create
	$(MAKE) kind-apply
	$(MAKE) kind-bootstrap

kind-down: ## Destroy the kind cluster and cleanup mounted volumes
	scripts/k8s/destroy_kind_cluster.sh ${KIND_FLAGS}

kind-apply-ui: ## Deploy optional UI overlay (Superset, Kafka UI, Grafana)
	scripts/k8s/apply_ui.sh

kind-delete-ui: ## Remove optional UI overlay from the kind cluster
	scripts/k8s/delete_ui.sh

# Kafka helpers
kafka-bootstrap: ## Register Avro schemas and set compatibility in Schema Registry
	SCHEMA_REGISTRY_URL=$${SCHEMA_REGISTRY_URL:-http://localhost:8081}; \
		scripts/kafka/bootstrap.sh --schema-registry-url "$$SCHEMA_REGISTRY_URL"

kafka-register-schemas: ## Register Kafka schemas only
	SCHEMA_REGISTRY_URL=$${SCHEMA_REGISTRY_URL:-http://localhost:8081}; \
		python scripts/kafka/register_schemas.py --schema-registry-url "$$SCHEMA_REGISTRY_URL"

kafka-set-compat: ## Enforce BACKWARD compatibility on Kafka subjects
	SCHEMA_REGISTRY_URL=$${SCHEMA_REGISTRY_URL:-http://localhost:8081}; \
		python scripts/kafka/set_compatibility.py --schema-registry-url "$$SCHEMA_REGISTRY_URL"

kafka-apply-topics: ## Apply topic definitions from config/kafka_topics.json
	KAFKA_BOOTSTRAP_SERVERS=$${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}; \
		python scripts/kafka/manage_topics.py --bootstrap-servers "$$KAFKA_BOOTSTRAP_SERVERS" --config config/kafka_topics.json

kafka-apply-topics-dry-run: ## Preview topic changes without applying
	KAFKA_BOOTSTRAP_SERVERS=$${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}; \
		python scripts/kafka/manage_topics.py --bootstrap-servers "$$KAFKA_BOOTSTRAP_SERVERS" --config config/kafka_topics.json --dry-run

kafka-apply-topics-kind: ## Apply single-node topic plan for kind cluster
	KAFKA_BOOTSTRAP_SERVERS=$${KAFKA_BOOTSTRAP_SERVERS:-localhost:31092}; \
		python scripts/kafka/manage_topics.py --bootstrap-servers "$$KAFKA_BOOTSTRAP_SERVERS" --config config/kafka_topics.kind.json

# Database operations
db-migrate: ## Run database migrations
	@echo "Running database migrations..."
	@alembic upgrade head
	@echo "✅ Database migrations completed"

db-rollback: ## Rollback database migrations
	@read -p "Enter number of migrations to rollback: " num; \
	alembic downgrade -$$num

# Security
security-pip-audit: ## Run pip-audit against project dependencies
	python3 scripts/ci/run_security_scans.py --tool pip-audit

security-bandit: ## Run bandit static analysis
	python3 scripts/ci/run_security_scans.py --tool bandit

security-scan: security-pip-audit security-bandit ## Run combined security scans

# Performance
trino-harness: ## Run Trino query harness with default plan
	python scripts/trino/query_harness.py \
		--server $(TRINO_SERVER) \
		--user $(TRINO_USER) \
		--catalog $(TRINO_CATALOG) \
		--schema $(TRINO_SCHEMA) \
		--plan config/trino_query_harness.json

trino-harness-metrics: ## Run harness and emit metrics to ops_metrics
	python scripts/trino/query_harness.py \
		--server $(TRINO_SERVER) \
		--user $(TRINO_USER) \
		--catalog $(TRINO_CATALOG) \
		--schema $(TRINO_SCHEMA) \
		--plan config/trino_query_harness.json \
		--emit-metrics

# Quality
reconcile-kafka-lake: ## Compare Kafka offsets vs. Iceberg/Timescale counts
	python scripts/quality/reconcile_kafka_lake.py --bootstrap $(KAFKA_BOOTSTRAP) --trino-server $(TRINO_SERVER) --trino-user $(TRINO_USER)

iceberg-maintenance: ## Run Iceberg optimize + expire snapshots for curve tables
	python scripts/trino/run_sql.py --server $(TRINO_SERVER) --user $(TRINO_USER) --catalog iceberg trino/ddl/iceberg_maintenance.sql

# Monitoring
monitoring-health: ## Check monitoring health
	@kubectl port-forward svc/prometheus -n monitoring 9090:9090 &
	@sleep 2
	@curl -f http://localhost:9090/-/healthy && echo "✅ Prometheus healthy" || echo "❌ Prometheus unhealthy"
	@curl -f http://localhost:9090/-/ready && echo "✅ Prometheus ready" || echo "❌ Prometheus not ready"
	@pkill -f "port-forward"

# CI/CD helpers
ci-docker: ## Run Docker CI pipeline locally
	docker build -f Dockerfile.api -t ghcr.io/aurum/api:test .
	docker build -f Dockerfile.worker -t ghcr.io/aurum/aurum-worker:test .

ci-test: ## Run full test suite locally
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) security-scan
	$(MAKE) docs-openapi-check-drift

ci-unit: ## Run unit tests + lint locally
	$(MAKE) lint
	$(MAKE) unit-test

ci-trino-smoke: ## Run Trino query smoke tests with latency thresholds
	python scripts/ci/run_trino_smoke.py \
		--server $(TRINO_SERVER) \
		--user $(TRINO_USER) \
		--catalog $(TRINO_CATALOG) \
		--schema $(TRINO_SCHEMA) \
		--plan config/trino_query_harness.json

cache-warm: ## Run cache warmer locally using configured modes
	python - <<'PY'
import json
import subprocess
import sys
from pathlib import Path

config = json.loads(Path("config/trino_query_harness.json").read_text(encoding="utf-8"))
cache_cfg = config.get("cache_warming", {})
modes = cache_cfg.get("modes", ["warm", "refresh"])
host = cache_cfg.get("host", "$(TRINO_SERVER_HOST)")
port = cache_cfg.get("port", $(TRINO_SERVER_PORT))
script = cache_cfg.get("script", "scripts/trino/cache_warmer.py")

for mode in modes:
    cmd = [sys.executable, script, "--host", str(host), "--port", str(port), "--mode", mode, "--verbose"]
    subprocess.check_call(cmd)
PY

ci-deploy: ## Run deployment pipeline locally
	$(MAKE) build
	$(MAKE) k8s-deploy
	$(MAKE) k8s-validate

# Airflow Variables management
airflow-check-vars: ## Validate Airflow variable mapping file (no changes applied)
	python3 scripts/airflow/validate_variables.py --file config/airflow_variables.json

airflow-print-vars: ## Preview commands to sync Airflow Variables (dry run)
	python3 scripts/airflow/set_variables.py --apply --dry-run --file config/airflow_variables.json

airflow-apply-vars: ## Apply Airflow Variables from config/airflow_variables.json
	@if command -v airflow >/dev/null 2>&1 || [ -n "$$AIRFLOW_CLI" ]; then \
		python3 scripts/airflow/set_variables.py --apply --file config/airflow_variables.json; \
	else \
		echo "Airflow CLI not found. Showing commands (dry-run). Set AIRFLOW_CLI to a wrapper (e.g., 'docker compose -f <file> exec -T <svc> airflow') or install 'airflow'."; \
		python3 scripts/airflow/set_variables.py --apply --dry-run --file config/airflow_variables.json; \
	fi

# Environment setup
env-setup: ## Set up development environment
	pip install -r requirements-dev.txt
	pre-commit install
	@echo "Development environment ready"

# Git workflow helpers
git-pre-commit: ## Run pre-commit checks
	pre-commit run --all-files

git-release: ## Create a release
	@read -p "Enter release version: " version; \
	git tag -a "v$${version}" -m "Release v$${version}"; \
	git push origin main --tags; \
	echo "✅ Release v$${version} created"

# Documentation
docs-serve: ## Serve documentation locally
	@echo "Documentation available at http://localhost:8000"

docs-build: ## Build API docs markdown + JSON (fallback reads docs/api/openapi-spec.yaml)
	python3 scripts/docs/build_docs.py

docs-redoc: ## Generate Redoc HTML/JSON from docs/api/openapi-spec.yaml
	python3 scripts/generate_redoc.py

# OpenAPI generation and validation
docs-openapi: ## Generate OpenAPI spec from FastAPI routes
	python3 scripts/docs/generate_openapi.py

docs-openapi-validate: ## Validate OpenAPI spec (requires openapi-spec-validator)
	python3 -c "import sys, yaml; from openapi_spec_validator import validate_spec; s=yaml.safe_load(open('docs/api/openapi.yaml')); validate_spec(s); print('✅ OpenAPI spec valid')"

docs-openapi-spectral: ## Validate OpenAPI spec with Spectral (requires spectral)
	@echo "Running Spectral validation on OpenAPI spec..."
	@spectral lint docs/api/openapi.yaml --config .spectral.yaml || (echo "❌ Spectral validation failed" && exit 1)
	@echo "✅ Spectral validation passed"

docs-openapi-check-drift: ## Fail if OpenAPI spec differs from generated output
	python3 scripts/docs/generate_openapi.py --check-drift

docs-redoc: ## Generate Redoc HTML documentation
	python3 scripts/docs/generate_openapi.py

docs-build: ## Build documentation
	@echo "Building documentation..."
	@python scripts/docs/build_docs.py 2>/dev/null || python3 scripts/docs/build_docs.py
	@echo "Run 'make docs-serve' to preview static docs"

test-api-oas: ## Run API tests using OpenAPI spec with Schemathesis
	@echo "Running API tests with Schemathesis..."
	@schemathesis run --config .schemathesis.yaml --report-json=reports/schemathesis-results.json --report-junit=reports/junit.xml
	@echo "✅ API testing completed"

noaa-register-schemas: ## Register NOAA Avro schemas for configured topics
	@echo "Registering NOAA schemas to Schema Registry..."
	@python scripts/noaa/register_schemas.py 2>/dev/null || python3 scripts/noaa/register_schemas.py

# Canary & Chaos
canary-api: ## Run basic API canary against local Compose or cluster
	python scripts/canary/run_api_canary.py --base $${AURUM_API_BASE:-http://localhost:8095}

chaos-worker: ## Simulate worker cancellation/retry scenarios (placeholder)
	@echo "Injecting test scenarios to exercise cancellation/retry..."
	@echo "(Implement producer that sends fast-cancel + failing payloads and checks metrics)"
