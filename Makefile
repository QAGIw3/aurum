.PHONY: help build test deploy lint clean docker-build docker-push k8s-deploy db-migrate security-scan

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

# Docker operations
docker-build: ## Build Docker images
	docker build -f Dockerfile.api -t ghcr.io/aurum/api:latest .
	docker build -f Dockerfile.worker -t ghcr.io/aurum/aurum-api:latest .

docker-push: ## Push Docker images to registry
	docker push ghcr.io/aurum/api:latest
	docker push ghcr.io/aurum/aurum-api:latest

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

# Database operations
db-migrate: ## Run database migrations
	@echo "Running database migrations..."
	@alembic upgrade head
	@echo "✅ Database migrations completed"

db-rollback: ## Rollback database migrations
	@read -p "Enter number of migrations to rollback: " num; \
	alembic downgrade -$$num

# Security
security-scan: ## Run security scans
	bandit -r src/ -f json -o bandit-report.json
	safety check --json | jq '.[] | .vulnerability' > safety-report.json
	@echo "Security scan reports generated"

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
	docker build -f Dockerfile.worker -t ghcr.io/aurum/aurum-api:test .

ci-test: ## Run full test suite locally
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) security-scan

ci-deploy: ## Run deployment pipeline locally
	$(MAKE) build
	$(MAKE) k8s-deploy
	$(MAKE) k8s-validate

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
	@cd docs && python -m http.server 8000

docs-build: ## Build documentation
	@echo "Building documentation..."
	# Add documentation build commands here
