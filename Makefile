.PHONY: install lint test format bootstrap trino-apply-iso-views timescale-apply-ddl

install:
	python -m venv .venv && . .venv/bin/activate && pip install --upgrade pip && pip install -r requirements-dev.txt

test:
	. .venv/bin/activate && pytest

lint:
	. .venv/bin/activate && ruff check src tests

format:
	. .venv/bin/activate && ruff check --fix src tests

bootstrap:
	docker compose -f compose/docker-compose.dev.yml --profile bootstrap up --exit-code-from bootstrap

.PHONY: kind-create kind-delete kind-apply kind-reset kind-bootstrap kind-apply-ui kind-delete-ui kafka-register-schemas kafka-set-compat

kind-create:
	scripts/k8s/create_kind_cluster.sh

kind-delete:
	scripts/k8s/destroy_kind_cluster.sh

kind-apply:
	scripts/k8s/apply.sh

kind-reset:
	scripts/k8s/delete.sh

kind-bootstrap:
	scripts/k8s/bootstrap.sh

kind-apply-ui:
	scripts/k8s/apply_ui.sh

kind-delete-ui:
	scripts/k8s/delete_ui.sh

kafka-register-schemas:
	python scripts/kafka/register_schemas.py --schema-registry-url $${SCHEMA_REGISTRY_URL:-http://localhost:8081}

kafka-set-compat:
	python scripts/kafka/set_compatibility.py --schema-registry-url $${SCHEMA_REGISTRY_URL:-http://localhost:8081} --level $${COMPATIBILITY_LEVEL:-BACKWARD}

kafka-bootstrap:
	scripts/kafka/bootstrap.sh --schema-registry-url $${SCHEMA_REGISTRY_URL:-http://localhost:8081}

trino-apply-iso-views:
	. .venv/bin/activate && \
	python scripts/trino/run_sql.py --server $${TRINO_SERVER:-http://localhost:8080} --user $${TRINO_USER:-aurum} trino/ddl/iso_lmp_views.sql

timescale-apply-ddl:
	. .venv/bin/activate && \
	python scripts/sql/apply_file.py --dsn $${TIMESCALE_DSN:-postgresql://timescale:timescale@localhost:5433/timeseries} timescale/ddl_timeseries.sql

.PHONY: api-up api-down api-built-up

api-up:
	docker compose -f compose/docker-compose.dev.yml up -d api

api-built-up:
	docker compose -f compose/docker-compose.dev.yml --profile api-built up -d api-built

api-down:
	docker compose -f compose/docker-compose.dev.yml rm -sf api api-built || true

.PHONY: test-docker
test-docker:
	scripts/dev/test_in_docker.sh
