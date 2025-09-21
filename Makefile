.PHONY: install lint test format bootstrap trino-apply-iso-views timescale-apply-ddl kind-scenario-smoke seed-ingest-sources

KIND_CLUSTER_NAME ?= $(if $(AURUM_KIND_CLUSTER),$(AURUM_KIND_CLUSTER),aurum-dev)
KIND_NAMESPACE ?= aurum-dev
KIND_API_IMAGE ?= ghcr.io/aurum/aurum-api
KIND_WORKER_IMAGE ?= ghcr.io/aurum/aurum-worker

.PHONY: db-upgrade db-downgrade

db-upgrade:
	alembic upgrade head

db-downgrade:
	alembic downgrade -1

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

.PHONY: kind-create kind-delete kind-apply kind-reset kind-bootstrap kind-apply-ui kind-delete-ui kind-up kind-down kind-load-api kind-load-worker kind-scenario-smoke kafka-register-schemas kafka-set-compat kafka-apply-topics kafka-apply-topics-dry-run kafka-apply-topics-kind kafka-apply-topics-kind-dry-run minio-bootstrap

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

kind-up:
	$(MAKE) kind-create
	$(MAKE) kind-apply
	$(MAKE) kind-bootstrap
	SCHEMA_REGISTRY_URL=$${SCHEMA_REGISTRY_URL:-http://schema-registry.aurum.localtest.me:8085} $(MAKE) kafka-bootstrap

kind-down:
	@if [ "$${KIND_DOWN_FORCE:-}" != "1" ] && [ "$${KIND_DOWN_FORCE:-}" != "true" ] && [ "$${KIND_DOWN_FORCE:-}" != "yes" ]; then \
		printf "This will delete the '%s' kind cluster and remove all aurum-dev resources. Continue? [y/N] " "${KIND_CLUSTER_NAME}"; \
		read answer; \
		case "$$answer" in \
			y|Y|yes|YES) ;; \
			*) echo "Aborted."; exit 1 ;; \
		esac; \
	fi
	$(MAKE) kind-reset
	AURUM_KIND_CLUSTER=${KIND_CLUSTER_NAME} $(MAKE) kind-delete

kind-load-api:
	docker build -f Dockerfile.api -t ${KIND_API_IMAGE}:dev .
	kind load docker-image ${KIND_API_IMAGE}:dev --name "${KIND_CLUSTER_NAME}"
	kubectl -n ${KIND_NAMESPACE} set image deployment/aurum-api api=${KIND_API_IMAGE}:dev

kind-load-worker:
	docker build -f Dockerfile.worker -t ${KIND_WORKER_IMAGE}:dev .
	kind load docker-image ${KIND_WORKER_IMAGE}:dev --name "${KIND_CLUSTER_NAME}"
	kubectl -n ${KIND_NAMESPACE} set image deployment/aurum-scenario-worker worker=${KIND_WORKER_IMAGE}:dev

kind-scenario-smoke:
	python scripts/dev/kind_scenario_smoke.py

kafka-register-schemas:
	python scripts/kafka/register_schemas.py --schema-registry-url $${SCHEMA_REGISTRY_URL:-http://localhost:8081}

kafka-set-compat:
	python scripts/kafka/set_compatibility.py --schema-registry-url $${SCHEMA_REGISTRY_URL:-http://localhost:8081} --level $${COMPATIBILITY_LEVEL:-BACKWARD}

kafka-bootstrap:
	scripts/kafka/bootstrap.sh --schema-registry-url $${SCHEMA_REGISTRY_URL:-http://localhost:8081}

kafka-apply-topics:
	python scripts/kafka/manage_topics.py --bootstrap-servers $${KAFKA_BOOTSTRAP:-localhost:9092} --config $${KAFKA_TOPICS_CONFIG:-config/kafka_topics.json}

kafka-apply-topics-dry-run:
	python scripts/kafka/manage_topics.py --bootstrap-servers $${KAFKA_BOOTSTRAP:-localhost:9092} --config $${KAFKA_TOPICS_CONFIG:-config/kafka_topics.json} --dry-run

kafka-apply-topics-kind:
	KAFKA_TOPICS_CONFIG=config/kafka_topics.kind.json $(MAKE) kafka-apply-topics

kafka-apply-topics-kind-dry-run:
	KAFKA_TOPICS_CONFIG=config/kafka_topics.kind.json $(MAKE) kafka-apply-topics-dry-run

minio-bootstrap:
	python scripts/storage/bootstrap_minio.py --endpoint $${AURUM_S3_ENDPOINT:-http://localhost:9000} --access-key $${AURUM_S3_ACCESS_KEY:-minio} --secret-key $${AURUM_S3_SECRET_KEY:-minio123}

seed-ingest-sources:
	python scripts/dev/seed_ingest_sources.py $(if $(DSN),--dsn $(DSN),) --with-watermarks

.PHONY: eia-validate-config airflow-eia-vars

eia-validate-config:
	python scripts/eia/validate_config.py

airflow-eia-vars:
	python scripts/eia/set_airflow_variables.py --apply

.PHONY: airflow-apply-vars airflow-print-vars

airflow-apply-vars:
	python scripts/airflow/set_variables.py --file config/airflow_variables.json --apply

airflow-print-vars:
	python scripts/airflow/set_variables.py --file config/airflow_variables.json

.PHONY: airflow-list-vars airflow-list-aurum-vars

airflow-list-vars:
	airflow variables list || true

airflow-list-aurum-vars:
	airflow variables list | grep -E "aurum_|AURUM_" || true

trino-apply-iso-views:
	. .venv/bin/activate && \
	python scripts/trino/run_sql.py --server $${TRINO_SERVER:-http://localhost:8080} --user $${TRINO_USER:-aurum} trino/ddl/iso_lmp_views.sql

trino-apply-eia:
	python scripts/trino/run_sql.py --server $${TRINO_SERVER:-http://localhost:8080} --user $${TRINO_USER:-aurum} trino/ddl/eia_series.sql

timescale-apply-ddl:
	. .venv/bin/activate && \
	python scripts/sql/apply_file.py --dsn $${TIMESCALE_DSN:-postgresql://timescale:timescale@localhost:5433/timeseries} timescale/ddl_timeseries.sql

timescale-apply-noaa:
	. .venv/bin/activate && \
	python scripts/sql/apply_file.py --dsn $${TIMESCALE_DSN:-postgresql://timescale:timescale@localhost:5433/timeseries} timescale/ddl_noaa.sql

timescale-apply-fred:
	. .venv/bin/activate && \
	python scripts/sql/apply_file.py --dsn $${TIMESCALE_DSN:-postgresql://timescale:timescale@localhost:5433/timeseries} timescale/ddl_fred.sql

timescale-apply-cpi:
	. .venv/bin/activate && \
	python scripts/sql/apply_file.py --dsn $${TIMESCALE_DSN:-postgresql://timescale:timescale@localhost:5433/timeseries} timescale/ddl_cpi.sql

timescale-apply-eia:
	. .venv/bin/activate && \
	python scripts/sql/apply_file.py --dsn $${TIMESCALE_DSN:-postgresql://timescale:timescale@localhost:5433/timeseries} timescale/ddl_eia.sql

.PHONY: api-up api-down api-built-up

api-up:
	docker compose -f compose/docker-compose.dev.yml up -d api

api-built-up:
	docker compose -f compose/docker-compose.dev.yml --profile api-built up -d api-built

api-down:
	docker compose -f compose/docker-compose.dev.yml rm -sf api api-built || true

.PHONY: worker-up worker-down worker-built-up worker-built-down

worker-up:
	docker compose -f compose/docker-compose.dev.yml --profile worker up -d scenario-worker

worker-down:
	docker compose -f compose/docker-compose.dev.yml --profile worker rm -sf scenario-worker || true

worker-built-up:
	docker compose -f compose/docker-compose.dev.yml --profile worker-built up -d scenario-worker-built

worker-built-down:
	docker compose -f compose/docker-compose.dev.yml --profile worker-built rm -sf scenario-worker-built || true

.PHONY: test-docker
test-docker:
	scripts/dev/test_in_docker.sh

.PHONY: noaa-kafka-render noaa-kafka-run noaa-timescale-render noaa-timescale-run

# Render the NOAA GHCND → Kafka job with current environment
noaa-kafka-render:
	scripts/seatunnel/run_job.sh noaa_ghcnd_to_kafka --render-only

# Run the NOAA GHCND → Kafka job (requires Docker, Kafka, and Schema Registry)
noaa-kafka-run:
	scripts/seatunnel/run_job.sh noaa_ghcnd_to_kafka

# Render the NOAA weather → Timescale job with current environment
noaa-timescale-render:
	scripts/seatunnel/run_job.sh noaa_weather_kafka_to_timescale --render-only

# Run the NOAA weather → Timescale job (requires Docker and Timescale JDBC access)
noaa-timescale-run:
	scripts/seatunnel/run_job.sh noaa_weather_kafka_to_timescale
