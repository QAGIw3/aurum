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

.PHONY: api-up api-down api-built-up

api-up:
	docker compose -f compose/docker-compose.dev.yml up -d api

api-built-up:
	docker compose -f compose/docker-compose.dev.yml --profile api-built up -d api-built

api-down:
	docker compose -f compose/docker-compose.dev.yml rm -sf api api-built || true

.PHONY: worker-up worker-down

worker-up:
	docker compose -f compose/docker-compose.dev.yml --profile worker up -d scenario-worker

worker-down:
	docker compose -f compose/docker-compose.dev.yml --profile worker rm -sf scenario-worker || true

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
