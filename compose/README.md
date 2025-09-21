# Aurum Dev Stack

This directory contains Docker Compose files for the minimal development stack described in the developer documentation. See also the onboarding guide in `docs/onboarding.md`.

The default `docker-compose.dev.yml` stands up the following core services:

- MinIO, lakeFS, Nessie (Iceberg catalog)
- Trino (coordinator) and Vector
- Postgres (operational metadata) and Timescale extension
- Kafka (single broker) with Schema Registry
- Airflow (Webserver + Scheduler + init)
- ClickHouse (single node)
- Redis (cache)

## Profiles

- `core` — baseline services (MinIO, lakeFS, Nessie, Trino, ClickHouse, Postgres, Redis, Vector, API).
- `api` — explicitly target the FastAPI container when you want to run only the service build (paired with `core`).
- `worker` — Airflow, Kafka, Schema Registry, and the async scenario worker.
- `ui` — optional Superset + Kafka UI consoles.
- `bootstrap` — one-shot seeding for MinIO buckets, lakeFS repo, Nessie namespaces, and schema registry.
- `pipeline` — build-image variants (`api-built`, `scenario-worker-built`) used by CI smoke tests.
- `ppa-smoke` — seeding helper that loads PPA sample data for API smoke tests.

Activate profiles by setting `COMPOSE_PROFILES` (comma separated). For example:

```bash
# Start the full developer core including API
COMPOSE_PROFILES=core docker compose -f compose/docker-compose.dev.yml up -d

# Add the async worker stack (Airflow + Kafka + worker)
COMPOSE_PROFILES=core,worker docker compose -f compose/docker-compose.dev.yml up -d

# Bring up UI consoles on top of the running core
COMPOSE_PROFILES=core,ui docker compose -f compose/docker-compose.dev.yml up -d

# Run the bootstrap job once after services are healthy
COMPOSE_PROFILES=core,bootstrap docker compose -f compose/docker-compose.dev.yml up bootstrap --exit-code-from bootstrap
```

## Usage

```bash
# start core stack
cp .env.example .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"  # update AIRFLOW_FERNET_KEY in .env
COMPOSE_PROFILES=core docker compose -f compose/docker-compose.dev.yml up -d

# run bootstrap helpers once (after stack is healthy)
COMPOSE_PROFILES=core,bootstrap docker compose -f compose/docker-compose.dev.yml up bootstrap --exit-code-from bootstrap

# include optional UI services
COMPOSE_PROFILES=core,ui docker compose -f compose/docker-compose.dev.yml up -d

# bootstrap Kafka schemas (idempotent)
make kafka-bootstrap

# tail structured logs (Vector ships all container logs into ClickHouse `ops.logs`)
docker compose -f compose/docker-compose.dev.yml logs vector
clickhouse-client --query "SELECT timestamp, service, level, message FROM ops.logs ORDER BY timestamp DESC LIMIT 20"

# sanity-check the API
curl http://localhost:8095/health

# stop stack
docker compose -f compose/docker-compose.dev.yml down
```

## Vault (dev) and secrets seeding

```bash
# start a local Vault dev server on :8200
scripts/vault/run_dev.sh

# seed required secrets from your env (example mappings)
export VAULT_ADDR=http://127.0.0.1:8200
export VAULT_TOKEN=aurum-dev-token
export EIA_API_KEY=your_eia_key
export FRED_API_KEY=your_fred_key
python scripts/secrets/push_vault_env.py \
  --mapping EIA_API_KEY=secret/data/aurum/eia:api_key \
  --mapping FRED_API_KEY=secret/data/aurum/fred:api_key
```
