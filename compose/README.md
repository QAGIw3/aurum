# Aurum Dev Stack

This directory contains Docker Compose files for the minimal development stack described in the developer documentation. The default `docker-compose.dev.yml` stands up the following core services:

- MinIO, lakeFS, Nessie (Iceberg catalog)
- Trino (coordinator) and Vector
- Postgres (operational metadata) and Timescale extension
- Kafka (single broker) with Schema Registry
- Airflow (Webserver + Scheduler + init)
- ClickHouse (single node)
- Redis (cache)

## Profiles

- `ui` - enables optional Superset and Kafka UI consoles (`--profile ui`).
- `bootstrap` - runs a one-shot initializer that creates the MinIO bucket, lakeFS repository, and Nessie namespaces (`--profile bootstrap`).

## Usage

```bash
# start core stack
cp compose/.env.example .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"  # update AIRFLOW_FERNET_KEY in .env
docker compose -f compose/docker-compose.dev.yml up -d

# run bootstrap helpers once (after stack is healthy)
docker compose -f compose/docker-compose.dev.yml --profile bootstrap up --exit-code-from bootstrap

# include optional UI services
docker compose -f compose/docker-compose.dev.yml --profile ui up -d

# bootstrap Kafka schemas (idempotent)
make kafka-bootstrap

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
