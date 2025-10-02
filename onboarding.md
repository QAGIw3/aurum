# Aurum Onboarding

This guide gets you productive quickly, whether you prefer running everything with Docker or developing locally in a Python virtualenv.

## Prerequisites

- Docker Desktop 4.20+ (with Compose v2)
- Python 3.9+ (matches `pyproject.toml`)
- Make (optional), jq (optional)
- ClickHouse client (optional; for log queries)
- Poetry (recommended for local installs): https://python-poetry.org/docs/#installation

## 1) Clone and bootstrap

```bash
# From the repo root (aurum/)
cp .env.example .env
# Generate Fernet key for Airflow and update .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## 2) Container‑first (recommended)

Start the minimal dev stack with the API and core dependencies:

```bash
# Core services + API
COMPOSE_PROFILES=core docker compose -f compose/docker-compose.dev.yml up -d

# Run bootstrap once after services are healthy
COMPOSE_PROFILES=core,bootstrap docker compose -f compose/docker-compose.dev.yml up bootstrap --exit-code-from bootstrap

# Optional consoles (Superset, Kafka UI)
COMPOSE_PROFILES=core,ui docker compose -f compose/docker-compose.dev.yml up -d
```

Useful URLs:

- API: http://localhost:8095 (health: `/health`, readiness: `/ready`)
- Trino: http://localhost:8080
- MinIO: http://localhost:9001
- lakeFS: http://localhost:8000
- ClickHouse: http://localhost:8123
- Airflow: http://localhost:8088

Tail logs and check recent entries in ClickHouse:

```bash
# Container logs
docker compose -f compose/docker-compose.dev.yml logs -f api

# Structured logs in ClickHouse
clickhouse-client --query "SELECT timestamp, service, level, left(message, 200) FROM ops.logs ORDER BY timestamp DESC LIMIT 20"
```

Add the async worker stack (Airflow + Kafka + worker) when you need scenarios and pipelines:

```bash
COMPOSE_PROFILES=core,worker docker compose -f compose/docker-compose.dev.yml up -d
```

Smoke‑test the API:

```bash
curl "http://localhost:8095/v1/curves?limit=5"
```

## 3) Local Python development (optional)

If you want to run unit tests or iterate on Python modules outside containers.

### Using Poetry (recommended)

```bash
# Create and activate a virtualenv managed by Poetry
poetry env use 3.9
poetry install --with dev,test

# Run tests
poetry run pytest -q

# Run API locally
poetry run uvicorn aurum.api.app:app --host 0.0.0.0 --port 8095 --reload
```

### Using vanilla venv + pip (alternative)

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
# Install runtime deps
pip install .
# Install dev tooling from pinned constraints
pip install -r constraints/dev.txt

# Run tests
pytest -q
```

## 4) Common tasks

- Scenario worker on the dev stack:

  ```bash
  COMPOSE_PROFILES=core,worker docker compose -f compose/docker-compose.dev.yml up -d scenario-worker
  ```

- Bootstrap Kafka schemas (idempotent):

  ```bash
  make kafka-bootstrap
  ```

- FX rates ingestion helper (publish to Kafka):

  ```bash
  python scripts/ingest/fx_rates_to_kafka.py --base EUR --symbols USD,GBP,JPY --bootstrap localhost:9092
  ```

## 5) Configuration cheatsheet

Environment variables are loaded from `.env` for the dev stack. Notable toggles used by the API:

- `AURUM_API_CORS_ORIGINS` — comma‑separated origins (use `*` for all)
- `AURUM_API_RATE_LIMIT_RPS` / `AURUM_API_RATE_LIMIT_BURST` — per‑client limits
- `AURUM_API_GZIP_MIN_BYTES` — enable compression for larger payloads
- `AURUM_APP_DB_DSN` — Postgres DSN for scenarios (e.g. `postgresql://aurum:aurum@localhost:5432/aurum`)
- `AURUM_API_REDIS_URL` — Redis URL for caching (enables hot slice cache)
- `AURUM_API_AUTH_DISABLED` — set to `1` to disable auth locally

More details in the API docs: `docs/api/README.md` and `README.md` (Curve API section).

## 6) Where to next

- Architecture and deep dive: `docs/aurum-developer-documentation.md`
- Scenarios overview and examples: `docs/scenarios.md`
- Kubernetes dev workflow: `docs/k8s-dev.md`
- Runbooks for operations: `docs/runbooks/`
- Data contracts and schemas: `docs/schema_registry.md`, `docs/data-contracts.md`

If you hit friction, open an issue with a short repro or ping the team in Slack.

