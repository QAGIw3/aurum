# Aurum

Developer-oriented scaffolding for the Aurum market intelligence platform. Start with the detailed [Aurum Developer Documentation](docs/aurum-developer-documentation.md) for architecture, data contracts, and operational guidance.

## Repository layout

- `airflow/` - DAG scaffolding for ingestion and orchestration flows.
- `dbt/` - dbt models, sources, and tests for curve transformations.
- `kafka/schemas/` - Avro schemas for Kafka topics.
- `trino/ddl/` - Iceberg DDL for canonical market tables.
- `postgres/ddl/`, `timescale/`, `clickhouse/` - Operational data stores.
- `parsers/` - Vendor workbook adapters.
- `openapi/` - API contract for serving curve and valuation data.
- `ge/` - Great Expectations suites used by pipelines.
- `vector/`, `traefik/` - Observability and ingress configuration.

## Next steps

1. Implement vendor parser logic inside `parsers/vendor_curves/` and connect to the Airflow tasks.
2. Flesh out dbt models and add seeds/fixtures for local development.
3. Add container healthchecks and optional UI services (Superset, Kafka UI) on top of the dev stack.

## Dev stack

1. Copy `.env.example` to `.env` and populate secrets (generate a Fernet key with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`).
2. Start the minimal platform: `docker compose -f compose/docker-compose.dev.yml up -d`.
3. Run the bootstrap helper once to create buckets/repos: `docker compose -f compose/docker-compose.dev.yml --profile bootstrap up --exit-code-from bootstrap`.
4. Access consoles: MinIO `http://localhost:9001`, lakeFS `http://localhost:8000`, Airflow `http://localhost:8088`, Trino `http://localhost:8080`, ClickHouse `http://localhost:8123`.
5. Enable optional UIs with `docker compose -f compose/docker-compose.dev.yml --profile ui up -d`.
6. When finished, stop the stack with `docker compose -f compose/docker-compose.dev.yml down` (add `--volumes` to reset state).

### Curve API

Spin up the API service (exposed on localhost:8095) alongside core services:

```bash
docker compose -f compose/docker-compose.dev.yml up -d api
curl "http://localhost:8095/v1/curves?iso=PJM&market=DA&block=ON_PEAK&limit=10"

# Pagination with offset
curl "http://localhost:8095/v1/curves?iso=PJM&limit=50&offset=50"

# Diff two as-of dates
curl "http://localhost:8095/v1/curves/diff?asof_a=2025-09-11&asof_b=2025-09-12&iso=PJM&market=DA&block=ON_PEAK&limit=10"

# Cursor-based pagination
first=$(curl -s "http://localhost:8095/v1/curves?limit=10" | jq -r .meta.next_cursor)
curl "http://localhost:8095/v1/curves?limit=10&cursor=${first}"

# Dimensions for UI filters
curl "http://localhost:8095/v1/metadata/dimensions?asof=2025-09-12"

# Strip curves
curl "http://localhost:8095/v1/curves/strips?type=CALENDAR&iso=PJM&limit=10"
```

The API queries `iceberg.market.curve_observation` via Trino and caches hot slices in Redis when `AURUM_API_REDIS_URL` is set.

Observability and limits:
- Metrics at `/metrics` (Prometheus format)
- Basic per-client rate limiting; tune with `AURUM_API_RATE_LIMIT_RPS` and `AURUM_API_RATE_LIMIT_BURST`
- Optional CORS: set `AURUM_API_CORS_ORIGINS` (comma-separated, `*` allowed)
- Optional OIDC/JWT auth: set `AURUM_API_AUTH_DISABLED=0` plus `AURUM_API_OIDC_{ISSUER,AUDIENCE,JWKS_URL}`

Alternative: run a prebuilt API image (separate service, port 8096):

```bash
docker compose -f compose/docker-compose.dev.yml --profile api-built up -d api-built
curl "http://localhost:8096/health"
```

### Kubernetes (kind) option

Prefer to iterate against Kubernetes primitives? Follow the workflow in `docs/k8s-dev.md` to spin up the same core services inside a local kind cluster (`make kind-create && make kind-apply && make kind-bootstrap`). The Kubernetes stack now installs the Strimzi operator to run Kafka in KRaft mode alongside Apicurio Registry, Airflow, ClickHouse, Vector, and the rest of the platform—and you can layer on Superset/Kafka UI with `make kind-apply-ui`.
Re-running `make kind-create` while the cluster exists is safe—the helper exits early without touching the node. To rebuild from scratch in one go, run `AURUM_KIND_FORCE_RECREATE=true make kind-create` or call `scripts/k8s/create_kind_cluster.sh --force`.
The mounted `trino/catalog` directory ships with catalogs for Iceberg, Postgres, Timescale, Kafka, and ClickHouse so federated queries work immediately once the stack is up.

Stateful services now persist their data under `.kind-data/` on the host (Postgres, Timescale, MinIO, ClickHouse) and the Kind node publishes Traefik on `localhost:8085/8443/8095` so you can reach the core consoles at human-friendly hostnames (for example `http://minio.aurum.localtest.me:8085`, `http://airflow.aurum.localtest.me:8085`, `http://vault.aurum.localtest.me:8085`). Vault is deployed in dev mode with a deterministic root token (`kubectl -n aurum-dev get secret vault-root-token -o jsonpath='{.data.token}' | base64 --decode`) and a bootstrap job enables the Kubernetes auth method, seeds `secret/data/aurum/*`, and provisions an `aurum-ingest` role bound to the `aurum-ingest` service account.

Nightly CronJobs dump Postgres and Timescale to `.kind-data/backups/` (retaining seven days) and mirror the MinIO `aurum` bucket for quick restores. Check `k8s/base/backups.yaml` if you need to change schedules or retention windows.

Run `make kafka-bootstrap` (optionally export `SCHEMA_REGISTRY_URL`) after the Schema Registry is online to publish the shared Avro contracts and enforce `BACKWARD` compatibility in one step. Individual commands remain available via `make kafka-register-schemas` and `make kafka-set-compat` (override the level with `COMPATIBILITY_LEVEL`).
Set `AURUM_DLQ_TOPIC` (or pass `--dlq-topic`) when running the ingestion helpers to capture failures in `aurum.ingest.error.v1` alongside the ingest attempt metadata.

Prototype SeaTunnel jobs live under `seatunnel/`; render and execute them with `scripts/seatunnel/run_job.sh` once the necessary environment variables are set (see `seatunnel/README.md`). NOAA, EIA, FRED, NYISO, PJM, and Timescale landing jobs are supported out of the box, and Python helpers cover CAISO PRC_LMP and ERCOT MIS ingestion.
Use Vault for runtime secrets: store tokens under `secret/data/aurum/<source>` (seed with `python scripts/secrets/push_vault_env.py --mapping NOAA_GHCND_TOKEN=secret/data/aurum/noaa:token ...`) and populate the environment with `python scripts/secrets/pull_vault_env.py --mapping secret/data/aurum/noaa:token=NOAA_GHCND_TOKEN ...` before launching ingestion helpers. When developing against kind, the in-cluster Vault instance already exposes those paths (`vault login $(kubectl -n aurum-dev get secret vault-root-token -o jsonpath='{.data.token}' | base64 --decode)`); for standalone docker-compose workflows you can still run `scripts/vault/run_dev.sh` to launch an ad-hoc dev-mode Vault on `http://localhost:8200` (default root token `aurum-dev-token`).

- Scripted ISO helpers:
  - `scripts/ingest/caiso_prc_lmp_to_kafka.py` downloads the PRC_LMP report (zip/XML), normalizes it to the ISO LMP Avro schema, and publishes to Kafka.
  - `scripts/ingest/ercot_mis_to_kafka.py` fetches a MIS zipped CSV file and emits ERCOT LMP records.
  - `scripts/ingest/miso_marketreports_to_kafka.py` processes day-ahead or real-time Market Reports CSVs.
  - `scripts/ingest/isone_ws_to_kafka.py` calls ISO-NE web services with start/end windows and publishes LMP data.
  - `scripts/ingest/spp_file_api_to_kafka.py` lists and downloads SPP Marketplace files, normalizes them, and sends to Kafka.
  ```bash
  SCHEMA_REGISTRY_URL=http://localhost:8081 \
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  python scripts/ingest/caiso_prc_lmp_to_kafka.py \
    --start 2024-01-01T00:00-0000 \
    --end 2024-01-01T01:00-0000 \
    --market RTPD

  python scripts/ingest/ercot_mis_to_kafka.py \
    --url https://mis.ercot.com/path/to/report.zip

  python scripts/ingest/miso_marketreports_to_kafka.py \
    --url https://docs.misoenergy.org/marketreports/20240101_da_expost_lmp.csv \
    --market DA

  python scripts/ingest/isone_ws_to_kafka.py \
    --url https://webservices.iso-ne.com/api/v1/lmp/5min \
    --start 2024-01-01T00:00:00Z --end 2024-01-01T01:00:00Z --market RT \
    --username USER --password PASS

  python scripts/ingest/spp_file_api_to_kafka.py \
    --base-url https://marketplace.spp.org/file-api \
    --report RTBM-LMPBYLP --date 2024-01-01
  ```


## Python tooling

- Install dependencies into a virtualenv: `make install` (uses `.venv`).
- Run unit tests: `make test`.
- Run tests inside Docker (Python 3.11): `make test-docker`.
- Run targeted suites without installing the package: `PYTHONPATH=src python -m pytest tests/parsers`.
- Lint/format with Ruff: `make lint` / `make format`.
- Enable pre-commit hooks: `pip install pre-commit && pre-commit install`.
- Parser utilities live under `src/aurum/parsers`; add new vendor adapters in `vendor_curves/` and cover with tests in `tests/parsers/`.

## Vendor parsing CLI

Run multiple vendors with the helper script:

```bash
python scripts/ingest_daily.py --as-of 2025-09-12 --write-iceberg --lakefs-commit files/EOD_*.xlsx
```


Use the runner to convert raw workbooks into the canonical schema.

```bash
python -m aurum.parsers.runner --as-of 2025-09-12 --format csv files/EOD_PW_20250912_1430.xlsx
```

Outputs are written to `./artifacts` by default.

### S3 output

Set `AURUM_PARSED_OUTPUT_URI` (e.g., `s3://aurum/curated/curves`) and standard MinIO credentials to have the runner or Airflow DAG upload canonical outputs directly to object storage instead of the local filesystem.

Set `AURUM_WRITE_ICEBERG=1` or pass `--write-iceberg` to publish parsed rows directly into the configured Iceberg table (requires `pip install "aurum[iceberg]"`).

Environment variables: `AURUM_LAKEFS_ENDPOINT`, `AURUM_LAKEFS_ACCESS_KEY`, `AURUM_LAKEFS_SECRET_KEY`, and `AURUM_LAKEFS_REPO` must be exported when enabling lakeFS integration.
