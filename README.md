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

Scenario outputs are now enabled on the API by default. To populate `/v1/scenarios/{id}/outputs`, start the scenario worker alongside core services:

```
COMPOSE_PROFILES=worker docker compose -f compose/docker-compose.dev.yml up -d scenario-worker
```

- Running on the kind stack? `make kind-scenario-smoke` will create a temporary tenant, enqueue a scenario run via the API, wait for the worker to flush outputs into Iceberg, and confirm `/v1/scenarios/{id}/outputs` returns data.

Set `AURUM_SCENARIO_METRICS_PORT=9500` (and optionally `AURUM_SCENARIO_METRICS_ADDR`) to expose Prometheus metrics from the worker for scraping.

Command-line helper: once the project is installed in a virtualenv, use `aurum-scenario` to manage scenarios, e.g. `aurum-scenario --tenant demo list` or `aurum-scenario --tenant demo create "Test" --assumption '{"driver_type":"policy","payload":{"policy_name":"RPS"}}'`.

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
# Alias using since_cursor (identical to cursor)
curl "http://localhost:8095/v1/curves?limit=10&since_cursor=${first}"
# Navigate backwards with prev_cursor
prev=$(curl -s "http://localhost:8095/v1/curves?limit=10&cursor=${first}" | jq -r .meta.prev_cursor)
curl "http://localhost:8095/v1/curves?limit=10&prev_cursor=${prev}"

# Dimensions for UI filters
curl "http://localhost:8095/v1/metadata/dimensions?asof=2025-09-12"

# Strip curves
curl "http://localhost:8095/v1/curves/strips?type=CALENDAR&iso=PJM&limit=10"

# Create scenario (MVP stub)
curl -X POST "http://localhost:8095/v1/scenarios" \
  -H "Content-Type: application/json" \
  -d '{
        "tenant_id": "demo-tenant",
        "name": "Test Scenario",
        "assumptions": [
          {"driver_type": "policy", "payload": {"policy_name": "RPS", "start_year": 2030}}
        ]
      }'

# Trigger scenario run
curl -X POST "http://localhost:8095/v1/scenarios/{scenario_id}/run" \
  -H "Content-Type: application/json" \
  -d '{"code_version": "v1"}'

# List scenarios and runs for the current tenant
curl "http://localhost:8095/v1/scenarios?limit=20"
curl "http://localhost:8095/v1/scenarios/{scenario_id}/runs"

# Cancel a run (e.g., when a worker is stuck)
curl -X POST "http://localhost:8095/v1/scenarios/runs/{run_id}/cancel"

# List scenario outputs (when `AURUM_API_SCENARIO_OUTPUTS_ENABLED=1`)
curl "http://localhost:8095/v1/scenarios/{scenario_id}/outputs?limit=20"

# Latest metrics per scenario
curl "http://localhost:8095/v1/scenarios/{scenario_id}/metrics/latest"

# Dimension counts for UI facets
curl "http://localhost:8095/v1/metadata/dimensions?include_counts=true"

# Readiness probe
curl -i "http://localhost:8095/ready"

# FX rates ingestion helper (publish to Kafka)
python scripts/ingest/fx_rates_to_kafka.py --base EUR --symbols USD,GBP,JPY --bootstrap localhost:9092
```

The API queries `iceberg.market.curve_observation` via Trino and caches hot slices in Redis when `AURUM_API_REDIS_URL` is set.

Observability and limits:
- Metrics at `/metrics` (Prometheus format)
- Basic per-client rate limiting; tune with `AURUM_API_RATE_LIMIT_RPS` and `AURUM_API_RATE_LIMIT_BURST`
- Optional CORS: set `AURUM_API_CORS_ORIGINS` (comma-separated, `*` allowed)
- Optional OIDC/JWT auth: set `AURUM_API_AUTH_DISABLED=0` plus `AURUM_API_OIDC_{ISSUER,AUDIENCE,JWKS_URL}`
- Optional Postgres storage for scenarios: set `AURUM_APP_DB_DSN` (e.g. `postgresql://aurum:aurum@localhost:5432/aurum`).
- GZip compression: enable response compression with `AURUM_API_GZIP_MIN_BYTES` (default 500 bytes)
- JSON access logs: emitted on logger `aurum.api.access` with fields `method,path,status,duration_ms,request_id,client_ip,tenant,subject`. Incoming `X-Request-Id` is honored and echoed.
- In-memory metadata cache: enable short-lived caching for ISO locations, Units, and EIA catalog endpoints with `AURUM_API_INMEMORY_TTL` (seconds, default 60).
- Override the EIA query base table with `AURUM_API_EIA_SERIES_TABLE` (defaults to the dbt `mart.mart_eia_series_latest` view).
- Rate limit headers: successful responses include `X-RateLimit-{Limit,Remaining,Reset}`; 429 includes `Retry-After`.
- `/ready` responds with `503` when the API cannot reach Trino; integrate with load balancer checks.
- Add `include_counts=true` to `/v1/metadata/dimensions` to receive per-dimension frequencies alongside values.
- Traefik + OAuth2 forward-auth wiring is documented in `docs/auth/oidc-forward-auth.md` (includes docker compose and Kubernetes notes).
- Pagination returns `meta.next_cursor`; supply it as either `cursor` or `since_cursor` on subsequent requests to resume iteration and avoid duplicate rows.

### Vendor Parser CLI

Parse vendor workbooks and optionally validate against an expectation suite before writing Parquet/CSV outputs.

Example (validate then write CSV):

```
python -m aurum.parsers.runner files/EOD_PW_*.xlsx \
  --as-of 2025-01-01 \
  --validate \
  --suite ge/expectations/curve_schema.json \
  --output-dir artifacts/curves --format csv
```

Notes:
- Supports vendors: PW, EUGP, RP, SIMPLE (auto-detected by filename).
- If unit strings are not explicitly mapped, the parser infers canonical currency/unit from ISO/region where possible.
- For quick inspection without writing files, use `--dry-run` to print a one-line summary (rows, distinct curves, as_of) and exit.
- To also print a summary after writing, pass `--summary`.

### Reference Metadata API

Useful for UI dropdowns and developer tooling.

- List ISO locations (filter by ISO, optional prefix on id/name):
  `GET /v1/metadata/locations?iso=PJM&prefix=AE`
- Get a specific location:
  `GET /v1/metadata/locations/{iso}/{location_id}`
- List canonical units and currencies:
  `GET /v1/metadata/units`
- List unit mappings (raw string to currency/unit):
  `GET /v1/metadata/units/mapping?prefix=USD`
- List calendars:
  `GET /v1/metadata/calendars`
- List blocks for a calendar:
  `GET /v1/metadata/calendars/{name}/blocks`
- Hours for a date/block:
  `GET /v1/metadata/calendars/{name}/hours?block=ON_PEAK&date=2024-01-02`
- Expand a block over a date range:
  `GET /v1/metadata/calendars/{name}/expand?block=ON_PEAK&start=2024-01-01&end=2024-01-02`

### Timescale Sink DAGs

Stream reference topics from Kafka into TimescaleDB with prebuilt DAGs and SeaTunnel jobs:

- EIA series → Timescale
  - DAG: `ingest_eia_series_timescale`
  - SeaTunnel template: `seatunnel/jobs/eia_series_kafka_to_timescale.conf.tmpl`
  - Variables: `aurum_kafka_bootstrap`, `aurum_schema_registry`, `aurum_timescale_jdbc`, optional `aurum_eia_topic_pattern`, `aurum_eia_series_table`
  - One‑time DDL (default DSN `postgresql://timescale:timescale@localhost:5433/timeseries`):
    - `make timescale-apply-eia` to create `eia_series_timeseries`
    - `make timescale-apply-cpi` (for CPI) and `make timescale-apply-fred` (for FRED) as needed
  - One-shot helper for the kind stack: `scripts/dev/kind_seed_eia.sh`

- FRED series → Timescale
  - DAG: `indigest_fred_series_timescale`
  - SeaTunnel template: `seatunnel/jobs/fred_series_kafka_to_timescale.conf.tmpl`
  - Make target: `make timescale-apply-fred`

- CPI series → Timescale
  - DAG: `ingest_cpi_series_timescale`
  - SeaTunnel template: `seatunnel/jobs/cpi_series_kafka_to_timescale.conf.tmpl`
  - Make target: `make timescale-apply-cpi`

See `seatunnel/README.md` for job rendering and environment variables; Airflow DAGs shell out to `scripts/seatunnel/run_job.sh` and read Timescale credentials from Vault (`secret/data/aurum/timescale:{user,password}`).

### Ingest backfills & watermarks

- Populate `ingest_source` rows (and seed `ingest_watermark` with the current timestamp) directly from `config/eia_ingest_datasets.json` / `config/eia_bulk_datasets.json` using `make seed-ingest-sources`. Pass `DSN=postgresql://...` to override the default Postgres connection.
- Run targeted backfills with `aurum/src/aurum/scripts/ingest/backfill.py`, for example:

  ```bash
  python aurum/src/aurum/scripts/ingest/backfill.py \
    --source eia_ng_storage \
    --start 2024-01-01 \
    --end 2024-01-03 \
    --command "KAFKA_BOOTSTRAP_SERVERS=kafka.aurum.localtest.me:31092 SCHEMA_REGISTRY_URL=http://schema-registry.aurum.localtest.me:8085 scripts/seatunnel/run_job.sh eia_series_to_kafka --render-only" \
    --dry-run
  ```

  Remove `--dry-run` (and switch to `--command ... --shell` as needed) to execute the ingestion job per day, then re-run without `--command` to bump watermarks only.
- Re-run `make airflow-eia-vars` after editing dataset config to ensure the scheduler has the correct variables, and inspect success using `clickhouse-client --query "SELECT count() FROM ops.logs"` or the Grafana dashboard.

Alternative: run a prebuilt API image (separate service, port 8096):

```bash
docker compose -f compose/docker-compose.dev.yml --profile api-built up -d api-built
curl "http://localhost:8096/health"
```

### Kubernetes (kind) option

Prefer to iterate against Kubernetes primitives? Follow the workflow in `docs/k8s-dev.md` to spin up the same core services inside a local kind cluster (`make kind-create && make kind-apply && make kind-bootstrap`). The Kubernetes stack now installs the Strimzi operator to run Kafka in KRaft mode alongside the Confluent Schema Registry, Airflow, ClickHouse, Vector, and the rest of the platform—and you can layer on Superset/Kafka UI/Grafana with `make kind-apply-ui`.
Re-running `make kind-create` while the cluster exists is safe—the helper exits early without touching the node. To rebuild from scratch in one go, run `AURUM_KIND_FORCE_RECREATE=true make kind-create` or call `scripts/k8s/create_kind_cluster.sh --force`.
The mounted `trino/catalog` directory ships with catalogs for Iceberg, Postgres, Timescale, Kafka, and ClickHouse so federated queries work immediately once the stack is up.

Stateful services now persist their data under `.kind-data/` on the host (Postgres, Timescale, MinIO, ClickHouse) and the Kind node publishes Traefik on `localhost:8085/8443/8095` so you can reach the core consoles at human-friendly hostnames (for example `http://minio.aurum.localtest.me:8085`, `http://airflow.aurum.localtest.me:8085`, `http://vault.aurum.localtest.me:8085`). Vault is deployed in dev mode with a deterministic root token (`kubectl -n aurum-dev get secret vault-root-token -o jsonpath='{.data.token}' | base64 --decode`) and a bootstrap job enables the Kubernetes auth method, seeds `secret/data/aurum/*`, and provisions an `aurum-ingest` role bound to the `aurum-ingest` service account.

Nightly CronJobs dump Postgres and Timescale to `.kind-data/backups/` (retaining seven days) and mirror the MinIO `aurum` bucket for quick restores. Check `k8s/base/backups.yaml` if you need to change schedules or retention windows.

Run `make kafka-bootstrap` (optionally export `SCHEMA_REGISTRY_URL`) after the Schema Registry is online to publish the shared Avro contracts and enforce `BACKWARD` compatibility in one step. Individual commands remain available via `make kafka-register-schemas` and `make kafka-set-compat` (override the level with `COMPATIBILITY_LEVEL`).
Apply topic definitions and retention policies with `make kafka-apply-topics` (preview changes via `make kafka-apply-topics-dry-run`). When you are targeting the single-node kind cluster, use the helper `make kafka-apply-topics-kind` to point at `config/kafka_topics.kind.json`, which dials partitions and replication down to one. The standard plan in `config/kafka_topics.json` seeds the canonical `aurum.curve.observation.v1` topic, its dead-letter companion `aurum.curve.observation.dlq`, and shared `aurum.ingest.error.v1` stream with sane defaults.
Bootstrap MinIO buckets, versioning, and lifecycle rules with `make minio-bootstrap` (uses the blueprint in `config/storage/minio_buckets.json`).
Set `AURUM_DLQ_TOPIC` (or pass `--dlq-topic`) when running the ingestion helpers to capture failures in `aurum.curve.observation.dlq`; generic ingestion pipelines continue to fall back to `aurum.ingest.error.v1` for structured error payloads.

**Operational follow-up**
- Register the updated Avro subjects after deploying (`iso.lmp.v1`, `iso.load.v1`, and `iso.genmix.v1` now cover AESO/MISO/SPP/CAISO). Run `make kafka-register-schemas` followed by
  `make kafka-set-compat` (or the equivalent `scripts/kafka/register_schemas.py` invocation) so Schema Registry picks up the new enum entry and topics.
- Backfill recent history per ISO by exporting the required environment variables and executing the new SeaTunnel templates, for example:
  ```bash
  # Load
  MISO_LOAD_ENDPOINT=https://example.com/miso/load \
  MISO_LOAD_INTERVAL_START=2024-01-01T00:00:00Z \
  MISO_LOAD_INTERVAL_END=2024-01-02T00:00:00Z \
  MISO_LOAD_TOPIC=aurum.iso.miso.load.v1 \
  KAFKA_BOOTSTRAP_SERVERS=broker:29092 \
  SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  scripts/seatunnel/run_job.sh miso_load_to_kafka

  # Generation mix
  SPP_GENMIX_ENDPOINT=https://example.com/spp/genmix \
  SPP_GENMIX_INTERVAL_START=2024-01-01T00:00:00Z \
  SPP_GENMIX_INTERVAL_END=2024-01-01T12:00:00Z \
  SPP_GENMIX_TOPIC=aurum.iso.spp.genmix.v1 \
  KAFKA_BOOTSTRAP_SERVERS=broker:29092 \
  SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  scripts/seatunnel/run_job.sh spp_genmix_to_kafka
  ```
- After backfills land, confirm the analytic surfaces include the new ISO codes:
  ```sql
  -- Timescale
  SELECT DISTINCT iso_code FROM public.iso_lmp_unified ORDER BY iso_code;

  -- Trino
  SELECT DISTINCT iso_code FROM iceberg.market.iso_lmp_unified ORDER BY iso_code;
  ```

Prototype SeaTunnel jobs live under `seatunnel/`; render and execute them with `scripts/seatunnel/run_job.sh` once the necessary environment variables are set (see `seatunnel/README.md`). NOAA, EIA, FRED, NYISO, PJM, and Timescale landing jobs are supported out of the box, and Python helpers cover CAISO PRC_LMP and ERCOT MIS ingestion.
EIA API coverage is catalog-driven: run `python scripts/eia/build_catalog.py` (requires `EIA_API_KEY`) to refresh `config/eia_catalog.json`, then regenerate `config/eia_ingest_datasets.json` with `python scripts/eia/generate_ingest_config.py`. The generator now infers cron schedules and sliding windows from dataset frequency so the Airflow DAG only requests slices small enough to respect the 5,000-row cap. Add or tweak dataset definitions in that JSON to control schedules, topics, window spans, filter/parameter overrides, and field expressions (see `docs/ingestion/eia_dataset_config.md` for the schema). Once updated, generate Airflow variable commands with `python scripts/eia/set_airflow_variables.py` (pass `--apply` to call `airflow variables set` directly). Bulk archives live under `config/eia_bulk_datasets.json` and are orchestrated by the `ingest_eia_bulk` DAG using the `eia_bulk_to_kafka` SeaTunnel template.
Schema registry subjects for every catalogued dataset can be registered in bulk via `python scripts/kafka/register_schemas.py --include-eia` (add `--dry-run` to preview). Apply the analytical lakehouse tables with `make trino-apply-eia`, and Timescale DDL remains available through `make timescale-apply-eia`.
Refer to `docs/api/eia.md` for example API and SQL queries against the curated outputs.

Need to replay historical windows? `python scripts/eia/backfill_series.py --job api --source eia_rto_region_daily --start 2024-01-01 --end 2024-01-07` shells out to `scripts/seatunnel/run_job.sh eia_series_to_kafka` for each day, then bumps the ingest watermark automatically. Pass `--job bulk` for one-off runs against the bulk archive definitions. If you only need to advance the watermark, the generic helper still works: `python src/aurum/scripts/ingest/backfill.py --source eia_rto_region_daily --start 2024-01-01 --end 2024-01-07`.
Use Vault for runtime secrets: store tokens under `secret/data/aurum/<source>` (seed with `python scripts/secrets/push_vault_env.py --mapping NOAA_GHCND_TOKEN=secret/data/aurum/noaa:token ...`) and populate the environment with `python scripts/secrets/pull_vault_env.py --mapping secret/data/aurum/noaa:token=NOAA_GHCND_TOKEN ...` before launching ingestion helpers. When developing against kind, the in-cluster Vault instance already exposes those paths (`vault login $(kubectl -n aurum-dev get secret vault-root-token -o jsonpath='{.data.token}' | base64 --decode)`); for standalone docker-compose workflows you can still run `scripts/vault/run_dev.sh` to launch an ad-hoc dev-mode Vault on `http://localhost:8200` (default root token `aurum-dev-token`).

- Scripted ISO helpers:
  - `scripts/ingest/caiso_prc_lmp_to_kafka.py` downloads the PRC_LMP report (zip/XML), normalizes it to the ISO LMP Avro schema, and publishes to Kafka.
  - `scripts/ingest/ercot_mis_to_kafka.py` fetches a MIS zipped CSV file and emits ERCOT LMP records.
  - `scripts/ingest/miso_marketreports_to_kafka.py` processes day-ahead or real-time Market Reports CSVs.
  - `scripts/ingest/isone_ws_to_kafka.py` calls ISO-NE web services with start/end windows and publishes LMP data.
  - `scripts/ingest/spp_file_api_to_kafka.py` lists and downloads SPP Marketplace files, normalizes them, and sends to Kafka.
  - Airflow Variables for the AESO SMP DAG: `aurum_aeso_endpoint` (default `https://api.aeso.ca/report/v1/price/systemMarginalPrice`) and `aurum_aeso_topic`
    (default `aurum.iso.aeso.lmp.v1`). Provide an `AESO_API_KEY` via Vault or environment variables when required by the AESO API.
  - New SeaTunnel jobs handle ISO load and generation mix ingestion: `miso_load_to_kafka`, `spp_load_to_kafka`, `caiso_load_to_kafka`, `aeso_load_to_kafka`, and
    their `*_genmix_to_kafka` counterparts. The companion Airflow DAGs (`ingest_iso_metrics_miso`, `ingest_iso_metrics_spp`, `ingest_iso_metrics_caiso`,
    `ingest_iso_metrics_aeso`) expect variables such as `aurum_<iso>_load_endpoint`, `aurum_<iso>_load_topic`, `aurum_<iso>_genmix_endpoint`, and
    `aurum_<iso>_genmix_topic`. For AESO supply `aurum_aeso_api_key` to populate the required `X-API-Key` header.
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

### Airflow Variables Matrix

Apply a standard set of Airflow Variables (Kafka bootstrap, Schema Registry URL, Timescale JDBC, topic names/patterns, and sink table names) from a single JSON mapping:

- Mapping file: `config/airflow_variables.json`
- Preview commands: `make airflow-print-vars`
- Validate mapping: `make airflow-check-vars`
- Apply to Airflow: `make airflow-apply-vars`

These variables are consumed by the public feeds DAGs and the Kafka→Timescale sink DAGs to avoid hardcoding endpoints and names.

Variables overview (defaults for dev):

- Endpoints
  - `aurum_kafka_bootstrap`: Kafka brokers (default `kafka:9092`)
  - `aurum_schema_registry`: Schema Registry (default `http://schema-registry:8081`)
  - `aurum_timescale_jdbc`: Timescale JDBC (default `jdbc:postgresql://timescale:5432/timeseries`)

- NOAA
  - `aurum_noaa_topic`: Kafka topic for NOAA GHCND (`aurum.ref.noaa.weather.v1`)
  - `aurum_noaa_timescale_table`: Timescale table (`noaa_weather_timeseries`)

- EIA
  - `aurum_eia_topic`: Base Kafka topic (`aurum.ref.eia.series.v1`)
  - `aurum_eia_topic_pattern`: Pattern for sink (`aurum\.ref\.eia\..*\.v1`)
  - `aurum_eia_series_table`: Timescale table (`eia_series_timeseries`)

- FRED
  - `aurum_fred_topic`: Base Kafka topic (`aurum.ref.fred.series.v1`)
  - `aurum_fred_topic_pattern`: Pattern for sink (`aurum\.ref\.fred\..*\.v1`)
  - `aurum_fred_series_table`: Timescale table (`fred_series_timeseries`)

- CPI (FRED)
  - `aurum_cpi_topic`: Base Kafka topic (`aurum.ref.cpi.series.v1`)
  - `aurum_cpi_topic_pattern`: Pattern for sink (`aurum\.ref\.cpi\..*\.v1`)
  - `aurum_cpi_series_table`: Timescale table (`cpi_series_timeseries`)

- ISO LMP (shared)
  - `aurum_iso_lmp_topic_pattern`: Pattern for ISO LMP topics (`aurum\.iso\..*\.lmp\.v1`)

Helpful targets:
- `make airflow-print-vars` (prints apply commands for the matrix)
- `make airflow-apply-vars` (applies variables)
- `make airflow-list-vars` (lists current Airflow variables)
- `make airflow-list-aurum-vars` (filters for aurum_ keys)

### PJM Data Miner integration

- Airflow DAGs and tasks:
  - Public feeds DAG adds PJM Day-Ahead LMP (`pjm_lmp_to_kafka`), Load (`pjm_load_to_kafka`), and Generation Mix (`pjm_genmix_to_kafka`).
  - Separate DAG `ingest_pjm_pnodes.py` ingests PNODES metadata periodically.
- Rate limiting:
  - Create an Airflow pool named `pjm_api` with size 1. This serializes PJM API calls across tasks and helps respect the published rate limits (example: 6 requests per minute).
- Variables (Airflow > Admin > Variables):
  - `aurum_pjm_topic` (default `aurum.iso.pjm.lmp.v1`)
  - `aurum_pjm_load_endpoint` (default `https://api.pjm.com/api/v1/inst_load`)
  - `aurum_pjm_load_topic` (default `aurum.iso.pjm.load.v1`)
  - `aurum_pjm_genmix_endpoint` (default `https://api.pjm.com/api/v1/gen_by_fuel`)
  - `aurum_pjm_genmix_topic` (default `aurum.iso.pjm.genmix.v1`)
  - `aurum_pjm_pnodes_endpoint` (default `https://api.pjm.com/api/v1/pnodes`)
  - `aurum_pjm_pnodes_topic` (default `aurum.iso.pjm.pnode.v1`)
  - `aurum_pjm_row_limit` (default `10000`)
  - `aurum_schema_registry` (default `http://localhost:8081` in dev)
- Secrets (optional):
  - Store a PJM token under `secret/data/aurum/pjm:token` in Vault. Jobs will pick it up automatically via the DAGs. If not provided, jobs will attempt to call public endpoints where permitted.

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
The runner enriches currency/unit metadata via the shared mapper, automatically shunts rows that miss core quality checks (missing currency, invalid tenor, etc.) into a quarantine dataset, and writes a JSONL DLQ payload mirroring `aurum.ingest.error.v1`. Override destinations with `--quarantine-dir`, `--quarantine-format`, and `--dlq-json-dir` (or disable the DLQ file via `--no-dlq-json`).
Operational details and remediation steps live in `docs/runbooks/vendor_ingestion.md`.

### S3 output

Set `AURUM_PARSED_OUTPUT_URI` (e.g., `s3://aurum/curated/curves`) and standard MinIO credentials to have the runner or Airflow DAG upload canonical outputs directly to object storage instead of the local filesystem.
Generate representative datasets for load tests via `python scripts/parsers/generate_synthetic_curves.py --output artifacts/synthetic.parquet` and replay historical drops with `python scripts/ingest/backfill_curves.py --start 2025-01-01 --end 2025-01-05 --vendor pw --vendor eugp` (pass extra `--runner-arg` flags such as `--write-iceberg` to control behavior).

Set `AURUM_WRITE_ICEBERG=1` or pass `--write-iceberg` to publish parsed rows directly into the configured Iceberg table (requires `pip install "aurum[iceberg]"`).

Environment variables: `AURUM_LAKEFS_ENDPOINT`, `AURUM_LAKEFS_ACCESS_KEY`, `AURUM_LAKEFS_SECRET_KEY`, and `AURUM_LAKEFS_REPO` must be exported when enabling lakeFS integration.
