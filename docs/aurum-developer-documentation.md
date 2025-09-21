# Aurum Developer Documentation

**Version:** 0.1 (developer preview)  
**Audience:** Software & Data Engineers building, operating, and extending Aurum  
**Scope:** Architecture, local + cluster setup, data model, pipelines, APIs, governance, security, testing, CI/CD, extension guides, and runbooks.

---

## Table of Contents

1. [What is Aurum?](#what-is-aurum)
2. [System Architecture](#system-architecture)
3. [Component Matrix](#component-matrix)
4. [Data Model & Contracts](#data-model--contracts)
5. [Naming Conventions](#naming-conventions)
6. [Local Development Setup](#local-development-setup)
7. [Cluster Deployment](#cluster-deployment)
8. [Data Ingestion Pipelines](#data-ingestion-pipelines)
9. [Transform & Modeling](#transform--modeling)
10. [Serving & Access](#serving--access)
11. [Governance & Lineage](#governance--lineage)
12. [Security, Tenancy & Secrets](#security-tenancy--secrets)
13. [Observability & Operations](#observability--operations)
14. [Testing Strategy](#testing-strategy)
15. [CI/CD](#cicd)
16. [Extension Guides](#extension-guides)
17. [Performance & Scalability](#performance--scalability)
18. [Disaster Recovery](#disaster-recovery)
19. [Runbooks](#runbooks)
20. [Glossary](#glossary)
21. [Appendix: DDL, Schemas & Samples](#appendix-ddl-schemas--samples)
22. [Quick Start Cheat Sheet](#quick-start-cheat-sheet)

---

## What is Aurum?

Aurum is a **governed, AI-ready market-intelligence platform** for power, fuels, and environmental commodities. It ingests daily vendor end-of-day curve workbooks (plus public and real-time feeds), validates and versions data, produces **forward curves, scenarios, and PPA valuations**, and serves results via **APIs, SQL (Trino), and files**.  
Core values: **Transparency, Reproducibility, Governance, and Speed.**

---

## System Architecture

```mermaid
flowchart LR
  subgraph Ingress
    T[Traefik]
  end
  subgraph Control
    AF[Airflow]
    OM[OpenMetadata]
    MZ[Marquez (OpenLineage)]
    VA[Vault]
  end
  subgraph Stream
    K[Kafka]
    SR[Schema Registry]
    KC[Kafka Connect]
    ST[SeaTunnel]
  end
  subgraph Storage
    S3[(MinIO S3)]
    LF[lakeFS]
    NS[Nessie (Iceberg Catalog)]
  end
  subgraph Compute
    TR[Trino]
    SP[Spark]
    CH[ClickHouse]
    TS[TimescaleDB]
    PG[Postgres]
    MY[MySQL]
    R[Redis]
  end
  subgraph Knowledge
    N4J[Neo4j]
    QD[Qdrant]
  end
  subgraph UX
    API[REST/GraphQL]
    SS[Superset]
    NB[Jupyter]
  end
  subgraph Obs
    VT[Vector]
  end

  T --> API
  API <---> PG
  API <---> TR
  API <---> R

  AF -->|Orchestrates| K
  AF --> S3
  AF --> TR
  AF --> SP
  AF --> OM
  AF --> MZ

  ST --> K
  KC --> K
  K --> TR

  S3 <---> LF
  TR <--> NS
  SP <--> NS
  NS <--> S3

  TR <--> CH
  TR <--> PG
  TR <--> MY
  TR <--> TS

  NB --> TR
  SS --> TR
  SS --> CH

  VT --> CH

  API --> N4J
  API --> QD
```

**Data lifecycle:** Ingest (files and feeds) --> Validate (Great Expectations) --> Version (lakeFS + Iceberg) --> Transform (dbt/Spark) --> Serve (APIs/SQL/files) --> Govern (OpenMetadata/Marquez) --> Observe (Vector/ClickHouse).

---

## Component Matrix

| Area            | Tech                             | Purpose                                              |
| --------------- | -------------------------------- | ---------------------------------------------------- |
| Ingress         | **Traefik**                      | TLS, routing, OIDC forward-auth                      |
| Metadata        | **Postgres**                     | Tenants, instruments, scenarios, runs, PPAs          |
| Time series     | **TimescaleDB**                  | High-resolution series (LMP, load, ops metrics)      |
| Cache           | **Redis**                        | API response cache, tokens, small reference data     |
| Stream          | **Kafka** + **Schema Registry**  | Event backbone + contracts (Avro)                    |
| Connectors      | **Kafka Connect**, **SeaTunnel** | JDBC/S3 sinks; HTTP to Kafka; public feeds           |
| Object store    | **MinIO**                        | Raw files, exports, logs                             |
| Data versioning | **lakeFS**                       | Branch/commit/merge for S3                           |
| Lake catalog    | **Nessie** (Iceberg)             | Transactional tables and time travel                 |
| Query           | **Trino**                        | Federated SQL (Iceberg, Postgres, ClickHouse, Kafka) |
| Batch compute   | **Spark**                        | Heavy ETL, backfills, ML training                    |
| OLAP            | **ClickHouse**                   | Low-latency marts, ops/log analytics                 |
| Knowledge graph | **Neo4j**                        | Instruments/locations/policies/scenarios graph       |
| Vector DB       | **Qdrant**                       | Semantic search (docs, columns, lineage)             |
| Viz             | **Superset**                     | Dashboards (market, QA, ops)                         |
| Orchestration   | **Airflow**                      | DAGs; emits OpenLineage                              |
| Catalog         | **OpenMetadata**                 | Data catalog, glossary, lineage UI                   |
| Lineage         | **Marquez**                      | OpenLineage backend                                  |
| Transform (SQL) | **dbt**                          | stg --> int --> mart models on Trino                 |
| Data quality    | **Great Expectations**           | Validation suites as pipeline gates                  |
| Notebooks       | **Jupyter**                      | Research and EDA                                     |
| Secrets         | **Vault**                        | Dynamic credentials, key/value                       |
| Experiments     | **MLflow**                       | Models, metrics, artifacts                           |
| Logs            | **Vector**                       | Ship logs to ClickHouse                              |

---

## Data Model & Contracts

### Canonical tables (Iceberg)

**`iceberg.market.curve_observation`** -- normalized long-form rows (monthly and strips)

* `asof_date` (DATE), `source_file` (STRING), `sheet_name` (STRING)
* Identity: `asset_class`, `region`, `iso`, `location`, `market`, `product`, `block`, `spark_location`
* Price shape: `price_type` (Mid/Bid/Ask), `units_raw`
* Tenor: `tenor_type` (MONTHLY/CALENDAR/SEASON/QUARTER), `contract_month` (DATE), `tenor_label` (STRING)
* Values: `value`, `bid`, `ask`, `mid`
* Lineage: `curve_key` (hash of identity), `version_hash` (file + sheet + asof), `_ingest_ts`

**`iceberg.market.scenario_output`** -- scenario curves with bands and attribution  
**`iceberg.market.ppa_valuation`** -- cashflows and risk metrics  
**`iceberg.market.qa_checks`** -- data quality results

> DDL: see [Appendix](#appendix-ddl-schemas--samples).

### Event contracts (Kafka/Avro)

* `aurum.curve.observation.v1` -- mirrors `curve_observation` rows
* `aurum.qa.result.v1` -- Great Expectations outcomes
* `aurum.scenario.request.v1` --> `aurum.scenario.output.v1` -- async scenarios
* `aurum.alert.v1` -- operational and data quality alerts

> Avro schemas: see [Appendix](#appendix-ddl-schemas--samples).

### Postgres operational schema (subset)

* `tenant(id, name)`
* `instrument(id, asset_class, iso, region, location, market, product, block, spark_location, units_raw, curve_key)`
* `curve_def(id, instrument_id, methodology, horizon_months, granularity, version)`
* `scenario(id, tenant_id, name, description)`
* `assumption(id, scenario_id, type, payload, version)`
* `model_run(id, scenario_id, curve_def_id, code_version, seed, state, version_hash)`
* `ppa_contract(id, instrument_id, terms)`
* `file_ingest_log(id, asof, path, sheet, status, details)`

---

## Naming Conventions

* **Kafka topics:** `aurum.{domain}.{entity}.v{n}` (example: `aurum.curve.observation.v1`)
* **Iceberg databases:** `market`, `ops`, `lineage`
* **dbt models:** `stg_*`, `int_*`, `mart_*`
* **S3 paths (MinIO):**

  ```
  s3://aurum/raw/vendor_curves/{asof}/*.xlsx
  s3://aurum/curated/iceberg/...      # Iceberg managed data
  s3://aurum/exports/{tenant}/asof={asof}/...
  s3://aurum/logs/vector/...
  ```

* **lakeFS branches:** `main`, `eod_{YYYYMMDD}`, `hotfix_*`, `client_release_*`
* **curve_key:** `hash(asset_class|iso|region|location|market|product|block|spark_location)`

---

## Local Development Setup

### Prerequisites

* Docker >= 24 / Compose >= 2
* Python >= 3.10, Java >= 17
* Node (if building UI), `trino` CLI (optional)
* `make`, `jq`, `curl` recommended

### Minimal Compose stack (dev)

Run a reduced set for rapid iteration:

* MinIO + lakeFS
* Nessie + Trino
* Postgres + Timescale
* Kafka + Schema Registry
* Airflow
* ClickHouse + Vector
* Redis
* (Optional) Neo4j, Qdrant for feature work

> Tip: keep Spark and Superset off for tight-loop development; enable when needed.

### Environment

Copy `.env.example` to `.env`; secrets in non-dev environments should come from **Vault**.

```bash
export AURUM_S3_ENDPOINT=http://localhost:9000
export AURUM_S3_ACCESS_KEY=minio
export AURUM_S3_SECRET_KEY=minio123
...
```
Set lakeFS credentials when using branch commits: `AURUM_LAKEFS_ENDPOINT`, `AURUM_LAKEFS_ACCESS_KEY`, `AURUM_LAKEFS_SECRET_KEY`, `AURUM_LAKEFS_REPO`.

### Bootstrapping

1. Create Iceberg schemas with Trino: `trino -f trino/ddl/iceberg_market.sql`
2. Apply Postgres, Timescale, and ClickHouse DDLs.
3. Register Avro schemas in Schema Registry with `make kafka-register-schemas` (set `SCHEMA_REGISTRY_URL` or use `--dry-run` on the underlying script if you need a preview).
4. Start Airflow; set `AURUM_EOD_ASOF=YYYY-MM-DD` in environment.

### Developer workflow

* **Python env:** run `make install` to create .venv with parser dependencies; use `make test` before committing.
* **Parsers:** modify `parsers/vendor_curves/` --> run unit tests --> run Airflow task locally (or Python script) to write into Iceberg.
* **CLI:** `python -m aurum.parsers.runner --as-of YYYY-MM-DD files/*.xlsx` to materialize canonical CSV/Parquet locally.
* **Helper:** `python scripts/ingest_daily.py --as-of YYYY-MM-DD --write-iceberg --lakefs-commit files/EOD_*.xlsx` to run the full workflow in one command.
* **Output:** set `AURUM_PARSED_OUTPUT_URI` (e.g., `s3://aurum/curated/curves`) to push parser outputs directly to MinIO/lakeFS; otherwise files land in `AURUM_PARSED_OUTPUT_DIR`.
* **Iceberg:** export `AURUM_WRITE_ICEBERG=1` (and Nessie env vars) or use `--write-iceberg` to append rows via pyiceberg.
* **dbt:** iterate on models: `dbt run -m stg,int,mart`.
* **APIs:** run local API service (FastAPI/Flask/Vert.x) against Trino and Redis.

---

## Cluster Deployment

* Kubernetes (prod/stage/dev namespaces), Helm or Kustomize per service.
* **Traefik** IngressRoutes for API, Superset, OpenMetadata, MLflow, Kafka UI.
* **Trino catalogs:** Iceberg (Nessie), Postgres, ClickHouse, Kafka, Timescale.
* **Spark** configured with Iceberg + Nessie + lakeFS (`s3a`).
* **Vault Agent Injector** for secrets.
* **Autoscaling:** Trino workers and Spark executors; ClickHouse cluster 3-node replicated; Kafka 3 brokers.

---

## Data Ingestion Pipelines

### Vendor Excel ingestion (daily EOD)

**Source:** Vendor workbooks (examples: `EOD_PW_*.xlsx`, `EOD_EUGP_*.xlsx`, `EOD_RP_*.xlsx`)  
**Flow:** MinIO --> lakeFS branch `eod_{asof}` --> Airflow DAG `ingest_vendor_curves_eod` --> Parse (Python/Spark) --> Iceberg table --> Great Expectations validate --> Merge to `main` --> Emit Kafka events --> Catalog and lineage update.

**Parsing rules (robust):**

* Detect **as-of date** anywhere in first 10 to 12 rows via regex.
* Label rows:
  * PW: `ISO:`, `Market:`, `Hours:`; units row contains currency marker; Bid/Ask row labeled `"Bid / Ask"`.
  * EUGP: `Location`, `Product`, `PEAK`; Spark Spread adds `Spark Location`.
  * RP: `Location`, `Product`.
* **Monthly:** Column 2 contains date tenors; **Strips** use textual tenors (`Calendar YYYY`, `Winter YYYY-YYYY`, `Q# YYYY`).
* **Bid/Ask split:** parse `"x / y"` robustly; `mid = (bid + ask) / 2` when Mid is not provided.
* **Identity --> `curve_key`** and **`version_hash`** computed per column group.

**DAG outline:**

* `lakefs_branch`
* `parse_pw` / `parse_eugp` / `parse_rp`
* `ge_validate`
* `openlineage_emit`
* `openmetadata_update`
* `lakefs_merge_tag`

### Public and real-time feeds

* **SeaTunnel** or **Kafka Connect** to ingest HTTP/REST or JDBC into Kafka topics. Prototype jobs live under `seatunnel/`; render and run them with `scripts/seatunnel/run_job.sh <job>` once required environment variables (including `SCHEMA_REGISTRY_URL` for Avro sinks) are exported. Current jobs cover NOAA GHCND (with station enrichment), EIA series, FRED series, CAISO PRC_LMP (helper script), ERCOT MIS (helper script), PJM day-ahead LMP ingestion, daily FX rates (scripted helper), and Kafka→Timescale landing for ISO LMP topics. All ISO LMP jobs join against the registry in `config/iso_nodes.csv` to populate zone/hub/timezone metadata.
* Keep `config/iso_nodes.csv` up to date; SeaTunnel LMP jobs join against this registry to populate zone, hub, and timezone attributes for downstream analytics.
* Store API credentials in Vault under `secret/data/aurum/<source>` (e.g., `secret/data/aurum/noaa`, `secret/data/aurum/eia`, `secret/data/aurum/aeso`, `secret/data/aurum/nepool`). Use `python scripts/secrets/push_vault_env.py --mapping NOAA_GHCND_TOKEN=secret/data/aurum/noaa:token ...` to seed Vault from a local `.env`, and `python scripts/secrets/pull_vault_env.py --mapping secret/data/aurum/noaa:token=NOAA_GHCND_TOKEN ...` to populate environment variables before running ingestion jobs.
* Need a local instance? Run `scripts/vault/run_dev.sh` to start a dev-mode Vault container on `http://localhost:8200` with root token `aurum-dev-token`.
* After Schema Registry is online, run `make kafka-register-schemas` followed by `make kafka-set-compat` to register Avro contracts and enforce `BACKWARD` compatibility across all ISO/reference subjects.
* Configure DLQ support by setting `AURUM_DLQ_TOPIC` (or passing `--dlq-topic`) so helpers publish failure diagnostics to the shared Avro subject `aurum.ingest.error.v1`.
* CAISO PRC_LMP ingestion can be driven by `scripts/ingest/caiso_prc_lmp_to_kafka.py` inside the cluster. The helper fetches zip/XML payloads, parses them, and emits Avro messages to Kafka.
* Daily FX rates from the ECB feed can be published with `python scripts/ingest/fx_rates_to_kafka.py --base EUR --symbols USD,GBP` (uses `aurum.ref.fx` Avro schema).
* TimescaleDB for real-time telemetry (LMP, load, weather).
* Trino exposes Kafka topics for live queries if needed.

---

## Transform & Modeling

### dbt on Trino

* `stg_curve_observation`: canonicalize types, compute `mid` if missing.
* `int_curve_monthly`: filter monthly, deduplicate.
* `int_curve_calendar`, `int_curve_quarter`: derive calendar-year and quarterly strips by averaging monthly curves (available for `/v1/curves/strips` once materialized).
* `mart_curve_latest`: last good as-of per instrument and tenor.
* `mart_scenario_diff`: A vs B deltas driven by variables.

### Spark jobs

* Backfills, scenario computations, feature store creation.
* Writes `scenario_output`, `ppa_valuation` (Iceberg).
* Logs experiments and metrics to **MLflow**.

### Great Expectations

* **Schema suite:** columns present, non-null keys.
* **Business suite:** `Mid` approximately equals `(Bid + Ask) / 2` within epsilon; ranges by asset_class.
* **Tenor suite:** monthly continuity, no duplicates.
* Fail-close pipeline on red; emit `aurum.alert.v1`.

---

## Serving & Access

### APIs (REST; GraphQL optional)

Key endpoints:

* `GET /v1/curves` -- slice curves by identity, tenor, as-of
* `POST /v1/scenarios` -- create scenarios (persists assumptions to Postgres when `AURUM_APP_DB_DSN` configured; falls back to in-memory otherwise)
* `POST /v1/scenarios/{id}/run` -- enqueue scenario run (stored in `model_run`, currently marked `QUEUED` until orchestration completes)
* `GET /v1/scenarios/{id}` / `GET /v1/scenarios/{id}/runs/{run_id}` -- inspect scenario metadata and run status
* `POST /v1/ppa/valuate` -- PPA cashflows and risk (scenario or ensemble aware)

> OpenAPI spec lives at `openapi/aurum.yaml`. Responses include `meta` and `data` arrays.

**Auth:** Traefik OIDC --> JWT; API enforces RBAC and tenant scoping; Redis caching for hot queries.

### SQL (Trino)

* Read `iceberg.market.curve_observation` and downstream marts.
* For tenant exposure, prefer per-tenant views (see Security) or serve via APIs only.

### Files

* Scheduled exports of Parquet or CSV to `s3://aurum/exports/{tenant}/asof=YYYY-MM-DD/...`.

### Dashboards

* **Superset** connects to Trino and ClickHouse:
  * Market Overview, Strips and Seasons, Scenario Diff, QA and Ingest, Ops Logs.

---

## Governance & Lineage

* **OpenMetadata** catalogs Trino, Iceberg, ClickHouse, and Timescale objects, owners, glossary.
* **Marquez** receives OpenLineage events from Airflow, Spark, dbt; lineage displayed in OpenMetadata.
* **lakeFS** retains raw and curated object versions; **Nessie** provides Iceberg table snapshots.

---

## Security, Tenancy & Secrets

* **Traefik + OIDC** (SSO) --> groups map to roles (`aurum-admin`, `aurum-ops`, `aurum-analyst`, `tenant-<id>`).
* **Vault** issues dynamic DB/Kafka/MinIO credentials; no static secrets in environment variables.
* **Tenancy:**
  * Trino: per-tenant schemas (views) and access control; prefer API for tenants.
  * Postgres/Timescale: row-level security policies.
  * ClickHouse: row policies and RBAC.
  * MinIO: bucket policies by prefix; lakeFS repository permissions.
  * Kafka: ACLs per service account; topics not exposed to tenants.

---

## Observability & Operations

* **Vector** ships logs to **ClickHouse** (`ops.logs`); operational events to `ops.events`.
* Superset operations dashboards: ingest latency, Great Expectations pass rate, API error rate, slow queries, Kafka lag.
* Alerts (Slack/Email/Pager) via notifier on `aurum.alert.v1` and operational thresholds.

---

## Testing Strategy

| Layer          | Tests                                                                             |
| -------------- | --------------------------------------------------------------------------------- |
| Parsers        | Unit tests with golden workbook snippets; property-based tests for Bid/Ask splitter |
| Data contracts | Avro evolution tests (compatibility)                                              |
| dbt            | Schema and uniqueness tests; snapshot tests for marts                              |
| Great Expectations | Suites run in Airflow; failure halts merge                                    |
| API            | Contract tests from OpenAPI; auth and RBAC tests                                  |
| E2E            | Ingest --> Iceberg --> dbt --> API snapshot parity                                |
| Load           | Trino and API latency under concurrent slices; Kafka throughput                    |
| Security       | Secrets scan; row-level security policy tests; ACL tests                          |

---

## CI/CD

* **GitHub Actions** (sample stages):
  1. Lint and unit tests (parsers, API)
  2. Build containers; SBOM (`syft`) and vulnerability scan (`grype`)
  3. Avro schema compatibility check
  4. `dbt compile` and run `stg` with ephemeral DuckDB (or Trino in CI)
  5. Great Expectations dry-run against a fixture
  6. Helm chart lint and Kustomize diff
  7. Deploy to `stage` --> run smoke E2E --> promote to `prod`
* **Releases:** Semantic versioning for services; data releases tagged in lakeFS (`release/asof`) and Nessie snapshot IDs stored in `model_run`.

---

## Extension Guides

### Add a new workbook format

1. Create `parsers/vendor_curves/parse_<vendor>.py`.
2. Implement adapter that yields the canonical DataFrame schema (matching `curve_observation`).
3. Register in `ingest_vendor_curves_eod` DAG.
4. Add **Great Expectations** tests tailored for this vendor; add **unit tests** with fixtures.
5. Update **OpenMetadata** descriptions if new columns or labels are introduced.

**Adapter interface (Python)**

```python
def parse(path: str, asof: date) -> pd.DataFrame:
    """
    Returns columns:
    [asof_date, source_file, sheet_name, asset_class, region, iso, location, market, product,
     block, spark_location, price_type, units_raw, tenor_type, contract_month, tenor_label,
     value, bid, ask, mid, curve_key, version_hash]
    """
```

### Add a new asset_class

* Extend enum recognition in parser and dbt; add unit mapping and Great Expectations thresholds.
* Create dedicated marts if needed; update Superset dashboards.
* Add glossary entries in OpenMetadata.

### Add a scenario driver

* Extend Postgres `assumption.type` (example: `policy`, `load_growth`, `fuel_curve`, `fleet_change`).
* Update scenario engine to map driver to transform; version assumption semantics.
* Update OpenAPI examples and add tests.

### New dbt model

* Place in `models/int/` or `models/mart/`; update `schema.yml`.
* Add tests (`unique`, `not_null`); document model in dbt docs; ingest into OpenMetadata.

---

## Performance & Scalability

* **Iceberg partitioning:** by `year(asof_date)`, `month(asof_date)`, and identities with `identity()` where selective.
* **Small files:** compact with Iceberg snapshot procedures.
* **Trino:** tune broadcast join thresholds; worker autoscale; pin frequently queried marts in ClickHouse.
* **API:** cache hot slices in Redis; pagination; GZIP; vectorized DB drivers.
* **Kafka:** partition topics by `curve_key` or `asof_date` to scale consumers.
* **Spark:** coalesce or writestream settings; adaptive query execution; use Parquet predicate pushdown.

---

## Disaster Recovery

* **MinIO:** bucket versioning + replication to DR; weekly immutable snapshots.
* **lakeFS:** mirror repositories and references to DR.
* **Postgres/Timescale:** streaming replication + nightly base backups.
* **ClickHouse:** replicated MergeTree; S3 backups.
* **Kafka:** in-sync replicas >= 2, suitable retention.
* **Runbook:** restore drills monthly; document RPO = 15m, RTO = 2h targets.

---

## Runbooks

### Great Expectations checkpoint failed (red)

1. Airflow shows `ge_validate_curves` failure --> inspect Great Expectations data docs artifact.
2. Common causes: Bid/Ask parse drift; unit mapping missing; tenor gap.
3. Roll back lakeFS branch (no merge).
4. Patch parser or mapping; re-run DAG; if vendor error, create `hotfix_*` branch.

### Trino slow queries

1. Check cluster load and splits; verify Iceberg file counts; run compaction.
2. Move hot queries to ClickHouse mart; add Redis caching in API.

### Kafka backlog

1. Inspect consumer lag; scale consumers; partition topics if needed.
2. Check Schema Registry and serialization errors.

### MinIO outage

1. Switch to DR MinIO if declared; replay from Kafka if necessary; use lakeFS snapshot to re-materialize.

---

## Glossary

* **ATC** -- Around-the-Clock (flat) block.
* **BASE/PEAK** -- European block definitions; PEAK typically business hours.
* **Curve** -- Forward price series (monthly or strip).
* **Strip** -- Aggregated tenor (Calendar, Winter, Summer, Quarter).
* **PPA** -- Power Purchase Agreement.
* **Iceberg** -- Table format with snapshots and partitioning.
* **lakeFS** -- Git-like versioning for object stores.
* **Nessie** -- Iceberg catalog service with branches and snapshots.

---

## Appendix: DDL, Schemas & Samples

* **Iceberg DDL:** `trino/ddl/iceberg_market.sql` (creates `curve_observation`, `scenario_output`, `ppa_valuation`, `qa_checks`)
* **Postgres DDL:** `postgres/ddl/app.sql` (tenants, instruments, scenarios, runs, PPAs, ingest logs, ingest_source / ingest_watermark helper functions)
* **Timescale DDL:** `timescale/ddl_timeseries.sql` (iso_lmp_timeseries, load_timeseries, ops_metrics)
* **ClickHouse DDL:** `clickhouse/ddl_ops.sql` (ops.logs, ops.events)
* **Avro Schemas:** `kafka/schemas/*.avsc` for curve, QA, scenario, alert topics
* **Trino catalogs:** `trino/catalog/iceberg.properties`, `postgres.properties`, `clickhouse.properties`
* **OpenAPI:** `openapi/aurum.yaml`
* **Airflow DAGs:** `airflow/dags/*.py` (ingest, scenarios, ppa, exports). Recent additions:
  * `ingest_public_feeds` – NOAA, EIA, FRED, PJM ingestion via SeaTunnel, registers/writes watermarks automatically.
  * `ingest_iso_prices` – CAISO & ERCOT helper scripts to Kafka with watermark tracking.
  * `ingest_iso_prices_nyiso_miso` – SeaTunnel jobs for NYISO + MISO feeds with registered metadata and watermarks.
  * `ingest_iso_prices_timescale` – consumes ISO Kafka topics into Timescale (registers source + watermark).
  * `ingest_iso_prices_caiso` – executes the CAISO PRC_LMP Python helper and pushes to Kafka.
  * `ingest_iso_prices_ercot` – runs the ERCOT MIS helper to publish LMP records into Kafka.
  * `ingest_iso_prices_miso` – fetches MISO day-ahead and real-time market reports and publishes to Kafka.
  * `ingest_iso_prices_isone` – pulls ISO-NE web services LMP data and pushes to Kafka.
  * `ingest_iso_prices_spp` – downloads SPP Marketplace files (DA/RT) and publishes to Kafka.
* **dbt models:** `dbt/models/stg`, `dbt/models/int`, `dbt/models/mart`, plus `schema.yml`
* **Great Expectations suites:** `ge/expectations/curve_schema.json`, `ge/expectations/curve_business.yml` plus checkpoint
* **Vector config:** `vector/vector.toml`
* **Traefik ingress:** `traefik/ingressroute.yaml`

---

## Quick Start Cheat Sheet

### 0. Common prep (5 minutes)

- Install Docker, kind, kubectl, and Python 3.11.
- Copy `.env.example` to `.env`, generate an Airflow Fernet key, and fill in any vendor/API credentials you have. The same file now feeds both Compose and the kind overlay (via Kustomize’s secret generator), so keep it authoritative.
- Optional but helpful: `python -m pip install --upgrade pip && pip install -r requirements-dev.txt` to get CLI helpers locally.

### Path A – Docker Compose stack (~10 minutes)

1. `docker compose -f compose/docker-compose.dev.yml up -d`
2. Wait until `docker compose ps` shows all core services `healthy`.
3. Seed buckets/repos/catalogs once: `docker compose -f compose/docker-compose.dev.yml --profile bootstrap up --exit-code-from bootstrap`
4. Register Kafka schemas: `make kafka-bootstrap` (Compose exposes Schema Registry at `http://localhost:8081` by default).
5. (Optional) bring up the UI helpers: `docker compose -f compose/docker-compose.dev.yml --profile ui up -d`
6. Verify the API: `curl http://localhost:8095/health`

> Compose tip: if a container refuses to start because a port is busy, run `docker compose down` and re-run step 1—every service is mapped under the `compose/docker-compose.dev.yml` namespace so cleanup is safe.

### Path B – kind (single-node Kubernetes) (~15 minutes)

1. `make kind-up` (chains cluster creation, `kubectl apply`, bootstrap jobs, Strimzi waits, and Kafka schema registration)
2. When iterating on code, rebuild and load images straight into the cluster: `make kind-load-api` and/or `make kind-load-worker`
3. Seed schemas again if you blow away the cluster: `make kafka-bootstrap SCHEMA_REGISTRY_URL=http://schema-registry.aurum.localtest.me:8085`
4. First API call through Traefik: `curl http://api.aurum.localtest.me:8085/health`
5. Tear the stack down interactively when you are finished: `make kind-down`

> kind tips:
> - If `scripts/k8s/install_strimzi.sh` times out waiting for CRDs, rerun `make kind-apply`; the tightened retry loop prints which CRD is missing.
> - If the API Deployment never becomes ready, check `kubectl -n aurum-dev logs deployment/aurum-api`—most issues are bad DSNs or redis hostnames pulled from an outdated `.env`.

### Vault seeding (both paths)

1. Start the dev vault (Compose: `scripts/vault/run_dev.sh`; kind: `kubectl -n aurum-dev port-forward svc/vault 8200:8200`).
2. Export `VAULT_ADDR=http://127.0.0.1:8200` and `VAULT_TOKEN=aurum-dev-token` (or the dev token printed on start-up).
3. Push secrets from your `.env`: `python scripts/secrets/push_vault_env.py --mapping EIA_API_KEY=secret/data/aurum/eia:api_key --mapping FRED_API_KEY=secret/data/aurum/fred:api_key`

### Kafka bootstrap (both paths)

Run `make kafka-bootstrap` any time you recreate Kafka or Schema Registry. Override `SCHEMA_REGISTRY_URL` as shown above for kind; Compose defaults to `http://localhost:8081`.

### First full API smoke test

```bash
# Compose
curl http://localhost:8095/v1/scenarios?limit=1

# kind via Traefik (works for Compose if you proxy through Traefik as well)
curl http://api.aurum.localtest.me:8085/v1/scenarios?limit=1
```

### Common failure modes

- **Kafka bootstrap fails with HTTP 409** → schemas already registered; safe to ignore.
- **`make kind-up` stops on Strimzi CRDs** → rerun `make kind-apply`; the script now blocks until CRDs report `Established`.
- **API returns 503 for scenario outputs** → ensure Redis is reachable and that `AURUM_APP_DB_DSN` points at Postgres (set in `.env`, synced to K8s via Kustomize).
- **Vault scripts fail with `connection refused`** → confirm the port-forward (kind) or dev server (Compose) is running and `VAULT_ADDR` matches.

### Ports & hostnames at a glance

| Service       | Compose default            | kind via Traefik                |
| ------------- | -------------------------- | -------------------------------- |
| API           | `http://localhost:8095`    | `http://api.aurum.localtest.me:8085` |
| Schema Reg    | `http://localhost:8081`    | `http://schema-registry.aurum.localtest.me:8085` |
| MinIO Console | `http://localhost:9001`    | `http://minio-console.aurum.localtest.me:8085` |
| Trino         | `http://localhost:8080`    | `http://trino.aurum.localtest.me:8085` |

---

**That is the complete developer documentation for Aurum v0.1.**
