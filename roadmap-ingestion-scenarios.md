# Ingestion Enrichment & Scenario Engine Roadmap

## Objectives

1. **Strengthen external data integrations** so ISO LMP, FX/CPI, and fuel/reference feeds land in normalized, queryable form with clear operational diagnostics.
2. **Introduce a scenario engine MVP** that captures structured assumptions, executes deterministic transforms, and exposes results via Iceberg/Trino and the `/v1/scenarios` API surface.

---

## Ingestion Enrichment

### 1. ISO LMP Normalisation
- **Location registry**: materialise a curated lookup (ISO, node id, node name, type, zone, hub, timezone). Store in `config/iso_nodes.csv`, seed into Postgres/Timescale for joins, and expose via dbt seed + reference table.
- **SeaTunnel templates**: extend `seatunnel/jobs/*_lmp_to_kafka.conf.tmpl` with consistent field aliases (`iso_code`, `location_id`, `location_name`, `location_type`, `market`, `currency`, `uom`, `interval_seconds`).
- **Downstream Timescale sync**: ensure `iso_lmp_kafka_to_timescale.conf.tmpl` runs via DAG and uses the registry to enrich records.
- **Tests**: add `tests/seatunnel/test_*.py` fixtures to validate schema assignments and the enrichment mapping.

### 2. Watermarks & Backfill
- Standardise `scripts/sql/update_watermark.py` usage across Airflow DAGs; add parameters for backfill window (e.g., `AURUM_BACKFILL_DAYS`).
- Extend each ingestion DAG to accept `backfill_days`, loop through the window, and update watermarks on success.
- Provide CLI helper (`scripts/ingest/backfill.py`) to trigger backfills via Airflow dag_run or SeaTunnel job wrappers.

### 3. Reference Feeds
- **FX & CPI**: add SeaTunnel jobs for ECB FX rates and FRED CPI; normalise currency codes and publish to Kafka topics `aurum.ref.fx.daily.v1` and `aurum.ref.cpi.monthly.v1` with Avro schemas.
- **Fuel curves**: integrate EIA natural gas / coal / CO₂ indices via SeaTunnel HTTP connectors; land to Kafka + Iceberg, join to scenario inputs later.
- **Weather/load metadata**: enrich NOAA/ISO load feeds with station metadata from the location registry.

### 4. DLQ Triage & Alerts
- Standardise DLQ schema usage (`aurum.ingest.error.v1`) in every SeaTunnel and Python helper.
- Create ClickHouse materialised view `ops.ingest_errors_latest` and a Superset dashboard summarising failures.
- Add Prometheus alert rules for DLQ volume and ingestion latency thresholds; hook into Slack via existing notifier.

---

## Scenario Engine MVP

### 1. Assumption & Driver Schema
- Extend Postgres schema (`postgres/ddl/app.sql`) with tables:
  - `scenario_driver (id, name, type, description)`
  - `scenario_assumption_value (id, scenario_id, driver_id, payload JSONB, version, created_at)`
- Driver types: `policy`, `load_growth`, `fuel_curve`, `fleet_change`.
- Define validation routines (Pydantic models) for payloads in `src/aurum/scenarios/models.py`.

### 2. Execution Model
- Async pipeline: publish `scenario.request.v1` to Kafka when `/v1/scenarios/{id}/run` is invoked.
- New Airflow DAG `run_scenario_mvp` (or a Kafka consumer) that:
  1. Loads assumptions and relevant reference data (FX, fuel curves, load forecasts).
  2. Performs deterministic transforms in dbt or pandas (start with SQL on Trino for transparency).
  3. Writes outputs to Iceberg tables `iceberg.market.scenario_output` and optional DLQ on failure.
- Capture lineage via OpenLineage events and set watermarks for scenario runs.

### 3. API Surface
- Implement endpoints:
  - `POST /v1/scenarios`: create scenario + assumptions.
  - `POST /v1/scenarios/{id}/run`: enqueue scenario run (returns run_id).
  - `GET /v1/scenarios/{id}` & `GET /v1/scenarios/{id}/runs/{run_id}`: status + links to results.
- Integrate with auth middleware to extract tenant ID and ensure runs are tenant-scoped.
- Add Redis caching for scenario metadata reads.

### 4. PPA Valuation Stub
- Extend scenario outputs with a simplified PPA cashflow calc (e.g., fixed vs market price difference) feeding `iceberg.market.ppa_valuation`.
- Provide `POST /v1/ppa/valuate` using scenario outputs and reference curves, returning cashflow summary.

### 5. Testing & Observability
- Unit tests for driver validation and scenario orchestration stubs.
- Contract tests hitting `/v1/scenarios` endpoints within docker-compose.
- Metrics: count scenario requests, successes, failures; log run durations and expose via Prometheus.

---

## Immediate Next Steps
1. Build ISO location registry (CSV + dbt seed + enrichment join) and update SeaTunnel templates to use standardised field names.
2. Implement FX/CPI ingestion templates with DLQ and watermarks; publish Avro schemas.
3. Add scenario driver schema + Pydantic validation; scaffold scenario run API endpoints and Kafka request topic.
4. Wire scenario DAG stub that writes to Iceberg, plus integration tests for the new endpoints.
5. Enhance observability (DLQ dashboard, Prometheus alerts) once new feeds are live.

## Backfill automation

- The `aurum_backfill_scenarios` DAG wraps `aurum.scripts.ingest.backfill`. Configure it via:
  - `AURUM_BACKFILL_SOURCE`: registered ingest source to update.
  - `AURUM_BACKFILL_DAYS`: rolling window of days to replay (default 7).
  - `AURUM_BACKFILL_HISTORY_LIMIT`: hard cap to prevent infinite history sweeps (default 30).
  - `AURUM_BACKFILL_COMMAND_TEMPLATE`: optional shell command rendered per day (the DAG passes `--shell` when `AURUM_BACKFILL_COMMAND_SHELL` is truthy).
  - `AURUM_BACKFILL_WATERMARK_KEY`: watermark key to bump (default `default`).

