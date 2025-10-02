# Airflow Guide

This guide summarizes Airflow DAGs, variables, and operational tips for Aurum’s ingestion and monitoring flows.

## Overview

- Location: airflow/dags/
- Profiles: Dev stacks bring up Airflow via docker compose (see README.md). Kubernetes setups can deploy the chart/manifest you use internally.
- Observability: Vector ships task logs to ClickHouse; use Grafana dashboards for SLA and error tracking.

## DAGs

External Data (Kafka → Timescale optional):
- EIA: `ingest_eia_series_timescale` (sink), sources publish via SeaTunnel job templates (see seatunnel/README.md)
- FRED: `ingest_fred_series_timescale`
- CPI: `ingest_cpi_series_timescale`

ISO Market Data:
- PJM LMP: `pjm_lmp_to_kafka`
- PJM Load: `pjm_load_to_kafka`
- PJM Generation Mix: `pjm_genmix_to_kafka`
- PJM PNODES: `ingest_pjm_pnodes`

Monitoring:
- NOAA monitoring (15‑min): `noaa_data_monitoring`
- NOAA monitoring (daily): `noaa_data_monitoring_daily`
  - See airflow/dags/noaa_monitoring_dag.py for tasks and thresholds.

## Variables and Pools

- Pools (example): create `pjm_api` with size 1 to serialize calls per PJM’s rate policy.
- Variables (examples):
  - Kafka/Schema Registry: `aurum_kafka_bootstrap`, `aurum_schema_registry`
  - Timescale JDBC: `aurum_timescale_jdbc`
  - PJM endpoints/topics: `aurum_pjm_*` (see README.md PJM section)
  - NOAA token: `aurum_noaa_api_token` (used by monitoring and ingestion when needed)

Helpers:
- List variables: `make airflow-list-vars`
- Apply variables from config: `make airflow-apply-vars`
- Print apply commands: `make airflow-print-vars`

See also: airflow-variables.md for a consolidated variable reference.

## SeaTunnel Jobs

Airflow DAGs shell out to seatunnel jobs via scripts/seatunnel/run_job.sh. See seatunnel/README.md for:
- Rendering templates (`--render-only` to review)
- Required env for each job
- Vault secret loading helpers

## Dev Tips

- Use the “render only” flow to verify job configs before execution.
- After editing dataset configs, re‑apply Airflow Variables (see helpers above).
- Check recent logs in ClickHouse: `clickhouse-client --query "SELECT * FROM ops.logs ORDER BY timestamp DESC LIMIT 50"`
