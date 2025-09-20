#!/usr/bin/env bash
set -euo pipefail

# Helper to create the PJM API Airflow pool and trigger a short backfill window
# Works against the local docker-compose dev stack

COMPOSE_FILE=${COMPOSE_FILE:-compose/docker-compose.dev.yml}
SERVICE=${AIRFLOW_CLI_SERVICE:-airflow-webserver}

# Backfill window (inclusive start, inclusive end)
START=${AURUM_BACKFILL_START:-2024-01-01}
END=${AURUM_BACKFILL_END:-2024-01-01}

echo "Creating Airflow pool 'pjm_api' (size=1) via ${SERVICE}..." >&2
docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE}" \
  airflow pools set pjm_api 1 "PJM API pool (serialize calls; external rate 6/min)" || true

echo "Listing pools for verification..." >&2
docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE}" airflow pools list | cat

echo "Triggering backfill for ingest_public_feeds from ${START} to ${END}..." >&2
docker compose -f "${COMPOSE_FILE}" exec -T "${SERVICE}" \
  airflow dags backfill ingest_public_feeds -s "${START}" -e "${END}" --reset-dagruns | cat

echo "Done." >&2

