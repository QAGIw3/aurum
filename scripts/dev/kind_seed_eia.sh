#!/usr/bin/env bash
set -euo pipefail

# End-to-end EIA ingestion helper for the local kind cluster.
#
# Steps:
#   1. Apply the Timescale EIA DDL
#   2. Register Avro schemas & compatibility in the Schema Registry
#   3. Run the SeaTunnel Kafka->Timescale job once
#   4. Verify the Aurum API returns at least one EIA record
#
# Environment overrides:
#   KAFKA_BOOTSTRAP            (default kafka.aurum.localtest.me:31092)
#   SCHEMA_REGISTRY_URL        (default http://schema-registry.aurum.localtest.me:8085)
#   TIMESCALE_JDBC_URL         (default jdbc:postgresql://localhost:5433/timeseries)
#   TIMESCALE_USER             (default timescale)
#   TIMESCALE_PASSWORD         (default timescale)
#   API_URL                    (default http://api.aurum.localtest.me:8085/v1/ref/eia/series?limit=1)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAKE="${MAKE:-make}"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka.aurum.localtest.me:31092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry.aurum.localtest.me:8085}"
TIMESCALE_JDBC_URL="${TIMESCALE_JDBC_URL:-jdbc:postgresql://localhost:5433/timeseries}"
TIMESCALE_USER="${TIMESCALE_USER:-timescale}"
TIMESCALE_PASSWORD="${TIMESCALE_PASSWORD:-timescale}"
API_URL="${API_URL:-http://api.aurum.localtest.me:8085/v1/ref/eia/series?limit=1}"

step() {
  printf '\n[%s] %s\n' "kind-eia" "$1" >&2
}

step "Applying Timescale EIA DDL"
"${MAKE}" -C "${ROOT_DIR}" timescale-apply-eia

step "Registering Kafka schemas"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL}" "${MAKE}" -C "${ROOT_DIR}" kafka-bootstrap

step "Running SeaTunnel job eia_series_kafka_to_timescale"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP}" \
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL}" \
TIMESCALE_JDBC_URL="${TIMESCALE_JDBC_URL}" \
TIMESCALE_USER="${TIMESCALE_USER}" \
TIMESCALE_PASSWORD="${TIMESCALE_PASSWORD}" \
"${ROOT_DIR}/scripts/seatunnel/run_job.sh" eia_series_kafka_to_timescale

step "Verifying API response from ${API_URL}"
python3 <<PY
import json
import sys
from urllib.request import urlopen, Request

url = ${API_URL!r}
try:
    with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=10) as resp:
        payload = json.load(resp)
except Exception as exc:  # noqa: BLE001
    raise SystemExit(f"failed to fetch {url}: {exc}")

data = payload.get("data")
if not data:
    raise SystemExit(f"no EIA rows returned from {url}: {payload}")
print(f"Fetched {len(data)} EIA rows. Sample series_id={data[0].get('series_id')}")
PY

step "EIA ingestion complete"
