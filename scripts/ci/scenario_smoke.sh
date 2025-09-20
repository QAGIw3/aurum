#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
COMPOSE_FILE="$ROOT_DIR/compose/docker-compose.dev.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")

cleanup() {
  set +e
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1
}
trap cleanup EXIT

SCENARIO_WORKER_SERVICE=${SCENARIO_WORKER_SERVICE:-scenario-worker}
SERVICES=(postgres nessie kafka schema-registry trino api "${SCENARIO_WORKER_SERVICE}")

BOOTSTRAP=${SCENARIO_SMOKE_BOOTSTRAP:-localhost:29092}
SCHEMA_REGISTRY_URL=${SCENARIO_SMOKE_SCHEMA_REGISTRY:-http://localhost:8081}
API_BASE=${SCENARIO_SMOKE_API_BASE:-http://localhost:8095}
TENANT_ID=${SCENARIO_SMOKE_TENANT:-00000000-0000-0000-0000-000000000001}
TENANT_HEADER="X-Aurum-Tenant"
"${COMPOSE[@]}" up -d "${SERVICES[@]}" --wait

echo "Waiting for scenario worker to be ready..."
sleep 15

echo "Waiting for API readiness..."
for _ in {1..30}; do
  if curl -sf "${API_BASE}/health" >/dev/null; then
    break
  fi
  sleep 2
done

export PYTHONPATH="$ROOT_DIR/src"
SCENARIO_OUTPUT=$(python "$ROOT_DIR/scripts/dev/scenario_smoke.py" \
  --bootstrap "$BOOTSTRAP" \
  --schema-registry "$SCHEMA_REGISTRY_URL" \
  --tenant-id "$TENANT_ID")
SCENARIO_ID=$(echo "$SCENARIO_OUTPUT" | awk -F'scenario_id=' 'NF>1 {print $2}' | tr -d '[:space:]')

if [[ -z "$SCENARIO_ID" ]]; then
  echo "Failed to capture scenario_id from smoke producer output" >&2
  exit 1
fi

echo "Scenario request produced with id ${SCENARIO_ID}. Waiting for worker output..."
sleep 20

QUERY="SELECT COUNT(*) FROM iceberg.market.scenario_output_latest_by_metric WHERE scenario_id='${SCENARIO_ID}'"
RESULT=$("${COMPOSE[@]}" exec -T trino trino --output-format TSV_HEADER --execute "$QUERY" | tail -n +2 | tr -d '\r')

if ! [[ "$RESULT" =~ ^[0-9]+$ ]]; then
  echo "Unexpected Trino response: $RESULT" >&2
  exit 1
fi

if [[ "$RESULT" -lt 1 ]]; then
  echo "Scenario smoke test failed: no outputs were written for ${SCENARIO_ID}" >&2
  exit 1
fi

echo "Scenario smoke test succeeded for scenario ${SCENARIO_ID} (${RESULT} rows)."

echo "Validating API responses..."

row_count=-1
for _ in {1..12}; do
  if OUTPUT_JSON=$(curl -sf -H "${TENANT_HEADER}: ${TENANT_ID}" "${API_BASE}/v1/scenarios/${SCENARIO_ID}/outputs?limit=1"); then
    row_count=$(python3 <<'PY'
import json, sys
try:
    payload = json.load(sys.stdin)
except json.JSONDecodeError:
    print(-1)
else:
    data = payload.get("data") or []
    print(len(data))
PY
<<< "$OUTPUT_JSON")
    if [[ "$row_count" -gt 0 ]]; then
      break
    fi
  fi
  sleep 5
done

if [[ "$row_count" -le 0 ]]; then
  echo "API outputs endpoint returned no data for scenario ${SCENARIO_ID}" >&2
  echo "$OUTPUT_JSON"
  exit 1
fi

metric_count=-1
for _ in {1..12}; do
  if METRIC_JSON=$(curl -sf -H "${TENANT_HEADER}: ${TENANT_ID}" "${API_BASE}/v1/scenarios/${SCENARIO_ID}/metrics/latest"); then
    metric_count=$(python3 <<'PY'
import json, sys
try:
    payload = json.load(sys.stdin)
except json.JSONDecodeError:
    print(-1)
else:
    data = payload.get("data") or []
    print(len(data))
PY
<<< "$METRIC_JSON")
    if [[ "$metric_count" -gt 0 ]]; then
      break
    fi
  fi
  sleep 5
done

if [[ "$metric_count" -le 0 ]]; then
  echo "API metrics endpoint returned no data for scenario ${SCENARIO_ID}" >&2
  echo "$METRIC_JSON"
  exit 1
fi

echo "API validation succeeded: outputs=${row_count}, metrics=${metric_count}."
