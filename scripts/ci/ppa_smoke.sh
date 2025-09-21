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

export COMPOSE_PROFILES=${COMPOSE_PROFILES:-ppa-smoke}

TENANT_ID=${PPA_SMOKE_TENANT_ID:-00000000-0000-0000-0000-000000000001}
SCENARIO_ID=${PPA_SMOKE_SCENARIO_ID:-11111111-1111-1111-1111-111111111111}
CONTRACT_ID=${PPA_SMOKE_CONTRACT_ID:-22222222-2222-2222-2222-222222222222}
API_BASE=${PPA_SMOKE_API_BASE:-http://localhost:8095}
TENANT_HEADER=${PPA_SMOKE_TENANT_HEADER:-X-Aurum-Tenant}
SCENARIO_VALUE=${PPA_SMOKE_SCENARIO_VALUE:-72.5}
PPA_PRICE=${PPA_SMOKE_PPA_PRICE:-50.0}
VOLUME=${PPA_SMOKE_VOLUME_MWH:-10.0}
UPFRONT=${PPA_SMOKE_UPFRONT_COST:-100.0}
ASOF_DATE=${PPA_SMOKE_ASOF_DATE:-2024-01-01}
CONTRACT_MONTH=${PPA_SMOKE_CONTRACT_MONTH:-2024-02-01}

SERVICES=(minio postgres nessie lakefs clickhouse trino redis api)

export SCENARIO_VALUE PPA_PRICE VOLUME UPFRONT

echo "[ppa-smoke] starting core services"
"${COMPOSE[@]}" up -d "${SERVICES[@]}" --wait

echo "[ppa-smoke] seeding sample contract and outputs"
"${COMPOSE[@]}" run --rm ppa-smoke-seed >/dev/null

echo "[ppa-smoke] waiting for API readiness"
for _ in {1..40}; do
  if curl -sf "${API_BASE}/health" >/dev/null; then
    break
  fi
  sleep 2
done

if ! curl -sf "${API_BASE}/health" >/dev/null; then
  echo "API failed to become ready" >&2
  exit 1
fi

EXPECTED_CASHFLOW=$(python - <<'PY'
import os
cashflow = (float(os.environ["SCENARIO_VALUE"]) - float(os.environ["PPA_PRICE"])) * float(os.environ["VOLUME"])
print(f"{cashflow:.6f}")
PY
)

EXPECTED_NPV=$(python - <<'PY'
import os
cashflow = (float(os.environ["SCENARIO_VALUE"]) - float(os.environ["PPA_PRICE"])) * float(os.environ["VOLUME"])
npv = cashflow - float(os.environ["UPFRONT"])
print(f"{npv:.6f}")
PY
)

echo "[ppa-smoke] fetching stored valuations"
VALUATIONS_JSON=$(curl -sf -H "${TENANT_HEADER}: ${TENANT_ID}" "${API_BASE}/v1/ppa/contracts/${CONTRACT_ID}/valuations?limit=10")

python - "$EXPECTED_CASHFLOW" "$EXPECTED_NPV" <<'PY' <<<"$VALUATIONS_JSON"
import json
import math
import sys

payload = json.load(sys.stdin)
data = payload.get("data") or []
if len(data) < 3:
    raise SystemExit("expected at least 3 valuation rows for seeded contract")

metrics = {row.get("metric"): row for row in data}
for required in ("cashflow", "NPV", "IRR"):
    if required not in metrics:
        raise SystemExit(f"missing valuation metric {required}")

cashflow = float(metrics["cashflow"].get("value", 0.0))
npv = float(metrics["NPV"].get("value", 0.0))
expected_cf = float(sys.argv[1])
expected_npv = float(sys.argv[2])
if math.fabs(cashflow - expected_cf) > 1e-3:
    raise SystemExit(f"cashflow valuation mismatch: got {cashflow}, expected {expected_cf}")
if math.fabs(npv - expected_npv) > 1e-3:
    raise SystemExit(f"NPV valuation mismatch: got {npv}, expected {expected_npv}")

print(f"Validated {len(data)} persisted valuation rows.")
PY

read -r -d '' VALUATE_PAYLOAD <<JSON || true
{
  "ppa_contract_id": "${CONTRACT_ID}",
  "scenario_id": "${SCENARIO_ID}",
  "asof_date": "${ASOF_DATE}",
  "options": {
    "ppa_price": ${PPA_PRICE},
    "volume_mwh": ${VOLUME},
    "upfront_cost": ${UPFRONT}
  }
}
JSON

echo "[ppa-smoke] calling valuation endpoint"
VALUATE_JSON=$(curl -sf -H "Content-Type: application/json" -H "${TENANT_HEADER}: ${TENANT_ID}" --data "${VALUATE_PAYLOAD}" "${API_BASE}/v1/ppa/valuate")

python - "$EXPECTED_CASHFLOW" "$EXPECTED_NPV" <<'PY' <<<"$VALUATE_JSON"
import json
import math
import sys

payload = json.load(sys.stdin)
data = payload.get("data") or []
if not data:
    raise SystemExit("PPA valuation returned no metrics")

metrics = {row.get("metric"): row for row in data}
for required in ("cashflow", "NPV"):
    if required not in metrics:
        raise SystemExit(f"valuation response missing {required}")

cashflow = float(metrics["cashflow"].get("value", 0.0))
npv = float(metrics["NPV"].get("value", 0.0))
expected_cf = float(sys.argv[1])
expected_npv = float(sys.argv[2])
if math.fabs(cashflow - expected_cf) > 1e-3:
    raise SystemExit(f"valuation cashflow mismatch: got {cashflow}, expected {expected_cf}")
if math.fabs(npv - expected_npv) > 1e-3:
    raise SystemExit(f"valuation NPV mismatch: got {npv}, expected {expected_npv}")

print(f"Valuation endpoint returned {len(data)} metrics (cashflow={cashflow}, npv={npv}).")
PY

echo "[ppa-smoke] PPA valuation smoke test completed successfully"
