#!/usr/bin/env bash
set -euo pipefail

# Bootstrap EIA ingestion in a kind-backed cluster: register schemas, seed Airflow variables,
# and trigger the DAGs for EIA ingestion and Timescale sinks.

usage() {
  cat <<'USAGE'
Usage: scripts/dev/kind_run_eia.sh [--ns NAMESPACE] [--schema-registry-url URL] [--dry-run]

Steps performed:
  1) Port-forward Schema Registry (unless --schema-registry-url provided)
  2) Register Kafka Avro schemas and set BACKWARD compatibility
  3) Generate Airflow variable commands from config/eia_ingest_datasets.json (and bulk datasets) and apply them in the scheduler pod
  4) Trigger the EIA public feeds, bulk ingestion, and Timescale sink DAGs

Options:
  --ns NAMESPACE              Kubernetes namespace (default: aurum-dev)
  --schema-registry-url URL   Schema Registry URL (default via port-forward http://localhost:8081)
  --dry-run                   Print actions without executing kubectl/registry calls
USAGE
}

NS="aurum-dev"
REGISTRY_URL=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ns)
      NS="$2"; shift 2;;
    --schema-registry-url)
      REGISTRY_URL="$2"; shift 2;;
    --dry-run)
      DRY_RUN=true; shift;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown option: $1" >&2; usage >&2; exit 1;;
  esac
done

ensure_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

ensure_cmd kubectl

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export PYTHONPATH="${PYTHONPATH:-}:${REPO_ROOT}/src"

PF_PID=""
cleanup() {
  if [[ -n "${PF_PID}" ]]; then
    kill "${PF_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [[ -z "${REGISTRY_URL}" ]]; then
  echo "[kind] Port-forwarding schema-registry svc in namespace ${NS} -> localhost:8081" >&2
  kubectl -n "${NS}" port-forward svc/schema-registry 8081:8081 >/dev/null 2>&1 &
  PF_PID=$!
  sleep 2
  REGISTRY_URL="http://localhost:8081"
fi

echo "[kind] Using Schema Registry at ${REGISTRY_URL}" >&2

if [[ "${DRY_RUN}" == true ]]; then
  echo "[dry-run] Register schemas" >&2
  SCHEMA_REGISTRY_URL="${REGISTRY_URL}" "${REPO_ROOT}/scripts/kafka/bootstrap.sh" --dry-run
else
  SCHEMA_REGISTRY_URL="${REGISTRY_URL}" "${REPO_ROOT}/scripts/kafka/bootstrap.sh"
fi

echo "[kind] Resolving Airflow scheduler pod" >&2
SCHEDULER="$(kubectl -n "${NS}" get pods -l component=scheduler -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -z "${SCHEDULER}" ]]; then
  SCHEDULER="$(kubectl -n "${NS}" get pods -l app=airflow,component=scheduler -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
fi
if [[ -z "${SCHEDULER}" ]]; then
  echo "Failed to resolve Airflow scheduler pod in namespace ${NS}" >&2
  exit 1
fi
echo "[kind] Airflow scheduler: ${SCHEDULER}" >&2

echo "[kind] Generating Airflow variable commands from config/eia_ingest_datasets.json" >&2
VARS_FILE="$(mktemp)"
python3 "${REPO_ROOT}/scripts/eia/set_airflow_variables.py" > "${VARS_FILE}"

if [[ "${DRY_RUN}" == true ]]; then
  echo "[dry-run] Would apply Airflow variables in ${SCHEDULER}:" >&2
  head -n 20 "${VARS_FILE}" >&2 || true
else
  kubectl -n "${NS}" exec -i "${SCHEDULER}" -- bash -lc 'cat >/tmp/eia_vars.sh && bash /tmp/eia_vars.sh' < "${VARS_FILE}"
fi

echo "[kind] Triggering DAGs" >&2
if [[ "${DRY_RUN}" == true ]]; then
  echo "[dry-run] airflow dags trigger ingest_public_feeds" >&2
  echo "[dry-run] airflow dags trigger ingest_eia_bulk" >&2
  echo "[dry-run] airflow dags trigger ingest_eia_series_timescale" >&2
else
  kubectl -n "${NS}" exec "${SCHEDULER}" -- airflow dags trigger ingest_public_feeds || true
  kubectl -n "${NS}" exec "${SCHEDULER}" -- airflow dags trigger ingest_eia_bulk || true
  kubectl -n "${NS}" exec "${SCHEDULER}" -- airflow dags trigger ingest_eia_series_timescale || true
fi

echo "[kind] Done." >&2
