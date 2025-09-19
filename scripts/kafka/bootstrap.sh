#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/kafka/bootstrap.sh [--schema-registry-url URL] [--dry-run]

Register Aurum Kafka schemas and enforce compatibility in one step.

Options:
  --schema-registry-url URL  Schema Registry base URL (default env SCHEMA_REGISTRY_URL or http://localhost:8081)
  --dry-run                  Print actions without making HTTP calls
  -h, --help                 Show this help message
USAGE
}

REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
DRY_RUN=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export PYTHONPATH="${PYTHONPATH:-}:${REPO_ROOT}/src"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema-registry-url)
      if [[ $# -lt 2 ]]; then
        echo "--schema-registry-url requires a value" >&2
        exit 1
      fi
      REGISTRY_URL="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${DRY_RUN}" == true ]]; then
  echo "[bootstrap] running in dry-run mode"
  python3 scripts/kafka/register_schemas.py --schema-registry-url "${REGISTRY_URL}" --dry-run
  python3 scripts/kafka/set_compatibility.py --schema-registry-url "${REGISTRY_URL}" --dry-run
else
  python3 scripts/kafka/register_schemas.py --schema-registry-url "${REGISTRY_URL}"
  python3 scripts/kafka/set_compatibility.py --schema-registry-url "${REGISTRY_URL}"
fi
