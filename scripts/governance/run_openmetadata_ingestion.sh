#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="${ROOT_DIR}/config/governance/openmetadata_ingestion.yaml"

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Configuration not found at ${CONFIG_FILE}" >&2
  exit 1
fi

export PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}"

echo "Running OpenMetadata ingestion using ${CONFIG_FILE}" >&2

metadata ingest --config "${CONFIG_FILE}"
