#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required to run the Vault dev container" >&2
  exit 1
fi

CONTAINER_NAME=${VAULT_DEV_CONTAINER:-aurum-vault-dev}
ROOT_TOKEN=${VAULT_DEV_ROOT_TOKEN:-aurum-dev-token}
LISTEN_ADDRESS=${VAULT_DEV_LISTEN_ADDR:-0.0.0.0}
PORT=${VAULT_DEV_PORT:-8200}

DETACH_FLAG="-d"
if [[ "${VAULT_DEV_DETACH:-true}" == "false" ]]; then
  DETACH_FLAG=""
fi

echo "Starting Vault dev server (${CONTAINER_NAME}) on port ${PORT}" >&2

docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

docker run ${DETACH_FLAG} --name "${CONTAINER_NAME}" \
  -e VAULT_DEV_ROOT_TOKEN_ID="${ROOT_TOKEN}" \
  -e VAULT_DEV_LISTEN_ADDRESS="${LISTEN_ADDRESS}:${PORT}" \
  -p "${PORT}:${PORT}" \
  hashicorp/vault:1.16 \
  server -dev -dev-root-token-id="${ROOT_TOKEN}" -dev-listen-address="${LISTEN_ADDRESS}:${PORT}"

if [[ -n "${DETACH_FLAG}" ]]; then
  echo "Vault dev server started. Export VAULT_ADDR=http://${LISTEN_ADDRESS}:${PORT} and VAULT_TOKEN=${ROOT_TOKEN}" >&2
  docker logs "${CONTAINER_NAME}" 2>/dev/null | head -n 5 >&2
fi
