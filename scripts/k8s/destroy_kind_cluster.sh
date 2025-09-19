#!/usr/bin/env bash
set -euo pipefail

NAME="${AURUM_KIND_CLUSTER:-aurum-dev}"

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required but not found in PATH" >&2
  exit 1
fi

kind delete cluster --name "${NAME}"
