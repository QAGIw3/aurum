#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required but not found in PATH" >&2
  exit 1
fi

kubectl apply -k "${ROOT_DIR}/k8s/ui"

kubectl -n aurum-dev wait --for=condition=available --timeout=180s deployment/superset deployment/kafka-ui
