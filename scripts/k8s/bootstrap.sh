#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required but not found in PATH" >&2
  exit 1
fi

kubectl -n aurum-dev delete job aurum-bootstrap --ignore-not-found
kubectl apply -k "${ROOT_DIR}/k8s/bootstrap"

kubectl -n aurum-dev wait --for=condition=complete --timeout=300s job/aurum-bootstrap
