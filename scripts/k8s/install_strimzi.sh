#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="aurum-dev"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required but not found in PATH" >&2
  exit 1
fi

STRIMZI_MANIFEST="https://strimzi.io/install/latest?namespace=${NAMESPACE}"

echo "Applying Strimzi operator manifest (${STRIMZI_MANIFEST})"
kubectl apply -f "${STRIMZI_MANIFEST}" >/dev/null

echo "Waiting for Strimzi Cluster Operator to become ready"
kubectl -n "${NAMESPACE}" wait --for=condition=Available --timeout=180s deployment/strimzi-cluster-operator
