#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-aurum-dev}"
STRIMZI_MANIFEST="${STRIMZI_MANIFEST:-https://strimzi.io/install/latest?namespace=${NAMESPACE}}"
APPLY_RETRIES=${APPLY_RETRIES:-5}
RETRY_SLEEP=${RETRY_SLEEP:-5}
CRD_WAIT_RETRIES=${CRD_WAIT_RETRIES:-12}
CRD_WAIT_TIMEOUT=${CRD_WAIT_TIMEOUT:-30}
STRIMZI_CRDS=("kafkas.kafka.strimzi.io")

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required but not found in PATH" >&2
  exit 1
fi

echo "Applying Strimzi operator manifest (${STRIMZI_MANIFEST})"
for attempt in $(seq 1 "${APPLY_RETRIES}"); do
  if kubectl apply -f "${STRIMZI_MANIFEST}" >/dev/null; then
    break
  fi
  if [[ "${attempt}" == "${APPLY_RETRIES}" ]]; then
    echo "Failed to apply Strimzi manifest after ${APPLY_RETRIES} attempts" >&2
    exit 1
  fi
  echo "Retrying Strimzi manifest apply (${attempt}/${APPLY_RETRIES})..."
  sleep "${RETRY_SLEEP}"
done

echo "Waiting for Strimzi Cluster Operator deployment in namespace ${NAMESPACE}"
kubectl -n "${NAMESPACE}" wait --for=condition=Available --timeout=180s deployment/strimzi-cluster-operator

echo "Ensuring Strimzi CRDs are established"
for crd in "${STRIMZI_CRDS[@]}"; do
  for attempt in $(seq 1 "${CRD_WAIT_RETRIES}"); do
    if kubectl wait --for=condition=Established "crd/${crd}" --timeout="${CRD_WAIT_TIMEOUT}s" >/dev/null 2>&1; then
      echo "  CRD ${crd} is ready"
      break
    fi
    if [[ "${attempt}" == "${CRD_WAIT_RETRIES}" ]]; then
      echo "Timed out waiting for Strimzi CRD ${crd}" >&2
      exit 1
    fi
    echo "  Waiting for CRD ${crd} to be established (${attempt}/${CRD_WAIT_RETRIES})"
    sleep "${RETRY_SLEEP}"
  done
done
