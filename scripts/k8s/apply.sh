#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required but not found in PATH" >&2
  exit 1
fi

"${ROOT_DIR}/scripts/k8s/install_strimzi.sh"

STRIMZI_CRDS=("kafkas.kafka.strimzi.io")
for crd in "${STRIMZI_CRDS[@]}"; do
  if ! kubectl get crd "${crd}" >/dev/null 2>&1; then
    echo "Strimzi CRD ${crd} is not available; aborting apply before submitting Kafka resources" >&2
    exit 1
  fi
done

# Remove legacy Kafka resources (pre-Strimzi) if they exist
kubectl -n aurum-dev delete deployment kafka --ignore-not-found >/dev/null
kubectl -n aurum-dev delete statefulset kafka --ignore-not-found >/dev/null
kubectl -n aurum-dev delete service kafka --ignore-not-found >/dev/null
kubectl -n aurum-dev delete service kafka-headless --ignore-not-found >/dev/null

# Resolve kustomize with --enable-helm support, downloading v5 if needed
KUSTOMIZE_BIN=""
if command -v kustomize >/dev/null 2>&1; then
  KUSTOMIZE_BIN="$(command -v kustomize)"
else
  KUSTOMIZE_BIN="${ROOT_DIR}/.bin/kustomize"
  if [[ ! -x "${KUSTOMIZE_BIN}" ]]; then
    mkdir -p "${ROOT_DIR}/.bin"
    echo "kustomize not found; downloading v5 binary locally..."
    OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
    ARCH_RAW="$(uname -m)"
    case "${ARCH_RAW}" in
      x86_64) ARCH=amd64 ;;
      arm64|aarch64) ARCH=arm64 ;;
      *) echo "Unsupported arch: ${ARCH_RAW}" >&2; exit 1 ;;
    esac
    VERSION="v5.4.2"
    TARBALL="kustomize_${VERSION}_${OS}_${ARCH}.tar.gz"
    URL="https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2F${VERSION}/${TARBALL}"
    TMPDIR="$(mktemp -d)"
    trap 'rm -rf "${TMPDIR}"' EXIT
    if ! curl -fsSL "${URL}" -o "${TMPDIR}/${TARBALL}"; then
      echo "Failed to download kustomize from ${URL}" >&2
      exit 1
    fi
    tar -xzf "${TMPDIR}/${TARBALL}" -C "${TMPDIR}" kustomize
    install -m 0755 "${TMPDIR}/kustomize" "${KUSTOMIZE_BIN}"
  fi
fi

"${KUSTOMIZE_BIN}" build --enable-helm --load-restrictor=LoadRestrictionsNone "${ROOT_DIR}/k8s/dev" | kubectl apply -f -

kubectl -n aurum-dev wait --for=condition=available --timeout=180s deployment/minio deployment/redis deployment/schema-registry deployment/nessie deployment/lakefs deployment/trino deployment/airflow-webserver deployment/airflow-scheduler deployment/traefik deployment/vault deployment/vault-agent-injector
kubectl -n aurum-dev wait --for=condition=ready --timeout=240s pod -l app=postgres
kubectl -n aurum-dev wait --for=condition=ready --timeout=240s pod -l app=timescale
kubectl -n aurum-dev wait --for=condition=ready --timeout=300s pod -l strimzi.io/name=aurum-kafka-kafka
kubectl -n aurum-dev wait --for=condition=ready --timeout=300s statefulset/clickhouse

kubectl -n aurum-dev wait --for=condition=complete --timeout=300s job/airflow-init
kubectl -n aurum-dev wait --for=condition=complete --timeout=300s job/vault-bootstrap
