#!/usr/bin/env bash
set -euo pipefail

NAME="${AURUM_KIND_CLUSTER:-aurum-dev}"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
FORCE_RECREATE="${AURUM_KIND_FORCE_RECREATE:-false}"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--force]

Create the kind cluster used for local Aurum development.

Options:
  -f, --force   Delete any existing cluster with the configured name before creating a new one.
  -h, --help    Show this message and exit.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--force)
      FORCE_RECREATE="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required but not found in PATH" >&2
  exit 1
fi

normalized_force=$(printf '%s' "${FORCE_RECREATE}" | tr '[:upper:]' '[:lower:]')
if [[ "${normalized_force}" == "1" || "${normalized_force}" == "true" || "${normalized_force}" == "yes" ]]; then
  FORCE_RECREATE="true"
else
  FORCE_RECREATE="false"
fi

clusters=$(kind get clusters 2>/dev/null || true)
cluster_exists="false"
while IFS= read -r cluster; do
  if [[ "${cluster}" == "${NAME}" ]]; then
    cluster_exists="true"
    break
  fi
done <<<"${clusters}"

if [[ "${cluster_exists}" == "true" ]]; then
  if [[ "${FORCE_RECREATE}" == "true" ]]; then
    echo "Cluster '${NAME}' already exists; deleting due to --force flag."
    kind delete cluster --name "${NAME}"
  else
    echo "Cluster '${NAME}' already exists. Skip creation or rerun with --force." >&2
    exit 0
  fi
fi

mkdir -p "${ROOT_DIR}/trino/catalog"
mkdir -p "${ROOT_DIR}/airflow/dags"

DATA_ROOT="${ROOT_DIR}/.kind-data"
mkdir -p "${DATA_ROOT}"
for dir in postgres timescale minio clickhouse vault backups; do
  mkdir -p "${DATA_ROOT}/${dir}"
done

cat <<KIND_CFG | kind create cluster --name "${NAME}" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30080
        hostPort: 8080
        protocol: TCP
      - containerPort: 30081
        hostPort: 8081
        protocol: TCP
      - containerPort: 30000
        hostPort: 8000
        protocol: TCP
      - containerPort: 30900
        hostPort: 9000
        protocol: TCP
      - containerPort: 30901
        hostPort: 9001
        protocol: TCP
      - containerPort: 30432
        hostPort: 5432
        protocol: TCP
      - containerPort: 30433
        hostPort: 5433
        protocol: TCP
      - containerPort: 30379
        hostPort: 6379
        protocol: TCP
      - containerPort: 32181
        hostPort: 2181
        protocol: TCP
      - containerPort: 30994
        hostPort: 29092
        protocol: TCP
      - containerPort: 30121
        hostPort: 19121
        protocol: TCP
      - containerPort: 30888
        hostPort: 8088
        protocol: TCP
      - containerPort: 30123
        hostPort: 8123
        protocol: TCP
      - containerPort: 30909
        hostPort: 9009
        protocol: TCP
      - containerPort: 30889
        hostPort: 8089
        protocol: TCP
      - containerPort: 30890
        hostPort: 8090
        protocol: TCP
      - containerPort: 32080
        hostPort: 8085
        protocol: TCP
      - containerPort: 32443
        hostPort: 8443
        protocol: TCP
      - containerPort: 32090
        hostPort: 8095
        protocol: TCP
      - containerPort: 30200
        hostPort: 8200
        protocol: TCP
    extraMounts:
      - hostPath: "${ROOT_DIR}/trino/catalog"
        containerPath: /workspace/trino/catalog
      - hostPath: "${ROOT_DIR}/airflow/dags"
        containerPath: /workspace/airflow/dags
      - hostPath: "${DATA_ROOT}/postgres"
        containerPath: /workspace/data/postgres
      - hostPath: "${DATA_ROOT}/timescale"
        containerPath: /workspace/data/timescale
      - hostPath: "${DATA_ROOT}/minio"
        containerPath: /workspace/data/minio
      - hostPath: "${DATA_ROOT}/clickhouse"
        containerPath: /workspace/data/clickhouse
      - hostPath: "${DATA_ROOT}/backups"
        containerPath: /workspace/data/backups
KIND_CFG
