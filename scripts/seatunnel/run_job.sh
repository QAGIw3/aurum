#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ISO_LMP_SCHEMA_PATH="${REPO_ROOT}/kafka/schemas/iso.lmp.v1.avsc"
NOAA_GHCND_SCHEMA_PATH="${REPO_ROOT}/kafka/schemas/noaa.weather.v1.avsc"
EIA_SERIES_SCHEMA_PATH="${REPO_ROOT}/kafka/schemas/eia.series.v1.avsc"
FRED_SERIES_SCHEMA_PATH="${REPO_ROOT}/kafka/schemas/fred.series.v1.avsc"

render_schema() {
  local schema_path="$1"
  python3 - <<'PY' "${schema_path}"
import json
import sys
from pathlib import Path

schema_path = Path(sys.argv[1])
if not schema_path.exists():
    raise SystemExit(f"Schema file not found: {schema_path}")

with schema_path.open(encoding="utf-8") as fp:
    schema = json.load(fp)

print(json.dumps(schema, indent=2))
PY
}

usage() {
  cat <<'USAGE'
Usage: scripts/seatunnel/run_job.sh <job-name> [--render-only] [--image IMAGE]

Render a SeaTunnel job template (seatunnel/jobs/<job-name>.conf.tmpl) using the
current environment and optionally execute it inside a container.

Options:
  --render-only    Generate the config file but do not run SeaTunnel
  --image IMAGE    Docker image to use when executing (default: ${SEATUNNEL_IMAGE:-apache/seatunnel:2.3.3})

Environment:
  SEATUNNEL_IMAGE      Override the default SeaTunnel image
  SEATUNNEL_OUTPUT_DIR Directory for rendered configs (default: seatunnel/jobs/generated)
  DOCKER_NETWORK       If set, attach the container to this docker network

Examples:
  NOAA_GHCND_TOKEN=... NOAA_GHCND_START_DATE=2024-01-01 NOAA_GHCND_END_DATE=2024-01-02 \
  NOAA_GHCND_TOPIC=aurum.ref.noaa.weather.v1 KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  scripts/seatunnel/run_job.sh noaa_ghcnd_to_kafka

  # Render only and inspect the config
  scripts/seatunnel/run_job.sh noaa_ghcnd_to_kafka --render-only | less
USAGE
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 1
fi

JOB_NAME=""
RENDER_ONLY="false"
IMAGE="${SEATUNNEL_IMAGE:-apache/seatunnel:2.3.3}"

while [[ $# > 0 ]]; do
  case "$1" in
    --render-only)
      RENDER_ONLY="true"
      shift
      ;;
    --image)
      if [[ $# -lt 2 ]]; then
        echo "--image requires an argument" >&2
        exit 1
      fi
      IMAGE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      JOB_NAME="$1"
      shift
      ;;
  esac
done

if [[ -z "${JOB_NAME}" ]]; then
  echo "Job name is required" >&2
  exit 1
fi

TEMPLATE="seatunnel/jobs/${JOB_NAME}.conf.tmpl"
if [[ ! -f "${TEMPLATE}" ]]; then
  echo "Template not found: ${TEMPLATE}" >&2
  exit 1
fi

export ISO_LOCATION_REGISTRY="${ISO_LOCATION_REGISTRY:-${REPO_ROOT}/config/iso_nodes.csv}"

case "${JOB_NAME}" in
  noaa_ghcnd_to_kafka)
    REQUIRED_VARS=(
      NOAA_GHCND_TOKEN
      NOAA_GHCND_START_DATE
      NOAA_GHCND_END_DATE
      NOAA_GHCND_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export NOAA_GHCND_BASE_URL="${NOAA_GHCND_BASE_URL:-https://www.ncdc.noaa.gov/cdo-web/api/v2}"
    export NOAA_GHCND_DATASET="${NOAA_GHCND_DATASET:-GHCND}"
    export NOAA_GHCND_LIMIT="${NOAA_GHCND_LIMIT:-1000}"
    export NOAA_GHCND_OFFSET="${NOAA_GHCND_OFFSET:-1}"
    export NOAA_GHCND_TIMEOUT="${NOAA_GHCND_TIMEOUT:-30000}"
    export NOAA_GHCND_STATION_LIMIT="${NOAA_GHCND_STATION_LIMIT:-1000}"
    export NOAA_GHCND_UNIT_CODE="${NOAA_GHCND_UNIT_CODE:-unknown}"
    export NOAA_GHCND_SUBJECT="${NOAA_GHCND_SUBJECT:-${NOAA_GHCND_TOPIC}-value}"
    if [[ -z "${NOAA_GHCND_SCHEMA:-}" ]]; then
      NOAA_GHCND_SCHEMA="$(render_schema "${NOAA_GHCND_SCHEMA_PATH}")"
    fi
    export NOAA_GHCND_SCHEMA
    ;;
  eia_series_to_kafka)
    REQUIRED_VARS=(
      EIA_API_KEY
      EIA_SERIES_PATH
      EIA_SERIES_ID
      EIA_FREQUENCY
      EIA_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export EIA_API_BASE_URL="${EIA_API_BASE_URL:-https://api.eia.gov/v2}"
    export EIA_START="${EIA_START:-2024-01-01}"
    export EIA_END="${EIA_END:-2024-12-31}"
    export EIA_OFFSET="${EIA_OFFSET:-0}"
    export EIA_LIMIT="${EIA_LIMIT:-5000}"
    export EIA_SORT="${EIA_SORT:-period}"
    export EIA_DIRECTION="${EIA_DIRECTION:-DESC}"
    export EIA_UNITS="${EIA_UNITS:-unknown}"
    export EIA_SEASONAL_ADJUSTMENT="${EIA_SEASONAL_ADJUSTMENT:-UNKNOWN}"
    export EIA_SOURCE="${EIA_SOURCE:-EIA}"
    export EIA_DATASET="${EIA_DATASET:-}"
    export EIA_SUBJECT="${EIA_SUBJECT:-${EIA_TOPIC}-value}"
    export EIA_AREA_EXPR="${EIA_AREA_EXPR:-CAST(NULL AS STRING)}"
    export EIA_SECTOR_EXPR="${EIA_SECTOR_EXPR:-CAST(NULL AS STRING)}"
    export EIA_DESCRIPTION_EXPR="${EIA_DESCRIPTION_EXPR:-CAST(NULL AS STRING)}"
    export EIA_SOURCE_EXPR="${EIA_SOURCE_EXPR:-COALESCE(source, '${EIA_SOURCE}')}"
    export EIA_DATASET_EXPR="${EIA_DATASET_EXPR:-COALESCE(dataset, '${EIA_DATASET}')}"
    export EIA_METADATA_EXPR="${EIA_METADATA_EXPR:-NULL}"
    if [[ -z "${EIA_SERIES_SCHEMA:-}" ]]; then
      EIA_SERIES_SCHEMA="$(render_schema "${EIA_SERIES_SCHEMA_PATH}")"
    fi
    export EIA_SERIES_SCHEMA
    ;;
 fred_series_to_kafka)
   REQUIRED_VARS=(
     FRED_API_KEY
     FRED_SERIES_ID
     FRED_FREQUENCY
      FRED_SEASONAL_ADJ
      FRED_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export FRED_START_DATE="${FRED_START_DATE:-}"
    export FRED_END_DATE="${FRED_END_DATE:-}"
    export FRED_UNITS="${FRED_UNITS:-Percent}"
    export FRED_TITLE="${FRED_TITLE:-}"
    export FRED_NOTES="${FRED_NOTES:-}"
    export FRED_SUBJECT="${FRED_SUBJECT:-${FRED_TOPIC}-value}"
   if [[ -z "${FRED_SERIES_SCHEMA:-}" ]]; then
     FRED_SERIES_SCHEMA="$(render_schema "${FRED_SERIES_SCHEMA_PATH}")"
   fi
   export FRED_SERIES_SCHEMA
   ;;
  cpi_series_to_kafka)
    REQUIRED_VARS=(
      FRED_API_KEY
      CPI_SERIES_ID
      CPI_FREQUENCY
      CPI_SEASONAL_ADJ
      CPI_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export CPI_AREA="${CPI_AREA:-US}"
    export CPI_UNITS="${CPI_UNITS:-Index}"
    export CPI_SOURCE="${CPI_SOURCE:-FRED}"
    export CPI_SUBJECT="${CPI_SUBJECT:-${CPI_TOPIC}-value}"
    export CPI_SCHEMA_PATH="${CPI_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/cpi.series.v1.avsc}"
    if [[ -z "${CPI_SCHEMA:-}" ]]; then
      CPI_SCHEMA="$(render_schema "${CPI_SCHEMA_PATH}")"
    fi
    export CPI_SCHEMA
    ;;
  pjm_lmp_to_kafka)
    REQUIRED_VARS=(
      PJM_API_KEY
      PJM_TOPIC
      PJM_INTERVAL_START
      PJM_INTERVAL_END
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export PJM_ENDPOINT="${PJM_ENDPOINT:-https://api.pjm.com/api/v1/da_hrl_lmps}"
    export PJM_ROW_LIMIT="${PJM_ROW_LIMIT:-10000}"
    export PJM_MARKET="${PJM_MARKET:-DAY_AHEAD}"
    export PJM_LOCATION_TYPE="${PJM_LOCATION_TYPE:-NODE}"
    export PJM_SUBJECT="${PJM_SUBJECT:-${PJM_TOPIC}-value}"
    if [[ -z "${ISO_LMP_SCHEMA:-}" ]]; then
      ISO_LMP_SCHEMA="$(render_schema "${ISO_LMP_SCHEMA_PATH}")"
    fi
    export ISO_LMP_SCHEMA
    ;;
  iso_lmp_kafka_to_timescale)
    REQUIRED_VARS=(
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
      TIMESCALE_JDBC_URL
      TIMESCALE_USER
      TIMESCALE_PASSWORD
    )
    export ISO_LMP_TOPIC_PATTERN="${ISO_LMP_TOPIC_PATTERN:-aurum\\.iso\\..*\\.lmp\\.v1}"
    export ISO_LMP_TABLE="${ISO_LMP_TABLE:-iso_lmp_timeseries}"
    export ISO_LMP_SAVE_MODE="${ISO_LMP_SAVE_MODE:-append}"
    ;;
  nyiso_lmp_to_kafka)
    REQUIRED_VARS=(
      NYISO_URL
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export NYISO_TOPIC="${NYISO_TOPIC:-aurum.iso.nyiso.lmp.v1}"
    export NYISO_SUBJECT="${NYISO_SUBJECT:-${NYISO_TOPIC}-value}"
    if [[ -z "${ISO_LMP_SCHEMA:-}" ]]; then
      ISO_LMP_SCHEMA="$(render_schema "${ISO_LMP_SCHEMA_PATH}")"
    fi
    export ISO_LMP_SCHEMA
    ;;
  miso_lmp_to_kafka)
    REQUIRED_VARS=(
      MISO_URL
      MISO_MARKET
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export MISO_TOPIC="${MISO_TOPIC:-aurum.iso.miso.lmp.v1}"
    export MISO_SUBJECT="${MISO_SUBJECT:-${MISO_TOPIC}-value}"
    export MISO_TIME_FORMAT="${MISO_TIME_FORMAT:-yyyy-MM-dd HH:mm:ss}"
    export MISO_TIME_COLUMN="${MISO_TIME_COLUMN:-Time}"
    export MISO_NODE_COLUMN="${MISO_NODE_COLUMN:-CPNode}"
    export MISO_NODE_ID_COLUMN="${MISO_NODE_ID_COLUMN:-CPNode ID}"
    export MISO_LMP_COLUMN="${MISO_LMP_COLUMN:-LMP}"
    export MISO_CONGESTION_COLUMN="${MISO_CONGESTION_COLUMN:-MCC}"
    export MISO_LOSS_COLUMN="${MISO_LOSS_COLUMN:-MLC}"
    if [[ -z "${MISO_INTERVAL_SECONDS:-}" ]]; then
      case "${MISO_MARKET^^}" in
        DA|DAY_AHEAD)
          MISO_INTERVAL_SECONDS=3600
          ;;
        *)
          MISO_INTERVAL_SECONDS=300
          ;;
      esac
    fi
    export MISO_INTERVAL_SECONDS
    export MISO_CURRENCY="${MISO_CURRENCY:-USD}"
    export MISO_UOM="${MISO_UOM:-MWh}"
    if [[ -z "${ISO_LMP_SCHEMA:-}" ]]; then
      ISO_LMP_SCHEMA="$(render_schema "${ISO_LMP_SCHEMA_PATH}")"
    fi
    export ISO_LMP_SCHEMA
    ;;
  isone_lmp_to_kafka)
    REQUIRED_VARS=(
      ISONE_URL
      ISONE_START
      ISONE_END
      ISONE_MARKET
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export ISONE_TOPIC="${ISONE_TOPIC:-aurum.iso.isone.lmp.v1}"
    export ISONE_SUBJECT="${ISONE_SUBJECT:-${ISONE_TOPIC}-value}"
    export ISONE_NODE_FIELD="${ISONE_NODE_FIELD:-name}"
    export ISONE_NODE_ID_FIELD="${ISONE_NODE_ID_FIELD:-ptid}"
    export ISONE_NODE_TYPE_FIELD="${ISONE_NODE_TYPE_FIELD:-locationType}"
    if [[ -n "${ISONE_AUTH_HEADER:-}" ]]; then
      export ISONE_HTTP_AUTH_ENABLED=false
    else
      if [[ -n "${ISONE_USERNAME:-}" && -n "${ISONE_PASSWORD:-}" ]]; then
        export ISONE_HTTP_AUTH_ENABLED=true
      else
        export ISONE_HTTP_AUTH_ENABLED=false
      fi
    fi
    if [[ -z "${ISO_LMP_SCHEMA:-}" ]]; then
      ISO_LMP_SCHEMA="$(render_schema "${ISO_LMP_SCHEMA_PATH}")"
    fi
    export ISO_LMP_SCHEMA
    ;;
  caiso_lmp_to_kafka)
    REQUIRED_VARS=(
      CAISO_INPUT_JSON
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export CAISO_TOPIC="${CAISO_TOPIC:-aurum.iso.caiso.lmp.v1}"
    export CAISO_SUBJECT="${CAISO_SUBJECT:-${CAISO_TOPIC}-value}"
    export CAISO_CURRENCY="${CAISO_CURRENCY:-USD}"
    export CAISO_UOM="${CAISO_UOM:-MWh}"
    if [[ -z "${ISO_LMP_SCHEMA:-}" ]]; then
      ISO_LMP_SCHEMA="$(render_schema "${ISO_LMP_SCHEMA_PATH}")"
    fi
    export ISO_LMP_SCHEMA
    ;;
  ercot_lmp_to_kafka)
    REQUIRED_VARS=(
      ERCOT_INPUT_JSON
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export ERCOT_TOPIC="${ERCOT_TOPIC:-aurum.iso.ercot.lmp.v1}"
    export ERCOT_SUBJECT="${ERCOT_SUBJECT:-${ERCOT_TOPIC}-value}"
    export ERCOT_CURRENCY="${ERCOT_CURRENCY:-USD}"
    export ERCOT_UOM="${ERCOT_UOM:-MWh}"
    if [[ -z "${ISO_LMP_SCHEMA:-}" ]]; then
      ISO_LMP_SCHEMA="$(render_schema "${ISO_LMP_SCHEMA_PATH}")"
    fi
    export ISO_LMP_SCHEMA
    ;;
  spp_lmp_to_kafka)
    REQUIRED_VARS=(
      SPP_INPUT_JSON
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export SPP_TOPIC="${SPP_TOPIC:-aurum.iso.spp.lmp.v1}"
    export SPP_SUBJECT="${SPP_SUBJECT:-${SPP_TOPIC}-value}"
    export SPP_CURRENCY="${SPP_CURRENCY:-USD}"
    export SPP_UOM="${SPP_UOM:-MWh}"
    if [[ -z "${ISO_LMP_SCHEMA:-}" ]]; then
      ISO_LMP_SCHEMA="$(render_schema "${ISO_LMP_SCHEMA_PATH}")"
    fi
    export ISO_LMP_SCHEMA
    ;;
  *)
    REQUIRED_VARS=()
    ;;
esac

for var in "${REQUIRED_VARS[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "Missing required env var ${var} for job ${JOB_NAME}" >&2
    exit 1
  fi
done

OUTPUT_DIR="${SEATUNNEL_OUTPUT_DIR:-seatunnel/jobs/generated}"
mkdir -p "${OUTPUT_DIR}"
OUTPUT_CONFIG="${OUTPUT_DIR}/${JOB_NAME}.conf"

if command -v envsubst >/dev/null 2>&1; then
  envsubst < "${TEMPLATE}" > "${OUTPUT_CONFIG}"
else
  python3 - <<'PY' "${TEMPLATE}" "${OUTPUT_CONFIG}"
import os
import sys
from pathlib import Path
from string import Template

template_path, output_path = sys.argv[1:3]
content = Path(template_path).read_text(encoding="utf-8")
rendered = Template(content).substitute(os.environ)
Path(output_path).write_text(rendered, encoding="utf-8")
PY
fi

if [[ "${RENDER_ONLY}" == "true" ]]; then
  cat "${OUTPUT_CONFIG}"
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required to execute the job automatically. Config rendered at ${OUTPUT_CONFIG}" >&2
  exit 1
fi

# Default to compose service hosts if not provided
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
export SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"

DOCKER_ARGS=(--rm -v "$(pwd)":"/workspace" -w /workspace -e SEATUNNEL_HOME=/opt/seatunnel)
if [[ -n "${DOCKER_NETWORK:-}" ]]; then
  DOCKER_ARGS+=(--network "${DOCKER_NETWORK}")
fi

echo "Running SeaTunnel job '${JOB_NAME}' using image ${IMAGE}" >&2

docker run "${DOCKER_ARGS[@]}" \
  "${IMAGE}" \
  /opt/seatunnel/bin/seatunnel.sh --config "${OUTPUT_CONFIG}"
