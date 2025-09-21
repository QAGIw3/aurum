#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export REPO_ROOT
ISO_LMP_SCHEMA_PATH="${ISO_LMP_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.lmp.v1.avsc}"
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

ensure_iso_lmp_schema() {
  if [[ -z "${ISO_LMP_SCHEMA:-}" ]]; then
    if [[ -n "${ISO_LMP_SCHEMA_PATH:-}" && -f "${ISO_LMP_SCHEMA_PATH}" ]]; then
      ISO_LMP_SCHEMA="$(render_schema "${ISO_LMP_SCHEMA_PATH}")"
    else
      ISO_LMP_SCHEMA='{"type":"record","name":"IsoLmpRecord","namespace":"aurum.iso","doc":"Normalized locational marginal price observation from an ISO day-ahead or real-time feed.","fields":[{"name":"iso_code","type":{"type":"enum","name":"IsoCode","doc":"Market operator the observation belongs to.","symbols":["PJM","CAISO","ERCOT","NYISO","MISO","ISONE","SPP","AESO"]}},{"name":"market","type":{"type":"enum","name":"IsoMarket","doc":"Normalized market/run identifier reported by the ISO.","symbols":["DAY_AHEAD","REAL_TIME","FIFTEEN_MINUTE","FIVE_MINUTE","HOUR_AHEAD","SETTLEMENT","UNKNOWN"]}},{"name":"delivery_date","doc":"Trading or operating date for the interval (ISO local calendar).","type":{"type":"int","logicalType":"date"}},{"name":"interval_start","doc":"UTC timestamp when the interval starts.","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"interval_end","doc":"Optional UTC timestamp when the interval ends (exclusive).","type":["null",{"type":"long","logicalType":"timestamp-micros"}],"default":null},{"name":"interval_minutes","doc":"Duration of the interval in minutes (if supplied by the ISO).","type":["null","int"],"default":null},{"name":"location_id","doc":"Primary identifier (node, zone, hub) from the ISO feed.","type":"string"},{"name":"location_name","doc":"Human-readable description of the location.","type":["null","string"],"default":null},{"name":"location_type","doc":"Classification of the location identifier.","type":{"type":"enum","name":"IsoLocationType","symbols":["NODE","ZONE","HUB","SYSTEM","AGGREGATE","INTERFACE","RESOURCE","OTHER"]},"default":"OTHER"},{"name":"zone","doc":"Optional ISO zone identifier derived from the location registry.","type":["null","string"],"default":null},{"name":"hub","doc":"Optional hub grouping associated with the location.","type":["null","string"],"default":null},{"name":"timezone","doc":"Preferred timezone for interpreting interval timestamps (IANA name).","type":["null","string"],"default":null},{"name":"price_total","doc":"Locational marginal price reported by the ISO.","type":"double"},{"name":"price_energy","doc":"Energy component of the price.","type":["null","double"],"default":null},{"name":"price_congestion","doc":"Congestion component of the price.","type":["null","double"],"default":null},{"name":"price_loss","doc":"Loss component of the price.","type":["null","double"],"default":null},{"name":"currency","doc":"ISO reported currency (ISO-4217 code).","type":"string","default":"USD"},{"name":"uom","doc":"Unit of measure for the price.","type":"string","default":"MWh"},{"name":"settlement_point","doc":"Optional ISO-specific settlement point grouping.","type":["null","string"],"default":null},{"name":"source_run_id","doc":"Identifier for the source extraction run (file, report id, etc.).","type":["null","string"],"default":null},{"name":"ingest_ts","doc":"Timestamp when the record entered the pipeline (UTC).","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"record_hash","doc":"Deterministic hash of the source fields for idempotency.","type":"string"},{"name":"metadata","doc":"Optional key/value metadata captured from the source feed.","type":["null",{"type":"map","values":"string"}],"default":null}]}'
    fi
  fi
  export ISO_LMP_SCHEMA
}

usage() {
  cat <<'USAGE'
Usage: scripts/seatunnel/run_job.sh <job-name> [--render-only] [--image IMAGE]
       scripts/seatunnel/run_job.sh --list
       scripts/seatunnel/run_job.sh --describe <job-name>

Render a SeaTunnel job template (seatunnel/jobs/<job-name>.conf.tmpl) using the
current environment and optionally execute it inside a container.

Options:
  --render-only    Generate the config file but do not run SeaTunnel
  --image IMAGE    Docker image to use when executing (default: ${SEATUNNEL_IMAGE:-apache/seatunnel:2.3.3})
  --list           List available job templates and exit
  --describe JOB   Print required env vars for JOB and exit

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
LIST_ONLY="false"
DESCRIBE_ONLY="false"
DESCRIBE_JOB=""

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
    --list|-l)
      LIST_ONLY="true"
      shift
      ;;
    --describe)
      if [[ $# -lt 2 ]]; then
        echo "--describe requires a job name" >&2
        exit 1
      fi
      DESCRIBE_ONLY="true"
      DESCRIBE_JOB="$2"
      JOB_NAME="$2"
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

if [[ "${LIST_ONLY}" == "true" ]]; then
  for f in seatunnel/jobs/*.conf.tmpl; do
    bn="$(basename "$f")"
    echo "${bn%.conf.tmpl}"
  done | sort
  exit 0
fi

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
    if [[ "${DESCRIBE_ONLY}" != "true" ]]; then
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
    fi
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
    export EIA_SERIES_ID_EXPR="${EIA_SERIES_ID_EXPR:-'${EIA_SERIES_ID}'}"
    export EIA_FILTER_EXPR="${EIA_FILTER_EXPR:-TRUE}"
    export EIA_PARAM_OVERRIDES_JSON="${EIA_PARAM_OVERRIDES_JSON:-[]}"
    export EIA_PERIOD_COLUMN="${EIA_PERIOD_COLUMN:-period}"
    export EIA_DATE_FORMAT="${EIA_DATE_FORMAT:-}"
    if [[ -z "${EIA_PARAM_OVERRIDES_JSON}" || "${EIA_PARAM_OVERRIDES_JSON}" == "[]" ]]; then
      EIA_PARAM_OVERRIDES=""
    else
      if ! EIA_PARAM_OVERRIDES="$(python3 - <<'PY'
import json
import os
import sys

raw = os.environ.get("EIA_PARAM_OVERRIDES_JSON", "[]")
try:
    overrides = json.loads(raw)
except json.JSONDecodeError as exc:
    print(f"Invalid JSON for EIA_PARAM_OVERRIDES_JSON: {exc}", file=sys.stderr)
    sys.exit(1)

def iter_entries(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            yield key, value
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                yield from iter_entries(item)
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                yield item[0], item[1]
            else:
                raise SystemExit(f"Unsupported override entry: {item!r}")
    elif obj is None:
        return
    else:
        raise SystemExit(
            "EIA_PARAM_OVERRIDES_JSON must encode a list or mapping of key/value overrides"
        )

lines = [f"      {key} = \"{value}\"" for key, value in iter_entries(overrides)]
print("\n".join(lines))
PY
)"; then
        echo "Failed to parse EIA_PARAM_OVERRIDES_JSON" >&2
        exit 1
      fi
    fi
    export EIA_PARAM_OVERRIDES
    export EIA_LIMIT="${EIA_LIMIT:-5000}"
    if [[ -z "${EIA_START:-}" || -z "${EIA_END:-}" ]]; then
      window_specified="${EIA_WINDOW_END:-}${EIA_WINDOW_HOURS:-}${EIA_WINDOW_DAYS:-}${EIA_WINDOW_MONTHS:-}${EIA_WINDOW_YEARS:-}"
      if [[ -n "${window_specified}" ]]; then
        if [[ -z "${EIA_WINDOW_END:-}" ]]; then
          EIA_WINDOW_END="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        fi
        if ! eval "$(python3 - <<'PY'
import os
import shlex
import sys
from pathlib import Path

repo_root = Path(os.environ.get("REPO_ROOT", Path.cwd()))
sys.path.insert(0, str(repo_root / "src"))

from aurum.eia.windows import WindowConfigError, compute_window_bounds

window_end = os.environ.get("EIA_WINDOW_END")
frequency = os.environ.get("EIA_FREQUENCY")
date_format = os.environ.get("EIA_DATE_FORMAT")

try:
    bounds = compute_window_bounds(
        window_end=window_end,
        frequency=frequency,
        date_format=date_format,
        hours=os.environ.get("EIA_WINDOW_HOURS"),
        days=os.environ.get("EIA_WINDOW_DAYS"),
        months=os.environ.get("EIA_WINDOW_MONTHS"),
        years=os.environ.get("EIA_WINDOW_YEARS"),
    )
except WindowConfigError as exc:
    print(exc, file=sys.stderr)
    sys.exit(1)

print(f"EIA_START={shlex.quote(bounds.start_token)}")
print(f"EIA_END={shlex.quote(bounds.end_token)}")
PY
)"; then
          echo "Failed to derive EIA window bounds" >&2
          exit 1
        fi
      fi
    fi
    export EIA_START
    export EIA_END
    if [[ -z "${EIA_PERIOD_START_EXPR:-}" || -z "${EIA_PERIOD_END_EXPR:-}" ]]; then
      if ! eval "$(python3 - <<'PY'
import os
import shlex
import sys
from pathlib import Path

repo_root = Path(os.environ.get("REPO_ROOT", Path.cwd()))
sys.path.insert(0, str(repo_root / "src"))

from aurum.eia.periods import PeriodParseError, build_sql_period_expressions

frequency = os.environ.get("EIA_FREQUENCY", "")
column = os.environ.get("EIA_PERIOD_COLUMN", "period")

try:
    start_expr, end_expr = build_sql_period_expressions(frequency, period_column=column)
except PeriodParseError as exc:
    print(exc, file=sys.stderr)
    sys.exit(1)

print(f"EIA_PERIOD_START_EXPR={shlex.quote(start_expr)}")
print(f"EIA_PERIOD_END_EXPR={shlex.quote(end_expr)}")
PY
)"; then
        echo "Failed to derive period expressions for frequency ${EIA_FREQUENCY}" >&2
        exit 1
      fi
    fi
    export EIA_PERIOD_START_EXPR
    export EIA_PERIOD_END_EXPR
    export EIA_CANONICAL_UNIT="${EIA_CANONICAL_UNIT:-}"
    export EIA_CANONICAL_CURRENCY="${EIA_CANONICAL_CURRENCY:-}"
    export EIA_UNIT_CONVERSION_JSON="${EIA_UNIT_CONVERSION_JSON:-null}"
    if [[ -z "${EIA_CANONICAL_UNIT_EXPR:-}" || -z "${EIA_CANONICAL_VALUE_EXPR:-}" ]]; then
      if ! eval "$(python3 - <<'PY'
import json
import os
import shlex
import sys

canonical_unit = os.environ.get("EIA_CANONICAL_UNIT")
canonical_currency = os.environ.get("EIA_CANONICAL_CURRENCY")
conversion_raw = os.environ.get("EIA_UNIT_CONVERSION_JSON", "null")
default_unit = os.environ.get("EIA_UNITS", "")

try:
    conversion = None if conversion_raw in {"", "null", "None"} else json.loads(conversion_raw)
except json.JSONDecodeError as exc:
    print(f"Invalid JSON for EIA_UNIT_CONVERSION_JSON: {exc}", file=sys.stderr)
    sys.exit(1)

if conversion:
    factor = float(conversion.get("factor", 1.0))
    target_unit = conversion.get("target_unit") or canonical_unit or conversion.get("source_unit")
    canonical_value_expr = f"CASE WHEN value IS NULL OR value = '' THEN NULL ELSE CAST(value AS DOUBLE) * {factor} END"
    conversion_factor_expr = str(factor)
    canonical_unit_expr = f"'{target_unit}'" if target_unit else (
        f"COALESCE(units, '{default_unit}')" if default_unit else "units"
    )
else:
    canonical_value_expr = "CASE WHEN value IS NULL OR value = '' THEN NULL ELSE CAST(value AS DOUBLE) END"
    conversion_factor_expr = "NULL"
    canonical_unit_expr = (
        f"'{canonical_unit}'" if canonical_unit else (f"COALESCE(units, '{default_unit}')" if default_unit else "units")
    )

canonical_currency_expr = f"'{canonical_currency}'" if canonical_currency else "NULL"

print(f"EIA_CANONICAL_VALUE_EXPR={shlex.quote(canonical_value_expr)}")
print(f"EIA_CONVERSION_FACTOR_EXPR={shlex.quote(conversion_factor_expr)}")
print(f"EIA_CANONICAL_UNIT_EXPR={shlex.quote(canonical_unit_expr)}")
print(f"EIA_CANONICAL_CURRENCY_EXPR={shlex.quote(canonical_currency_expr)}")
PY
)"; then
        echo "Failed to derive unit normalization expressions" >&2
        exit 1
      fi
    fi
    export EIA_CANONICAL_VALUE_EXPR
    export EIA_CONVERSION_FACTOR_EXPR
    export EIA_CANONICAL_UNIT_EXPR
    export EIA_CANONICAL_CURRENCY_EXPR
    if [[ -z "${EIA_SERIES_SCHEMA:-}" ]]; then
      EIA_SERIES_SCHEMA="$(render_schema "${EIA_SERIES_SCHEMA_PATH}")"
    fi
    export EIA_SERIES_SCHEMA
    ;;
  eia_bulk_to_kafka)
    REQUIRED_VARS=(
      EIA_BULK_URL
      EIA_BULK_TOPIC
      EIA_BULK_FREQUENCY
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export EIA_BULK_SERIES_ID_EXPR="${EIA_BULK_SERIES_ID_EXPR:-series_id}"
    export EIA_BULK_PERIOD_EXPR="${EIA_BULK_PERIOD_EXPR:-period}"
    export EIA_BULK_VALUE_EXPR="${EIA_BULK_VALUE_EXPR:-value}"
    export EIA_BULK_RAW_VALUE_EXPR="${EIA_BULK_RAW_VALUE_EXPR:-value}"
    export EIA_BULK_UNITS_EXPR="${EIA_BULK_UNITS_EXPR:-units}"
    export EIA_BULK_AREA_EXPR="${EIA_BULK_AREA_EXPR:-area}"
    export EIA_BULK_SECTOR_EXPR="${EIA_BULK_SECTOR_EXPR:-sector}"
    export EIA_BULK_DESCRIPTION_EXPR="${EIA_BULK_DESCRIPTION_EXPR:-description}"
    export EIA_BULK_SOURCE_EXPR="${EIA_BULK_SOURCE_EXPR:-source}"
    export EIA_BULK_DATASET_EXPR="${EIA_BULK_DATASET_EXPR:-dataset}"
    export EIA_BULK_METADATA_EXPR="${EIA_BULK_METADATA_EXPR:-NULL}"
    export EIA_BULK_FILTER_EXPR="${EIA_BULK_FILTER_EXPR:-TRUE}"
    export EIA_BULK_SUBJECT="${EIA_BULK_SUBJECT:-${EIA_BULK_TOPIC}-value}"
    export EIA_BULK_DECODE="${EIA_BULK_DECODE:-zip}"
    export EIA_BULK_CSV_DELIMITER="${EIA_BULK_CSV_DELIMITER:-,}"
    export EIA_BULK_SKIP_HEADER="${EIA_BULK_SKIP_HEADER:-1}"
    if [[ -n "${EIA_BULK_SCHEMA_FIELDS_JSON:-}" ]]; then
      if ! EIA_BULK_SCHEMA_FIELDS="$(python3 - <<'PY'
import json
import os
import sys

raw = os.environ.get("EIA_BULK_SCHEMA_FIELDS_JSON", "[]")
try:
    payload = json.loads(raw)
except json.JSONDecodeError as exc:
    print(f"Invalid JSON for EIA_BULK_SCHEMA_FIELDS_JSON: {exc}", file=sys.stderr)
    sys.exit(1)

lines: list[str] = []
if isinstance(payload, list):
    for item in payload:
        if isinstance(item, dict):
            name = item.get("name")
            field_type = item.get("type", "string")
            if name:
                lines.append(f"        {name} = {field_type}")
        elif isinstance(item, str):
            parts = [part.strip() for part in item.split(":", 1)]
            if parts and parts[0]:
                field_type = parts[1] if len(parts) == 2 and parts[1] else "string"
                lines.append(f"        {parts[0]} = {field_type}")
if not lines:
    lines.append("        series_id = string")
    lines.append("        period = string")
    lines.append("        value = string")
    lines.append("        units = string")
    lines.append("        area = string")
    lines.append("        sector = string")
    lines.append("        description = string")
    lines.append("        source = string")
    lines.append("        dataset = string")
    lines.append("        metadata = string")

print("\n".join(lines))
PY
)"; then
        echo "Failed to parse EIA_BULK_SCHEMA_FIELDS_JSON" >&2
        exit 1
      fi
    fi
    if [[ -z "${EIA_BULK_SCHEMA_FIELDS:-}" ]]; then
      EIA_BULK_SCHEMA_FIELDS=$'        series_id = string\n        period = string\n        value = string\n        units = string\n        area = string\n        sector = string\n        description = string\n        source = string\n        dataset = string\n        metadata = string'
    fi
    export EIA_BULK_SCHEMA_FIELDS
    if [[ -z "${EIA_BULK_SCHEMA:-}" ]]; then
      if [[ -z "${EIA_SERIES_SCHEMA:-}" ]]; then
        EIA_SERIES_SCHEMA="$(render_schema "${EIA_SERIES_SCHEMA_PATH}")"
      fi
      EIA_BULK_SCHEMA="${EIA_SERIES_SCHEMA}"
    fi
    export EIA_BULK_SCHEMA
    ;;
  eia_fuel_curve_to_kafka)
    REQUIRED_VARS=(
      EIA_API_KEY
      FUEL_EIA_PATH
      FUEL_SERIES_ID
      FUEL_FUEL_TYPE
      FUEL_FREQUENCY
      FUEL_TOPIC
      FUEL_UNITS
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export EIA_API_BASE_URL="${EIA_API_BASE_URL:-https://api.eia.gov/v2}"
    export FUEL_START="${FUEL_START:-}"
    export FUEL_END="${FUEL_END:-}"
    export FUEL_OFFSET="${FUEL_OFFSET:-0}"
    export FUEL_LIMIT="${FUEL_LIMIT:-5000}"
    export FUEL_SORT="${FUEL_SORT:-period}"
    export FUEL_DIRECTION="${FUEL_DIRECTION:-DESC}"
    export FUEL_BENCHMARK_EXPR="${FUEL_BENCHMARK_EXPR:-CAST(NULL AS STRING)}"
    export FUEL_REGION_EXPR="${FUEL_REGION_EXPR:-CAST(NULL AS STRING)}"
    export FUEL_VALUE_EXPR="${FUEL_VALUE_EXPR:-value}"
    export FUEL_METADATA_EXPR="${FUEL_METADATA_EXPR:-NULL}"
    export FUEL_CURRENCY="${FUEL_CURRENCY:-}"
    export FUEL_SOURCE="${FUEL_SOURCE:-EIA}"
    export FUEL_SUBJECT="${FUEL_SUBJECT:-${FUEL_TOPIC}-value}"
    export FUEL_FILTER_EXPR="${FUEL_FILTER_EXPR:-TRUE}"
    export FUEL_SCHEMA_PATH="${FUEL_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/fuel.curve.v1.avsc}"
    if [[ -z "${FUEL_SCHEMA:-}" ]]; then
      FUEL_SCHEMA="$(render_schema "${FUEL_SCHEMA_PATH}")"
    fi
    export FUEL_SCHEMA
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
    # Ensure PJM_API_KEY is defined for template substitution even if empty
    export PJM_API_KEY="${PJM_API_KEY:-}"
    if [[ -z "${ISO_LMP_SCHEMA:-}" ]]; then
      ISO_LMP_SCHEMA="$(render_schema "${ISO_LMP_SCHEMA_PATH}")"
    fi
    export ISO_LMP_SCHEMA
    ;;
  pjm_load_to_kafka)
    REQUIRED_VARS=(
      PJM_LOAD_ENDPOINT
      PJM_ROW_LIMIT
      PJM_INTERVAL_START
      PJM_INTERVAL_END
      PJM_LOAD_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export PJM_API_KEY="${PJM_API_KEY:-}"
    export PJM_LOAD_SUBJECT="${PJM_LOAD_SUBJECT:-${PJM_LOAD_TOPIC}-value}"
    export ISO_LOAD_SCHEMA_PATH="${ISO_LOAD_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.load.v1.avsc}"
    if [[ -z "${ISO_LOAD_SCHEMA:-}" ]]; then
      ISO_LOAD_SCHEMA="$(render_schema "${ISO_LOAD_SCHEMA_PATH}")"
    fi
    export ISO_LOAD_SCHEMA
    ;;
  miso_load_to_kafka)
    REQUIRED_VARS=(
      MISO_LOAD_ENDPOINT
      MISO_LOAD_INTERVAL_START
      MISO_LOAD_INTERVAL_END
      MISO_LOAD_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export MISO_LOAD_AUTH_HEADER="${MISO_LOAD_AUTH_HEADER:-}"
    export MISO_LOAD_JSONPATH="${MISO_LOAD_JSONPATH:-$$.data[*]}"
    export MISO_LOAD_START_FIELD="${MISO_LOAD_START_FIELD:-startTime}"
    export MISO_LOAD_END_FIELD="${MISO_LOAD_END_FIELD:-endTime}"
    export MISO_LOAD_AREA_FIELD="${MISO_LOAD_AREA_FIELD:-area}"
    export MISO_LOAD_MW_FIELD="${MISO_LOAD_MW_FIELD:-loadMw}"
    export MISO_LOAD_START_EXTRACT="${MISO_LOAD_START_EXTRACT:-\`$MISO_LOAD_START_FIELD\`}"
    export MISO_LOAD_END_EXTRACT="${MISO_LOAD_END_EXTRACT:-\`$MISO_LOAD_END_FIELD\`}"
    export MISO_LOAD_AREA_EXPR="${MISO_LOAD_AREA_EXPR:-\`$MISO_LOAD_AREA_FIELD\`}"
    export MISO_LOAD_MW_EXPR="${MISO_LOAD_MW_EXPR:-\`$MISO_LOAD_MW_FIELD\`}"
    export MISO_LOAD_TIME_FORMAT="${MISO_LOAD_TIME_FORMAT:-yyyy-MM-dd''T''HH:mm:ss}"
    export MISO_LOAD_EXTRA_PARAM_KEY="${MISO_LOAD_EXTRA_PARAM_KEY:-limit}"
    export MISO_LOAD_EXTRA_PARAM_VALUE="${MISO_LOAD_EXTRA_PARAM_VALUE:-1000}"
    export MISO_LOAD_SUBJECT="${MISO_LOAD_SUBJECT:-${MISO_LOAD_TOPIC}-value}"
    export ISO_LOAD_SCHEMA_PATH="${ISO_LOAD_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.load.v1.avsc}"
    if [[ -z "${ISO_LOAD_SCHEMA:-}" ]]; then
      ISO_LOAD_SCHEMA="$(render_schema "${ISO_LOAD_SCHEMA_PATH}")"
    fi
    export ISO_LOAD_SCHEMA
    ;;
  spp_load_to_kafka)
    REQUIRED_VARS=(
      SPP_LOAD_ENDPOINT
      SPP_LOAD_INTERVAL_START
      SPP_LOAD_INTERVAL_END
      SPP_LOAD_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export SPP_LOAD_AUTH_HEADER="${SPP_LOAD_AUTH_HEADER:-}"
    export SPP_LOAD_JSONPATH="${SPP_LOAD_JSONPATH:-$$.data[*]}"
    export SPP_LOAD_START_FIELD="${SPP_LOAD_START_FIELD:-startTime}"
    export SPP_LOAD_END_FIELD="${SPP_LOAD_END_FIELD:-endTime}"
    export SPP_LOAD_AREA_FIELD="${SPP_LOAD_AREA_FIELD:-area}"
    export SPP_LOAD_MW_FIELD="${SPP_LOAD_MW_FIELD:-loadMw}"
    export SPP_LOAD_START_EXTRACT="${SPP_LOAD_START_EXTRACT:-\`$SPP_LOAD_START_FIELD\`}"
    export SPP_LOAD_END_EXTRACT="${SPP_LOAD_END_EXTRACT:-\`$SPP_LOAD_END_FIELD\`}"
    export SPP_LOAD_AREA_EXPR="${SPP_LOAD_AREA_EXPR:-\`$SPP_LOAD_AREA_FIELD\`}"
    export SPP_LOAD_MW_EXPR="${SPP_LOAD_MW_EXPR:-\`$SPP_LOAD_MW_FIELD\`}"
    export SPP_LOAD_TIME_FORMAT="${SPP_LOAD_TIME_FORMAT:-yyyy-MM-dd''T''HH:mm:ss}"
    export SPP_LOAD_EXTRA_PARAM_KEY="${SPP_LOAD_EXTRA_PARAM_KEY:-limit}"
    export SPP_LOAD_EXTRA_PARAM_VALUE="${SPP_LOAD_EXTRA_PARAM_VALUE:-1000}"
    export SPP_LOAD_SUBJECT="${SPP_LOAD_SUBJECT:-${SPP_LOAD_TOPIC}-value}"
    export ISO_LOAD_SCHEMA_PATH="${ISO_LOAD_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.load.v1.avsc}"
    if [[ -z "${ISO_LOAD_SCHEMA:-}" ]]; then
      ISO_LOAD_SCHEMA="$(render_schema "${ISO_LOAD_SCHEMA_PATH}")"
    fi
    export ISO_LOAD_SCHEMA
    ;;
  caiso_load_to_kafka)
    REQUIRED_VARS=(
      CAISO_LOAD_ENDPOINT
      CAISO_LOAD_INTERVAL_START
      CAISO_LOAD_INTERVAL_END
      CAISO_LOAD_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export CAISO_LOAD_AUTH_HEADER="${CAISO_LOAD_AUTH_HEADER:-}"
    export CAISO_LOAD_JSONPATH="${CAISO_LOAD_JSONPATH:-$$.data[*]}"
    export CAISO_LOAD_START_FIELD="${CAISO_LOAD_START_FIELD:-startTime}"
    export CAISO_LOAD_END_FIELD="${CAISO_LOAD_END_FIELD:-endTime}"
    export CAISO_LOAD_AREA_FIELD="${CAISO_LOAD_AREA_FIELD:-area}"
    export CAISO_LOAD_MW_FIELD="${CAISO_LOAD_MW_FIELD:-loadMw}"
    export CAISO_LOAD_START_EXTRACT="${CAISO_LOAD_START_EXTRACT:-\`$CAISO_LOAD_START_FIELD\`}"
    export CAISO_LOAD_END_EXTRACT="${CAISO_LOAD_END_EXTRACT:-\`$CAISO_LOAD_END_FIELD\`}"
    export CAISO_LOAD_AREA_EXPR="${CAISO_LOAD_AREA_EXPR:-\`$CAISO_LOAD_AREA_FIELD\`}"
    export CAISO_LOAD_MW_EXPR="${CAISO_LOAD_MW_EXPR:-\`$CAISO_LOAD_MW_FIELD\`}"
    export CAISO_LOAD_TIME_FORMAT="${CAISO_LOAD_TIME_FORMAT:-yyyy-MM-dd''T''HH:mm:ss}"
    export CAISO_LOAD_EXTRA_PARAM_KEY="${CAISO_LOAD_EXTRA_PARAM_KEY:-limit}"
    export CAISO_LOAD_EXTRA_PARAM_VALUE="${CAISO_LOAD_EXTRA_PARAM_VALUE:-1000}"
    export CAISO_LOAD_SUBJECT="${CAISO_LOAD_SUBJECT:-${CAISO_LOAD_TOPIC}-value}"
    export ISO_LOAD_SCHEMA_PATH="${ISO_LOAD_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.load.v1.avsc}"
    if [[ -z "${ISO_LOAD_SCHEMA:-}" ]]; then
      ISO_LOAD_SCHEMA="$(render_schema "${ISO_LOAD_SCHEMA_PATH}")"
    fi
    export ISO_LOAD_SCHEMA
    ;;
  aeso_load_to_kafka)
    REQUIRED_VARS=(
      AESO_LOAD_ENDPOINT
      AESO_LOAD_INTERVAL_START
      AESO_LOAD_INTERVAL_END
      AESO_LOAD_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export AESO_LOAD_API_KEY="${AESO_LOAD_API_KEY:-}"
    export AESO_LOAD_JSONPATH="${AESO_LOAD_JSONPATH:-$$.data[*]}"
    export AESO_LOAD_START_FIELD="${AESO_LOAD_START_FIELD:-begin}"
    export AESO_LOAD_END_FIELD="${AESO_LOAD_END_FIELD:-end}"
    export AESO_LOAD_AREA_FIELD="${AESO_LOAD_AREA_FIELD:-region}"
    export AESO_LOAD_MW_FIELD="${AESO_LOAD_MW_FIELD:-loadMw}"
    export AESO_LOAD_START_EXTRACT="${AESO_LOAD_START_EXTRACT:-\`$AESO_LOAD_START_FIELD\`}"
    export AESO_LOAD_END_EXTRACT="${AESO_LOAD_END_EXTRACT:-\`$AESO_LOAD_END_FIELD\`}"
    export AESO_LOAD_AREA_EXPR="${AESO_LOAD_AREA_EXPR:-\`$AESO_LOAD_AREA_FIELD\`}"
    export AESO_LOAD_MW_EXPR="${AESO_LOAD_MW_EXPR:-\`$AESO_LOAD_MW_FIELD\`}"
    export AESO_LOAD_TIME_FORMAT="${AESO_LOAD_TIME_FORMAT:-yyyy-MM-dd''T''HH:mm:ss}"
    export AESO_LOAD_EXTRA_PARAM_KEY="${AESO_LOAD_EXTRA_PARAM_KEY:-limit}"
    export AESO_LOAD_EXTRA_PARAM_VALUE="${AESO_LOAD_EXTRA_PARAM_VALUE:-1000}"
    export AESO_LOAD_SUBJECT="${AESO_LOAD_SUBJECT:-${AESO_LOAD_TOPIC}-value}"
    export ISO_LOAD_SCHEMA_PATH="${ISO_LOAD_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.load.v1.avsc}"
    if [[ -z "${ISO_LOAD_SCHEMA:-}" ]]; then
      ISO_LOAD_SCHEMA="$(render_schema "${ISO_LOAD_SCHEMA_PATH}")"
    fi
    export ISO_LOAD_SCHEMA
    ;;
  pjm_genmix_to_kafka)
    REQUIRED_VARS=(
      PJM_GENMIX_ENDPOINT
      PJM_ROW_LIMIT
      PJM_INTERVAL_START
      PJM_INTERVAL_END
      PJM_GENMIX_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export PJM_API_KEY="${PJM_API_KEY:-}"
    export PJM_GENMIX_SUBJECT="${PJM_GENMIX_SUBJECT:-${PJM_GENMIX_TOPIC}-value}"
    export ISO_GENMIX_SCHEMA_PATH="${ISO_GENMIX_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.genmix.v1.avsc}"
    if [[ -z "${ISO_GENMIX_SCHEMA:-}" ]]; then
      ISO_GENMIX_SCHEMA="$(render_schema "${ISO_GENMIX_SCHEMA_PATH}")"
    fi
    export ISO_GENMIX_SCHEMA
    ;;
  miso_genmix_to_kafka)
    REQUIRED_VARS=(
      MISO_GENMIX_ENDPOINT
      MISO_GENMIX_INTERVAL_START
      MISO_GENMIX_INTERVAL_END
      MISO_GENMIX_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export MISO_GENMIX_AUTH_HEADER="${MISO_GENMIX_AUTH_HEADER:-}"
    export MISO_GENMIX_JSONPATH="${MISO_GENMIX_JSONPATH:-$$.data[*]}"
    export MISO_GENMIX_ASOF_FIELD="${MISO_GENMIX_ASOF_FIELD:-asOf}"
    export MISO_GENMIX_FUEL_FIELD="${MISO_GENMIX_FUEL_FIELD:-fuel}"
    export MISO_GENMIX_MW_FIELD="${MISO_GENMIX_MW_FIELD:-mw}"
    export MISO_GENMIX_ASOF_EXTRACT="${MISO_GENMIX_ASOF_EXTRACT:-\`$MISO_GENMIX_ASOF_FIELD\`}"
    export MISO_GENMIX_FUEL_EXPR="${MISO_GENMIX_FUEL_EXPR:-\`$MISO_GENMIX_FUEL_FIELD\`}"
    export MISO_GENMIX_MW_EXPR="${MISO_GENMIX_MW_EXPR:-\`$MISO_GENMIX_MW_FIELD\`}"
    export MISO_GENMIX_TIME_FORMAT="${MISO_GENMIX_TIME_FORMAT:-yyyy-MM-dd''T''HH:mm:ss}"
    export MISO_GENMIX_EXTRA_PARAM_KEY="${MISO_GENMIX_EXTRA_PARAM_KEY:-limit}"
    export MISO_GENMIX_EXTRA_PARAM_VALUE="${MISO_GENMIX_EXTRA_PARAM_VALUE:-1000}"
    export MISO_GENMIX_UNIT="${MISO_GENMIX_UNIT:-MW}"
    export MISO_GENMIX_SUBJECT="${MISO_GENMIX_SUBJECT:-${MISO_GENMIX_TOPIC}-value}"
    export ISO_GENMIX_SCHEMA_PATH="${ISO_GENMIX_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.genmix.v1.avsc}"
    if [[ -z "${ISO_GENMIX_SCHEMA:-}" ]]; then
      ISO_GENMIX_SCHEMA="$(render_schema "${ISO_GENMIX_SCHEMA_PATH}")"
    fi
    export ISO_GENMIX_SCHEMA
    ;;
  spp_genmix_to_kafka)
    REQUIRED_VARS=(
      SPP_GENMIX_ENDPOINT
      SPP_GENMIX_INTERVAL_START
      SPP_GENMIX_INTERVAL_END
      SPP_GENMIX_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export SPP_GENMIX_AUTH_HEADER="${SPP_GENMIX_AUTH_HEADER:-}"
    export SPP_GENMIX_JSONPATH="${SPP_GENMIX_JSONPATH:-$$.data[*]}"
    export SPP_GENMIX_ASOF_FIELD="${SPP_GENMIX_ASOF_FIELD:-asOf}"
    export SPP_GENMIX_FUEL_FIELD="${SPP_GENMIX_FUEL_FIELD:-fuel}"
    export SPP_GENMIX_MW_FIELD="${SPP_GENMIX_MW_FIELD:-mw}"
    export SPP_GENMIX_ASOF_EXTRACT="${SPP_GENMIX_ASOF_EXTRACT:-\`$SPP_GENMIX_ASOF_FIELD\`}"
    export SPP_GENMIX_FUEL_EXPR="${SPP_GENMIX_FUEL_EXPR:-\`$SPP_GENMIX_FUEL_FIELD\`}"
    export SPP_GENMIX_MW_EXPR="${SPP_GENMIX_MW_EXPR:-\`$SPP_GENMIX_MW_FIELD\`}"
    export SPP_GENMIX_TIME_FORMAT="${SPP_GENMIX_TIME_FORMAT:-yyyy-MM-dd''T''HH:mm:ss}"
    export SPP_GENMIX_EXTRA_PARAM_KEY="${SPP_GENMIX_EXTRA_PARAM_KEY:-limit}"
    export SPP_GENMIX_EXTRA_PARAM_VALUE="${SPP_GENMIX_EXTRA_PARAM_VALUE:-1000}"
    export SPP_GENMIX_UNIT="${SPP_GENMIX_UNIT:-MW}"
    export SPP_GENMIX_SUBJECT="${SPP_GENMIX_SUBJECT:-${SPP_GENMIX_TOPIC}-value}"
    export ISO_GENMIX_SCHEMA_PATH="${ISO_GENMIX_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.genmix.v1.avsc}"
    if [[ -z "${ISO_GENMIX_SCHEMA:-}" ]]; then
      ISO_GENMIX_SCHEMA="$(render_schema "${ISO_GENMIX_SCHEMA_PATH}")"
    fi
    export ISO_GENMIX_SCHEMA
    ;;
  caiso_genmix_to_kafka)
    REQUIRED_VARS=(
      CAISO_GENMIX_ENDPOINT
      CAISO_GENMIX_INTERVAL_START
      CAISO_GENMIX_INTERVAL_END
      CAISO_GENMIX_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export CAISO_GENMIX_AUTH_HEADER="${CAISO_GENMIX_AUTH_HEADER:-}"
    export CAISO_GENMIX_JSONPATH="${CAISO_GENMIX_JSONPATH:-$$.data[*]}"
    export CAISO_GENMIX_ASOF_FIELD="${CAISO_GENMIX_ASOF_FIELD:-asOf}"
    export CAISO_GENMIX_FUEL_FIELD="${CAISO_GENMIX_FUEL_FIELD:-fuel}"
    export CAISO_GENMIX_MW_FIELD="${CAISO_GENMIX_MW_FIELD:-mw}"
    export CAISO_GENMIX_ASOF_EXTRACT="${CAISO_GENMIX_ASOF_EXTRACT:-\`$CAISO_GENMIX_ASOF_FIELD\`}"
    export CAISO_GENMIX_FUEL_EXPR="${CAISO_GENMIX_FUEL_EXPR:-\`$CAISO_GENMIX_FUEL_FIELD\`}"
    export CAISO_GENMIX_MW_EXPR="${CAISO_GENMIX_MW_EXPR:-\`$CAISO_GENMIX_MW_FIELD\`}"
    export CAISO_GENMIX_TIME_FORMAT="${CAISO_GENMIX_TIME_FORMAT:-yyyy-MM-dd''T''HH:mm:ss}"
    export CAISO_GENMIX_EXTRA_PARAM_KEY="${CAISO_GENMIX_EXTRA_PARAM_KEY:-limit}"
    export CAISO_GENMIX_EXTRA_PARAM_VALUE="${CAISO_GENMIX_EXTRA_PARAM_VALUE:-1000}"
    export CAISO_GENMIX_UNIT="${CAISO_GENMIX_UNIT:-MW}"
    export CAISO_GENMIX_SUBJECT="${CAISO_GENMIX_SUBJECT:-${CAISO_GENMIX_TOPIC}-value}"
    export ISO_GENMIX_SCHEMA_PATH="${ISO_GENMIX_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.genmix.v1.avsc}"
    if [[ -z "${ISO_GENMIX_SCHEMA:-}" ]]; then
      ISO_GENMIX_SCHEMA="$(render_schema "${ISO_GENMIX_SCHEMA_PATH}")"
    fi
    export ISO_GENMIX_SCHEMA
    ;;
  aeso_genmix_to_kafka)
    REQUIRED_VARS=(
      AESO_GENMIX_ENDPOINT
      AESO_GENMIX_INTERVAL_START
      AESO_GENMIX_INTERVAL_END
      AESO_GENMIX_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export AESO_GENMIX_API_KEY="${AESO_GENMIX_API_KEY:-}"
    export AESO_GENMIX_JSONPATH="${AESO_GENMIX_JSONPATH:-$$.data[*]}"
    export AESO_GENMIX_ASOF_FIELD="${AESO_GENMIX_ASOF_FIELD:-begin}"
    export AESO_GENMIX_FUEL_FIELD="${AESO_GENMIX_FUEL_FIELD:-fuelType}"
    export AESO_GENMIX_MW_FIELD="${AESO_GENMIX_MW_FIELD:-mw}"
    export AESO_GENMIX_ASOF_EXTRACT="${AESO_GENMIX_ASOF_EXTRACT:-\`$AESO_GENMIX_ASOF_FIELD\`}"
    export AESO_GENMIX_FUEL_EXPR="${AESO_GENMIX_FUEL_EXPR:-\`$AESO_GENMIX_FUEL_FIELD\`}"
    export AESO_GENMIX_MW_EXPR="${AESO_GENMIX_MW_EXPR:-\`$AESO_GENMIX_MW_FIELD\`}"
    export AESO_GENMIX_TIME_FORMAT="${AESO_GENMIX_TIME_FORMAT:-yyyy-MM-dd''T''HH:mm:ss}"
    export AESO_GENMIX_EXTRA_PARAM_KEY="${AESO_GENMIX_EXTRA_PARAM_KEY:-limit}"
    export AESO_GENMIX_EXTRA_PARAM_VALUE="${AESO_GENMIX_EXTRA_PARAM_VALUE:-1000}"
    export AESO_GENMIX_UNIT="${AESO_GENMIX_UNIT:-MW}"
    export AESO_GENMIX_SUBJECT="${AESO_GENMIX_SUBJECT:-${AESO_GENMIX_TOPIC}-value}"
    export ISO_GENMIX_SCHEMA_PATH="${ISO_GENMIX_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.genmix.v1.avsc}"
    if [[ -z "${ISO_GENMIX_SCHEMA:-}" ]]; then
      ISO_GENMIX_SCHEMA="$(render_schema "${ISO_GENMIX_SCHEMA_PATH}")"
    fi
    export ISO_GENMIX_SCHEMA
    ;;
  pjm_pnodes_to_kafka)
    REQUIRED_VARS=(
      PJM_PNODES_ENDPOINT
      PJM_ROW_LIMIT
      PJM_PNODES_TOPIC
      PJM_PNODES_EFFECTIVE_START
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export PJM_API_KEY="${PJM_API_KEY:-}"
    export PJM_PNODES_SUBJECT="${PJM_PNODES_SUBJECT:-${PJM_PNODES_TOPIC}-value}"
    export ISO_PNODE_SCHEMA_PATH="${ISO_PNODE_SCHEMA_PATH:-${REPO_ROOT}/kafka/schemas/iso.pnode.v1.avsc}"
    if [[ -z "${ISO_PNODE_SCHEMA:-}" ]]; then
      ISO_PNODE_SCHEMA="$(render_schema "${ISO_PNODE_SCHEMA_PATH}")"
    fi
    export ISO_PNODE_SCHEMA
    ;;
  aeso_lmp_to_kafka)
    REQUIRED_VARS=(
      AESO_ENDPOINT
      AESO_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export AESO_API_KEY="${AESO_API_KEY:-}"
    export AESO_START="${AESO_START:-}"
    export AESO_END="${AESO_END:-}"
    export AESO_SUBJECT="${AESO_SUBJECT:-${AESO_TOPIC}-value}"
    ensure_iso_lmp_schema
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
  eia_series_kafka_to_timescale)
    REQUIRED_VARS=(
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
      TIMESCALE_JDBC_URL
      TIMESCALE_USER
      TIMESCALE_PASSWORD
    )
    export EIA_TOPIC_PATTERN="${EIA_TOPIC_PATTERN:-aurum\\.ref\\.eia\\..*\\.v1}"
    export EIA_SERIES_TABLE="${EIA_SERIES_TABLE:-eia_series_timeseries}"
    export EIA_SERIES_SAVE_MODE="${EIA_SERIES_SAVE_MODE:-append}"
    ;;
  fred_series_kafka_to_timescale)
    REQUIRED_VARS=(
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
      TIMESCALE_JDBC_URL
      TIMESCALE_USER
      TIMESCALE_PASSWORD
    )
    export FRED_TOPIC_PATTERN="${FRED_TOPIC_PATTERN:-aurum\\.ref\\.fred\\..*\\.v1}"
    export FRED_SERIES_TABLE="${FRED_SERIES_TABLE:-fred_series_timeseries}"
    export FRED_SERIES_SAVE_MODE="${FRED_SERIES_SAVE_MODE:-append}"
    ;;
  cpi_series_kafka_to_timescale)
    REQUIRED_VARS=(
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
      TIMESCALE_JDBC_URL
      TIMESCALE_USER
      TIMESCALE_PASSWORD
    )
    export CPI_TOPIC_PATTERN="${CPI_TOPIC_PATTERN:-aurum\\.ref\\.cpi\\..*\\.v1}"
    export CPI_SERIES_TABLE="${CPI_SERIES_TABLE:-cpi_series_timeseries}"
    export CPI_SERIES_SAVE_MODE="${CPI_SERIES_SAVE_MODE:-append}"
    ;;
  noaa_weather_kafka_to_timescale)
    REQUIRED_VARS=(
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
      TIMESCALE_JDBC_URL
      TIMESCALE_USER
      TIMESCALE_PASSWORD
    )
    export NOAA_TOPIC_PATTERN="${NOAA_TOPIC_PATTERN:-aurum\\.ref\\.noaa\\.weather\\.v1}"
    export NOAA_TABLE="${NOAA_TABLE:-noaa_weather_timeseries}"
    export NOAA_SAVE_MODE="${NOAA_SAVE_MODE:-append}"
    ;;
  nyiso_lmp_to_kafka)
    REQUIRED_VARS=(
      NYISO_URL
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export NYISO_TOPIC="${NYISO_TOPIC:-aurum.iso.nyiso.lmp.v1}"
    export NYISO_SUBJECT="${NYISO_SUBJECT:-${NYISO_TOPIC}-value}"
    ensure_iso_lmp_schema
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
      MISO_MARKET_UPPER="$(printf '%s' "${MISO_MARKET}" | tr '[:lower:]' '[:upper:]')"
      case "${MISO_MARKET_UPPER}" in
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
    ensure_iso_lmp_schema
    ;;
  miso_rtd_lmp_to_kafka)
    REQUIRED_VARS=(
      MISO_RTD_ENDPOINT
      MISO_RTD_START
      MISO_RTD_END
      MISO_RTD_TOPIC
      KAFKA_BOOTSTRAP_SERVERS
      SCHEMA_REGISTRY_URL
    )
    export MISO_RTD_MARKET="${MISO_RTD_MARKET:-RTM}"
    export MISO_RTD_REGION="${MISO_RTD_REGION:-ALL}"
    export MISO_RTD_MARKET_PARAM="${MISO_RTD_MARKET_PARAM:-${MISO_RTD_MARKET}}"
    export MISO_RTD_REGION_PARAM="${MISO_RTD_REGION_PARAM:-${MISO_RTD_REGION}}"
    export MISO_RTD_AUTH_HEADER="${MISO_RTD_AUTH_HEADER:-}"
    export MISO_RTD_JSONPATH="${MISO_RTD_JSONPATH:-\$.Items[*].Rows[*]}"
    export MISO_RTD_REF_FIELD="${MISO_RTD_REF_FIELD:-RefId}"
    export MISO_RTD_REF_EXPR="${MISO_RTD_REF_EXPR:-\`${MISO_RTD_REF_FIELD}\`}"
    export MISO_RTD_HOURMIN_FIELD="${MISO_RTD_HOURMIN_FIELD:-HourAndMin}"
    export MISO_RTD_HOURMIN_EXPR="${MISO_RTD_HOURMIN_EXPR:-\`${MISO_RTD_HOURMIN_FIELD}\`}"
    export MISO_RTD_NODE_FIELD="${MISO_RTD_NODE_FIELD:-SettlementLocation}"
    export MISO_RTD_NODE_EXPR="${MISO_RTD_NODE_EXPR:-\`${MISO_RTD_NODE_FIELD}\`}"
    export MISO_RTD_NODE_ID_FIELD="${MISO_RTD_NODE_ID_FIELD:-LocationId}"
    export MISO_RTD_NODE_ID_EXPR="${MISO_RTD_NODE_ID_EXPR:-\`${MISO_RTD_NODE_ID_FIELD}\`}"
    export MISO_RTD_LMP_FIELD="${MISO_RTD_LMP_FIELD:-SystemMarginalPrice}"
    export MISO_RTD_LMP_EXPR="${MISO_RTD_LMP_EXPR:-\`${MISO_RTD_LMP_FIELD}\`}"
    export MISO_RTD_MCC_FIELD="${MISO_RTD_MCC_FIELD:-MarginalCongestionComponent}"
    export MISO_RTD_MCC_EXPR="${MISO_RTD_MCC_EXPR:-\`${MISO_RTD_MCC_FIELD}\`}"
    export MISO_RTD_MLC_FIELD="${MISO_RTD_MLC_FIELD:-MarginalLossComponent}"
    export MISO_RTD_MLC_EXPR="${MISO_RTD_MLC_EXPR:-\`${MISO_RTD_MLC_FIELD}\`}"
    export MISO_RTD_INTERVAL_SECONDS="${MISO_RTD_INTERVAL_SECONDS:-300}"
    export MISO_RTD_TOPIC="${MISO_RTD_TOPIC:-aurum.iso.miso.lmp.v1}"
    export MISO_RTD_SUBJECT="${MISO_RTD_SUBJECT:-${MISO_RTD_TOPIC}-value}"
    export MISO_RTD_COMBINED_FORMAT="${MISO_RTD_COMBINED_FORMAT:-yyyy-MM-dd HH:mm}"
    if [[ -z "${MISO_RTD_DATE:-}" ]]; then
      MISO_RTD_DATE="${MISO_RTD_START%%T*}"
    fi
    export MISO_RTD_DATE
    ensure_iso_lmp_schema
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
    ensure_iso_lmp_schema
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
    ensure_iso_lmp_schema
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
    ensure_iso_lmp_schema
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
    ensure_iso_lmp_schema
    ;;
  *)
    REQUIRED_VARS=()
    ;;
esac

if [[ "${DESCRIBE_ONLY}" != "true" ]]; then
  for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var:-}" ]]; then
      echo "Missing required env var ${var} for job ${JOB_NAME}" >&2
      exit 1
    fi
  done
fi

if [[ "${DESCRIBE_ONLY}" == "true" ]]; then
  echo "Job: ${DESCRIBE_JOB}"
  echo "Required vars:"
  for var in "${REQUIRED_VARS[@]}"; do
    echo "  - ${var}"
  done
  echo "Optional vars (commonly used):"
  # Heuristic: show envs introduced in this case section that have defaults and look meaningful
  # (We can't statically analyze, so list a few widely-used ones)
  case "${JOB_NAME}" in
    noaa_ghcnd_to_kafka)
      printf "  - NOAA_GHCND_UNIT_CODE (default: %s)\n" "${NOAA_GHCND_UNIT_CODE:-unknown}"
      printf "  - NOAA_GHCND_STATION_LIMIT (default: %s)\n" "${NOAA_GHCND_STATION_LIMIT:-1000}"
      ;;
    eia_series_to_kafka)
      printf "  - EIA_FREQUENCY (e.g., HOURLY/DAILY)\n"
      printf "  - EIA_UNITS (e.g., USD/MWh)\n"
      ;;
    pjm_lmp_to_kafka)
      printf "  - PJM_MARKET (default: %s)\n" "${PJM_MARKET:-DAY_AHEAD}"
      printf "  - PJM_LOCATION_TYPE (default: %s)\n" "${PJM_LOCATION_TYPE:-NODE}"
      ;;
  esac
  exit 0
fi

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

# Optionally disable execution in environments where Docker is unavailable (e.g., Airflow pods)
if [[ "${AURUM_EXECUTE_SEATUNNEL:-1}" != "1" ]]; then
  echo "AURUM_EXECUTE_SEATUNNEL is disabled; rendered config at ${OUTPUT_CONFIG}" >&2
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
