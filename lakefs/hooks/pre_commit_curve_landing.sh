#!/usr/bin/env bash
# Validate landing dataset with the curve landing expectation suite before committing to lakeFS.
set -euo pipefail

DATA_PATH=${1:-}
FORMAT=${2:-parquet}
SUITE_PATH=${AURUM_GE_LANDING_SUITE:-ge/expectations/curve_landing.json}

if [[ -z "$DATA_PATH" ]]; then
  echo "Usage: $0 <path-to-dataset> [parquet|csv]" >&2
  exit 1
fi

python - "$DATA_PATH" "$FORMAT" "$SUITE_PATH" <<'PY'
import sys
from pathlib import Path

import pandas as pd

from aurum.dq.validator import enforce_expectation_suite

if len(sys.argv) != 4:
    raise SystemExit("Expected dataset path, format, and suite path")

path = Path(sys.argv[1])
fmt = sys.argv[2].lower()
suite = Path(sys.argv[3])

if not path.exists():
    raise SystemExit(f"Dataset not found: {path}")
if fmt not in {"parquet", "csv"}:
    raise SystemExit(f"Unsupported format '{fmt}'")

if fmt == "parquet":
    df = pd.read_parquet(path)
else:
    df = pd.read_csv(path)

enforce_expectation_suite(df, suite, suite_name="curve_landing")
PY

echo "curve landing expectation suite passed for ${DATA_PATH}"
