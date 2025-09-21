#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
COMPOSE_FILE="${ROOT_DIR}/../compose/docker-compose.dev.yml"
PYTHONPATH="${ROOT_DIR}/../src:${PYTHONPATH:-}"
export PYTHONPATH

function cleanup() {
  docker compose -f "${COMPOSE_FILE}" down -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

# Start minimal lakehouse stack.
docker compose -f "${COMPOSE_FILE}" up -d minio nessie trino

echo "Waiting for Trino to become ready..."
for _ in {1..60}; do
  if curl -sf "http://localhost:8080/v1/info" >/dev/null; then
    break
  fi
  sleep 2
else
  echo "Trino did not become ready in time" >&2
  exit 1
fi

echo "Applying external schema DDL..."
for sql_file in $(ls "${ROOT_DIR}/../sql/iceberg/external"/*.sql | sort); do
  python3 "${ROOT_DIR}/trino/run_sql.py" \
    --server "http://localhost:8080" \
    --user "ci" \
    --catalog "iceberg" \
    --schema "external" \
    "${sql_file}"
done

echo "Loading sample fixtures via Trino..."
python3 "${ROOT_DIR}/test_fixtures/load_external_fixtures.py" \
  --host localhost \
  --port 8080 \
  --user ci \
  --catalog iceberg \
  --schema external

echo "Verifying smoke queries..."
python3 - <<'PY'
from trino.dbapi import connect

with connect(host="localhost", port=8080, user="ci", catalog="iceberg", schema="external") as conn:
    cur = conn.cursor()
    cur.execute("SHOW TABLES IN iceberg.external")
    tables = {row[0] if isinstance(row, (list, tuple)) else row for row in cur.fetchall()}
    expected = {
        "dataset",
        "frequency",
        "geo",
        "series_catalog",
        "timeseries_observation",
        "unit",
    }
    missing = expected - tables
    if missing:
        raise SystemExit(f"Missing external tables: {missing}")

    cur.execute(
        "SELECT provider, series_id, COUNT(*) FROM iceberg.external.timeseries_observation GROUP BY 1, 2 ORDER BY 1, 2"
    )
    rows = cur.fetchall()
    if not rows:
        raise SystemExit("No observation rows found after fixture load")
    print("Observation counts:", rows)

    cur.execute(
        "SELECT provider, series_id, title FROM iceberg.external.series_catalog ORDER BY provider, series_id"
    )
    catalogs = cur.fetchall()
    if not catalogs:
        raise SystemExit("No series catalog rows found after fixture load")
    print("Catalog entries:", catalogs)
PY

echo "External contract smoke tests passed."
