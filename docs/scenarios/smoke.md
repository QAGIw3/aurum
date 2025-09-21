# Scenario Engine Smoke Test

This smoke test exercises the scenario worker end-to-end against the compose stack.

> CI runs this script on every pull request using a Docker-enabled runner and blocks merges if the smoke fails. If you see failures in GitHub Actions, reproduce locally with the steps below.

## Prerequisites

- `docker` and `docker compose`
- `confluent-kafka[avro]` installed in the local Python environment (used by the smoke script)
- Compose stack built with the latest images: `docker compose -f compose/docker-compose.dev.yml build`

## Steps

1. Run the automated smoke harness (brings up the minimal stack, produces a request, and verifies Iceberg output). This script now executes in CI for every pull request, so local runs should pass without additional setup beyond Docker:

   ```bash
   scripts/ci/scenario_smoke.sh
   ```

   The script tears the stack down automatically. Skip to the notes below if you prefer to run the steps manually.

### Manual run (optional)

1. Start the core services required by the scenario pipeline:

   ```bash
   docker compose -f compose/docker-compose.dev.yml up -d postgres nessie kafka schema-registry trino scenario-worker
   ```

2. Produce a sample scenario request (the script emits a single policy assumption and will create a deterministic output row). The defaults match the compose configuration.

   ```bash
   python scripts/dev/scenario_smoke.py
   ```

   The script prints the generated `scenario_id` and confirms that the record was produced.

3. Query the materialised view to confirm the worker appended a row into Iceberg:

   ```bash
   docker compose -f compose/docker-compose.dev.yml exec trino trino \
     --execute "SELECT scenario_id, tenant_id, metric, value FROM iceberg.market.scenario_output_latest ORDER BY computed_ts DESC LIMIT 1"
   ```

   You should see the scenario emitted in the previous step. The `value` field is derived deterministically from the policy assumption payload.

4. Tear down when finished:

   ```bash
   docker compose -f compose/docker-compose.dev.yml down
   ```

This workflow is not wired into the default test suite so it can run on-demand without slowing down unit tests.

### API validation

Once the worker has produced output, the API should expose the same data. Replace `SCENARIO_ID` and `TENANT_ID` with values printed by the smoke script:

```bash
export BASE_URL=${BASE_URL:-http://localhost:8095}
export TENANT_ID=${TENANT_ID:-00000000-0000-0000-0000-000000000001}

curl -H "X-Aurum-Tenant: $TENANT_ID" "$BASE_URL/v1/scenarios/SCENARIO_ID/outputs?limit=5"
curl -H "X-Aurum-Tenant: $TENANT_ID" "$BASE_URL/v1/scenarios/SCENARIO_ID/metrics/latest"
```

Both endpoints should return JSON payloads with non-empty `data` arrays and include `meta.next_cursor`/`meta.prev_cursor` when pagination is available.

### CLI quickstart

The `aurum-scenario` helper wraps the API with a lightweight CLI. Examples:

```bash
# List scenarios for the tenant
aurum-scenario --tenant "$TENANT_ID" list

# Inspect runs for a scenario
aurum-scenario --tenant "$TENANT_ID" runs SCENARIO_ID

# Fetch outputs (tabular output by default)
aurum-scenario --tenant "$TENANT_ID" outputs SCENARIO_ID --limit 10
```

Pass `--cursor`, `--prev-cursor`, or `--json` to tailor pagination and formatting. The CLI reads `AURUM_API_BASE_URL`, `AURUM_TENANT_ID`, and `AURUM_API_TOKEN` from the environment when available.

### Troubleshooting

- **No rows in Iceberg:** ensure the worker service is running and check the worker logs for ingestion errors.
- **API returns 503:** confirm the API container is part of the compose profile and the health endpoint (`/health`) succeeds.
- **Empty CLI/API responses:** verify the tenant header matches the tenant used when producing the scenario request.
- **Expectation failures in DAGs:** inspect `ge/expectations/scenario_output.json` for the enforced value ranges and adjust environment overrides (e.g., `GE_SCENARIO_METRIC_MID_MAX`) when tuning thresholds.

### MISO RT LMP ingestion sanity check

The new MISO Data Broker integration produces five-minute LMP observations through both the Python ingest script and the optional SeaTunnel job.

1. Export credentials and request parameters (values below are placeholders – adjust the window, market, and region for your test):

   ```bash
   export AURUM_MISO_RTDB_BASE=https://api.misoenergy.org
   export AURUM_MISO_RTDB_ENDPOINT=/MISORTDBService/rest/data/getLMPConsolidatedTable
   export AURUM_MISO_RTDB_MARKET=RTM
   export AURUM_MISO_RTDB_REGION=ALL
   export AURUM_MISO_RTDB_HEADERS="Ocp-Apim-Subscription-Key:demo-key"
   export START_TS=2024-05-01T10:00:00-05:00
   export END_TS=2024-05-01T10:15:00-05:00
   ```

2. Fetch a sample payload directly (useful when debugging credentials or query parameters):

   ```bash
   curl -H "${AURUM_MISO_RTDB_HEADERS%%||*}" \
     "${AURUM_MISO_RTDB_BASE}${AURUM_MISO_RTDB_ENDPOINT}?start=${START_TS}&end=${END_TS}&market=${AURUM_MISO_RTDB_MARKET}&region=${AURUM_MISO_RTDB_REGION}" | jq .
   ```

   The response should contain `RefId`, `HourAndMin`, and price components for one or more settlement locations.

3. Dry-run the Python publisher to confirm the JSON→Avro mapping:

   ```bash
   ${VENV_PYTHON:-venv311/bin/python} scripts/ingest/miso_rtdb_to_kafka.py \
     --base-url "$AURUM_MISO_RTDB_BASE" \
     --endpoint "$AURUM_MISO_RTDB_ENDPOINT" \
     --start "$START_TS" --end "$END_TS" \
     --market "$AURUM_MISO_RTDB_MARKET" --region "$AURUM_MISO_RTDB_REGION" \
     --interval-seconds 300 --dry-run | jq '.[0]'
   ```

   Replace `--dry-run` with Kafka/Schema Registry coordinates to publish records (`--bootstrap-servers`, `--schema-registry`).

4. Render the SeaTunnel job if you prefer a containerised ingest:

   ```bash
   MISO_RTD_ENDPOINT="$AURUM_MISO_RTDB_BASE$AURUM_MISO_RTDB_ENDPOINT" \
   MISO_RTD_START="$START_TS" MISO_RTD_END="$END_TS" \
   MISO_RTD_TOPIC=aurum.iso.miso.lmp.v1 \
   MISO_RTD_HEADERS="$AURUM_MISO_RTDB_HEADERS" \
   KAFKA_BOOTSTRAP_SERVERS=broker:29092 \
   SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
   scripts/seatunnel/run_job.sh miso_rtd_lmp_to_kafka --render-only
   ```

5. Register or update schemas after changing LMP ingestion logic:

   ```bash
   make kafka-register-schemas kafka-set-compat
   ```

Use the existing LMP QA dashboards (or `trino/ddl/iso_lmp_views.sql`) to confirm the new records appear alongside other ISO feeds.
