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
