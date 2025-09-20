# Scenario Engine Smoke Test

This smoke test exercises the scenario worker end-to-end against the compose stack.

## Prerequisites

- `docker` and `docker compose`
- `confluent-kafka[avro]` installed in the local Python environment (used by the smoke script)
- Compose stack built with the latest images: `docker compose -f compose/docker-compose.dev.yml build`

## Steps

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
