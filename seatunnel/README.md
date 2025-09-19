# SeaTunnel Jobs

This directory contains SeaTunnel job templates used to ingest external data sources into Kafka.

## Usage

1. Ensure Docker is available locally and the Kafka cluster/Schema Registry are running.
2. Export the environment variables required by the job template. For the NOAA GHCND job this includes at least:

   ```bash
   export NOAA_GHCND_TOKEN=...
   export NOAA_GHCND_START_DATE=2024-01-01
   export NOAA_GHCND_END_DATE=2024-01-02
   export NOAA_GHCND_TOPIC=aurum.ref.noaa.weather.v1
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   export SCHEMA_REGISTRY_URL=http://localhost:8081
   # Optional tweaks
   export NOAA_GHCND_UNIT_CODE=degC
   export NOAA_GHCND_STATION_LIMIT=500
   ```

3. Render and run the job:

   ```bash
   scripts/seatunnel/run_job.sh noaa_ghcnd_to_kafka
   ```

   Use `--render-only` to output the generated config without executing the job. The rendered configuration is written to `seatunnel/jobs/generated/<job>.conf` by default.
   Edit the rendered configuration if you need to add NOAA filters such as `locationid` or `datatypeid` before running the job manually.

### Loading secrets from Vault

Store API credentials in Vault following the convention `secret/data/aurum/<source>` (e.g. `secret/data/aurum/noaa`, `secret/data/aurum/eia`). Export them into the environment before rendering a job:

```bash
export VAULT_ADDR=https://vault.example.com
export VAULT_TOKEN=...
eval "$(python scripts/secrets/pull_vault_env.py \
  --mapping secret/data/aurum/noaa:token=NOAA_GHCND_TOKEN \
  --mapping secret/data/aurum/eia:api_key=EIA_API_KEY)"
```

Set additional mappings for AESO/NEPOOL credentials as needed (e.g. `--mapping secret/data/aurum/aeso:token=AESO_API_TOKEN`). To seed Vault from local environment variables, use `python scripts/secrets/push_vault_env.py --mapping NOAA_GHCND_TOKEN=secret/data/aurum/noaa:token ...` once per secret.

### Local Vault dev server

Start a throwaway Vault instance using Docker:

```bash
scripts/vault/run_dev.sh
```

This exposes Vault at `http://localhost:8200` with root token `aurum-dev-token` (override with `VAULT_DEV_ROOT_TOKEN`). After the container is running, seed secrets via `push_vault_env.py` and then pull them into the environment before executing SeaTunnel jobs.

## Templates

- `noaa_ghcnd_to_kafka.conf.tmpl`: Fetches NOAA GHCND daily observations over HTTP, enriches them with station metadata from the `/stations` endpoint, and publishes Avro records (Confluent framing) to Kafka aligned with `aurum.ref.noaa.weather.v1`. Optional filters such as `locationid` or `datatypeid` can be added to the rendered config before execution.
- `eia_series_to_kafka.conf.tmpl`: Calls the EIA v2 API for a series path and emits Avro records matching `aurum.ref.eia.series.v1`. Provide `EIA_SERIES_PATH`, `EIA_SERIES_ID`, and `EIA_FREQUENCY`; adjust pagination or filters by editing the rendered configuration before running the job.
- `fred_series_to_kafka.conf.tmpl`: Pulls observations for a given FRED series ID and emits Avro records aligned with `aurum.ref.fred.series.v1`. Set `FRED_SERIES_ID`, frequency, and seasonal adjustment; the API key is read from Vault.
- `pjm_lmp_to_kafka.conf.tmpl`: Queries PJM Data Miner for day-ahead LMPs and emits Avro records matching `aurum.iso.*.lmp.v1`. Configure interval start/end and the Data Miner token via Vault.
- `iso_lmp_kafka_to_timescale.conf.tmpl`: Reads all ISO LMP Kafka topics and writes rows into TimescaleDB via JDBC. Configure the topic pattern and Timescale connection environment variables before execution.
- Not using SeaTunnel for CAISO/ ERCOT yet? Helper scripts `scripts/ingest/caiso_prc_lmp_to_kafka.py` and `scripts/ingest/ercot_mis_to_kafka.py` normalize the OASIS/MIS payloads and publish Avro records to Kafka.
- `nyiso_lmp_to_kafka.conf.tmpl`: Fetches NYISO LBMP CSV data over HTTP, maps columns to the ISO LMP schema, and publishes to Kafka.
- `miso_lmp_to_kafka.conf.tmpl`: Downloads MISO market report CSVs (configurable DA/RT) and maps their columns into the ISO LMP Avro schema. Column names and interval length can be overridden via environment variables.
- `isone_lmp_to_kafka.conf.tmpl`: Calls the ISO-NE JSON web services, handles optional basic/bearer auth, and projects results into the ISO LMP schema before emitting to Kafka.
- `caiso_lmp_to_kafka.conf.tmpl`: Reads staged CAISO JSON payloads (typically produced by `scripts/ingest/caiso_prc_lmp_to_kafka.py --output-json`) and publishes Avro messages to the CAISO topic.
- `ercot_lmp_to_kafka.conf.tmpl`: Consumes normalized ERCOT MIS observations from a JSON file and emits them to Kafka using the shared ISO LMP schema.
- `spp_lmp_to_kafka.conf.tmpl`: Loads SPP Marketplace LMP records from staged JSON and writes them to Kafka.

Add additional job templates following the same pattern and extend `scripts/seatunnel/run_job.sh` to declare required environment variables per job.
