# External Incremental Processor

Incremental processing for external data providers (EIA, FRED, NOAA, WorldBank) is scaffolded in `src/aurum/external/incremental.py`.

## Overview

- Emits two Kafka streams per run:
  - Catalog upserts: `aurum.ext.series_catalog.upsert.incremental.v1`
  - Time series observations: `aurum.ext.timeseries.obs.incremental.v1`
- Default window: last 24 hours (`DEFAULT_INCREMENTAL_WINDOW_HOURS`)
- Default schedule cadence: every 4 hours (`DEFAULT_UPDATE_FREQUENCY_MINUTES`)

## Code Structure

- `IncrementalConfig`: runtime knobs (window size, batch size, frequency)
- `IncrementalProcessor`: entry point that builds collectors and delegates to provider-specific processors
- Provider processors reuse the rich collectors in `src/aurum/external/providers` to fetch, normalize, and emit records for each source.
- `ExternalContractsPublisher` (``src/aurum/external_contracts/publisher.py``) wraps the incremental processor with a simple API that Airflow and CLIs can call.

## Collectors and Checkpointing

- Kafka collectors are created via `_build_kafka_collector(name, topic, avro_schema)`
- Checkpoints use `PostgresCheckpointStore` (configurable via env) to track last-successful timestamps per provider/series

## Running Incremental Updates

Python example:

```
import asyncio
from aurum.external.incremental import run_incremental_update

async def main():
    result = await run_incremental_update(
        provider="eia",
        vault_addr="http://localhost:8200",
        vault_token="dev-token",
        window_hours=24,
    )
    print(result)

asyncio.run(main())
```

## Airflow / Dataset Integration

- The `external_contracts_ingestion` DAG orchestrates the publisher and merge jobs.
- Schedule and lineage rely on dataset URIs built via `dataset://aurum/triggers/external/<provider>/incremental_ready` and `dataset://aurum/warehouse/external/<provider>/timeseries_observation` (see `aurum.airflow_utils.datasets`).
- Each provider run records per-source metrics (`aurum_external_contract_publish_total`, `aurum_external_contract_merge_records_total`) and advances Postgres watermarks via `aurum.db.watermark.update_ingest_watermark`.
- Freshness gauges (`aurum_external_data_freshness_hours`) are updated using the DAG execution timestamp to provide dashboard visibility.

## Implementation Tips

- Provider dataset manifests (`config/eia_ingest_datasets.json`, `config/external_incremental_config.json`) control series filters and rate limits; CI validates the manifests via the "External Contracts" workflow.
- Checkpointing is automatic via `PostgresCheckpointStore`; ensure `AURUM_COLLECTOR_CHECKPOINT_DSN` (or `AURUM_APP_DB_DSN`) is configured in the Airflow environment.
- Collectors automatically encode Kafka keys as `provider|series_id`, enabling per-series partitioning and idempotent merges.
- Respect upstream rate limits by configuring the incremental settings or the provider-specific rate limiter classes (`NoaaRateLimiter`, `DailyQuota`).

## Validation

- Avro schemas: `kafka/schemas/ExtSeriesCatalogUpsertV1.avsc`, `ExtTimeseriesObsV1.avsc`
- Great Expectations suites: see `ge/expectations/external_*`
- The `TrinoExternalContractsConsumer` materializes Kafka payloads via `sql/merge/catalog_merge.sql` and `sql/merge/obs_merge.sql`; fixtures in `scripts/test_fixtures/load_external_fixtures.py` exercise the same code paths.
- Downstream dbt models expect tenant scoping if multi-tenant flows are used; otherwise the canonical pipelines populate the default tenant.

## Related Docs

- External integration overview: `docs/external-data.md`
- Quotas and concurrency: `docs/quotas_and_concurrency.md`
