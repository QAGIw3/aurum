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
- Provider hooks (to implement):
  - `_create_eia_processor(...)`
  - `_create_fred_processor(...)`
  - `_create_noaa_processor(...)`
  - `_create_worldbank_processor(...)`

All provider hooks currently raise `NotImplementedError`. Implement these to wire provider clients, checkpointing, and record emission.

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

Airflow/DAG notes:
- Use an async-compatible operator or wrap via `asyncio.run`
- Configure rate limiting per provider (e.g., serial pools) to respect upstream quotas

## Implementation Tips

- Start with EIA or FRED: re-use dataset configs (`config/eia_ingest_datasets.json`) for series selection
- Build a provider abstraction with:
  - `list_series_to_update(window_start, window_end, checkpoint)`
  - `fetch_observations(series, window_start, window_end)`
  - `emit_catalog(series)` and `emit_observation(obs)` via the collectors
- Commit checkpoint after successful batch per series/provider
- Backoff and retry on 429/5xx; respect DailyQuota/NoaaRateLimiter where applicable

## Validation

- Avro schemas: `kafka/schemas/ExtSeriesCatalogUpsertV1.avsc`, `ExtTimeseriesObsV1.avsc`
- Great Expectations suites: see `ge/expectations/external_*`
- Downstream dbt models expect tenant scoping if multi-tenant flows are used; otherwise default tenant is used by the pipelines

## Related Docs

- External integration overview: `docs/external-data.md`
- Quotas and concurrency: `docs/quotas_and_concurrency.md`
