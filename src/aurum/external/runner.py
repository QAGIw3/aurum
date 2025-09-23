from __future__ import annotations

"""CLI runner for external data collectors (EIA, FRED, NOAA, WorldBank).

Provides a unified entrypoint to sync catalogs and ingest observations either
once or in a managed loop. Emission targets Kafka topics with Avro schemas or
can be configured to run in HTTP/no‑op mode for debugging.
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List

from aurum.external.collect import CollectorConfig, ExternalCollector
from aurum.external.collect.base import RetryConfig
from aurum.compat.requests import ResilienceConfig
from aurum.external.collect.checkpoints import PostgresCheckpointStore
from aurum.external.quota_manager import get_quota_manager, DatasetQuota, configure_dataset_quotas
from aurum.external.providers import (
    EiaApiClient,
    EiaCollector,
    FredApiClient,
    FredCollector,
    DailyQuota,
    NoaaApiClient,
    NoaaCollector,
    NoaaDatasetConfig,
    NoaaRateLimiter,
    WorldBankApiClient,
    WorldBankCollector,
    load_eia_dataset_configs,
    load_fred_dataset_configs,
    load_noaa_dataset_configs,
    load_worldbank_dataset_configs,
)

CATALOG_TOPIC = "aurum.ext.series_catalog.upsert.v1"
OBS_TOPIC = "aurum.ext.timeseries.obs.v1"
DEFAULT_PROVIDERS = ("eia", "fred", "noaa", "worldbank")
SCHEMA_FILES = {
    "catalog": "ExtSeriesCatalogUpsertV1.avsc",
    "observation": "ExtTimeseriesObsV1.avsc",
}

logger = logging.getLogger("aurum.external.runner")


def _load_schema(name: str) -> Dict[str, object]:
    schema_dir = Path(os.getenv("AURUM_SCHEMA_DIR", Path(__file__).resolve().parents[3] / "kafka" / "schemas"))
    schema_path = schema_dir / name
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    return json.loads(schema_path.read_text(encoding="utf-8"))


def _build_kafka_collector(provider: str, topic: str, schema_name: str) -> ExternalCollector:
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")
    if not schema_registry_url:
        raise RuntimeError("SCHEMA_REGISTRY_URL must be set for collector emission")
    value_schema = _load_schema(schema_name)
    config = CollectorConfig(
        provider=provider,
        base_url="kafka://",
        kafka_topic=topic,
        kafka_bootstrap_servers=kafka_servers,
        schema_registry_url=schema_registry_url,
        value_schema=value_schema,
        kafka_config={
            "linger.ms": 250,
            "batch.num.messages": 500,
        },
        resilience_config=ResilienceConfig(
            timeout=30.0,
            max_retries=3,
            backoff_factor=0.5,
            circuit_breaker_failure_threshold=5,
            circuit_breaker_recovery_timeout=60.0,
        ),
    )
    return ExternalCollector(config)


def _build_http_collector(provider: str, base_url: str, *, headers: Dict[str, str] | None = None) -> ExternalCollector:
    config = CollectorConfig(
        provider=provider,
        base_url=base_url,
        kafka_topic=f"{provider}.noop",
        default_headers=headers or {},
        retry=RetryConfig(max_attempts=5, backoff_factor=0.5, max_backoff_seconds=30.0),
        resilience_config=ResilienceConfig(
            timeout=30.0,
            max_retries=3,
            backoff_factor=0.5,
            circuit_breaker_failure_threshold=5,
            circuit_breaker_recovery_timeout=60.0,
        ),
    )
    return ExternalCollector(config)


async def _build_checkpoint_store() -> Any:
    """Build checkpoint store using watermark store for consistency."""
    try:
        from aurum.external.collect.checkpoints import WatermarkCheckpointStore
        from aurum.data_ingestion.watermark_store import WatermarkStore
        watermark_store = WatermarkStore()
        return WatermarkCheckpointStore(watermark_store)
    except ImportError:
        # Fallback to Postgres checkpoint store
        from aurum.external.collect.checkpoints import PostgresCheckpointStore
        dsn = os.getenv("AURUM_COLLECTOR_CHECKPOINT_DSN", os.getenv("AURUM_APP_DB_DSN", "postgresql://aurum:aurum@postgres:5432/aurum"))
        return PostgresCheckpointStore(dsn=dsn)


async def _configure_dataset_quotas() -> None:
    """Configure dataset quotas for external providers."""
    quotas = []

    # EIA quotas
    for data_type in ['electricity', 'petroleum', 'natural_gas']:
        quotas.append(DatasetQuota(
            dataset_id=f"eia_{data_type}",
            iso_code="EIA",
            max_records_per_hour=500_000,
            max_bytes_per_hour=500_000_000,
            max_concurrent_requests=3,
            max_requests_per_minute=30,
            max_requests_per_hour=1000,
            priority=1
        ))

    # FRED quotas
    quotas.append(DatasetQuota(
        dataset_id="fred_economic",
        iso_code="FRED",
        max_records_per_hour=200_000,
        max_bytes_per_hour=200_000_000,
        max_concurrent_requests=2,
        max_requests_per_minute=20,
        max_requests_per_hour=500,
        priority=2
    ))

    # NOAA quotas
    quotas.append(DatasetQuota(
        dataset_id="noaa_weather",
        iso_code="NOAA",
        max_records_per_hour=100_000,
        max_bytes_per_hour=100_000_000,
        max_concurrent_requests=5,
        max_requests_per_minute=10,
        max_requests_per_hour=300,
        priority=3
    ))

    # World Bank quotas
    quotas.append(DatasetQuota(
        dataset_id="worldbank_development",
        iso_code="WB",
        max_records_per_hour=50_000,
        max_bytes_per_hour=50_000_000,
        max_concurrent_requests=2,
        max_requests_per_minute=15,
        max_requests_per_hour=200,
        priority=4
    ))

    await get_quota_manager()
    await configure_dataset_quotas(quotas)


async def run_eia(*, catalog: bool, observations: bool) -> None:
    # Configure quotas for EIA
    await _configure_dataset_quotas()

    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        logger.warning("Skipping EIA collector; EIA_API_KEY is not set")
        return
    base_url = os.getenv("EIA_API_BASE_URL", "https://api.eia.gov/v2/")
    http_collector = _build_http_collector("eia-http", base_url)
    api_client = EiaApiClient(http_collector, api_key=api_key)

    # Configure resilience for EIA provider
    if hasattr(http_collector, 'resilient_session') and http_collector.resilient_session:
        from aurum.compat.requests import ResilienceConfig
        http_collector.resilient_session.config = ResilienceConfig(
            timeout=30.0,
            max_retries=3,
            backoff_factor=0.5,
            circuit_breaker_failure_threshold=5,
            circuit_breaker_recovery_timeout=60.0,
        )
    datasets = load_eia_dataset_configs()
    store = await _build_checkpoint_store()
    catalog_collector = _build_kafka_collector("eia-catalog", CATALOG_TOPIC, SCHEMA_FILES["catalog"])
    obs_collector = _build_kafka_collector("eia-observation", OBS_TOPIC, SCHEMA_FILES["observation"])
    for dataset in datasets:
        collector = EiaCollector(
            dataset,
            api_client=api_client,
            catalog_collector=catalog_collector,
            observation_collector=obs_collector,
            checkpoint_store=store,
        )
        if catalog:
            count = collector.sync_catalog()
            logger.info("EIA catalog", extra={"dataset": dataset.source_name, "records": count})
        if observations:
            count = collector.ingest_observations()
            logger.info("EIA observations", extra={"dataset": dataset.source_name, "records": count})
    catalog_collector.flush()
    obs_collector.flush()


async def run_fred(*, catalog: bool, observations: bool) -> None:
    # Configure quotas for FRED
    await _configure_dataset_quotas()

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        logger.warning("Skipping FRED collector; FRED_API_KEY is not set")
        return
    base_url = os.getenv("FRED_API_BASE_URL", "https://api.stlouisfed.org/")
    http_collector = _build_http_collector("fred-http", base_url)
    api_client = FredApiClient(http_collector, api_key=api_key)
    datasets = load_fred_dataset_configs()
    store = await _build_checkpoint_store()
    catalog_collector = _build_kafka_collector("fred-catalog", CATALOG_TOPIC, SCHEMA_FILES["catalog"])
    obs_collector = _build_kafka_collector("fred-observation", OBS_TOPIC, SCHEMA_FILES["observation"])
    for dataset in datasets:
        collector = FredCollector(
            dataset,
            api_client=api_client,
            catalog_collector=catalog_collector,
            observation_collector=obs_collector,
            checkpoint_store=store,
        )
        if catalog:
            count = collector.sync_catalog()
            logger.info("FRED catalog", extra={"series": dataset.series_id, "records": count})
        if observations:
            count = collector.ingest_observations()
            logger.info("FRED observations", extra={"series": dataset.series_id, "records": count})
    catalog_collector.flush()
    obs_collector.flush()


async def run_noaa(*, catalog: bool, observations: bool) -> None:
    # Configure quotas for NOAA
    await _configure_dataset_quotas()

    token = os.getenv("NOAA_GHCND_TOKEN")
    if not token:
        logger.warning("Skipping NOAA collector; NOAA_GHCND_TOKEN is not set")
        return
    base_url = os.getenv("NOAA_API_BASE_URL", "https://www.ncdc.noaa.gov/cdo-web/api/v2")
    rate = float(os.getenv("NOAA_RATE_LIMIT_RPS", "5"))
    daily_quota = os.getenv("NOAA_DAILY_QUOTA")
    quota_limit = int(daily_quota) if daily_quota else 0
    http_collector = _build_http_collector("noaa-http", base_url)
    rate_limiter = NoaaRateLimiter(rate_per_sec=rate)
    quota = None
    if quota_limit:
        quota = DailyQuota(limit=quota_limit)
    api_client = NoaaApiClient(http_collector, token=token, rate_limiter=rate_limiter, quota=quota, base_url=base_url)
    datasets = load_noaa_dataset_configs()
    store = await _build_checkpoint_store()
    catalog_collector = _build_kafka_collector("noaa-catalog", CATALOG_TOPIC, SCHEMA_FILES["catalog"])
    obs_collector = _build_kafka_collector("noaa-observation", OBS_TOPIC, SCHEMA_FILES["observation"])
    for dataset in datasets:
        collector = NoaaCollector(
            dataset,
            api_client=api_client,
            catalog_collector=catalog_collector,
            observation_collector=obs_collector,
            checkpoint_store=store,
        )
        if catalog:
            count = collector.sync_catalog()
            logger.info("NOAA catalog", extra={"dataset": dataset.dataset, "records": count})
        if observations:
            count = collector.ingest_observations()
            logger.info("NOAA observations", extra={"dataset": dataset.dataset, "records": count})
    catalog_collector.flush()
    obs_collector.flush()


async def run_worldbank(*, catalog: bool, observations: bool) -> None:
    # Configure quotas for World Bank
    await _configure_dataset_quotas()

    base_url = os.getenv("WORLD_BANK_API_BASE_URL", "https://api.worldbank.org/v2")
    http_collector = _build_http_collector("worldbank-http", base_url)
    api_client = WorldBankApiClient(http_collector, base_url=base_url)
    datasets = load_worldbank_dataset_configs()
    store = await _build_checkpoint_store()
    catalog_collector = _build_kafka_collector("worldbank-catalog", CATALOG_TOPIC, SCHEMA_FILES["catalog"])
    obs_collector = _build_kafka_collector("worldbank-observation", OBS_TOPIC, SCHEMA_FILES["observation"])
    for dataset in datasets:
        collector = WorldBankCollector(
            dataset,
            api_client=api_client,
            catalog_collector=catalog_collector,
            observation_collector=obs_collector,
            checkpoint_store=store,
        )
        if catalog:
            count = collector.sync_catalog()
            logger.info("WorldBank catalog", extra={"indicator": dataset.indicator_id, "records": count})
        if observations:
            count = collector.ingest_observations()
            logger.info("WorldBank observations", extra={"indicator": dataset.indicator_id, "records": count})
    catalog_collector.flush()
    obs_collector.flush()


RUNNERS = {
    "eia": run_eia,
    "fred": run_fred,
    "noaa": run_noaa,
    "worldbank": run_worldbank,
}


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run external data collectors")
    parser.add_argument(
        "--providers",
        default=",".join(DEFAULT_PROVIDERS),
        help="Comma-separated list of providers to run (default: eia,fred,noaa,worldbank)",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run collectors continuously with a sleep interval",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("AURUM_COLLECTORS_INTERVAL_SECONDS", "3600")),
        help="Sleep interval between collector runs in seconds when --loop is set",
    )
    parser.add_argument(
        "--catalog-only",
        action="store_true",
        help="Sync catalog only (skip observations)",
    )
    parser.add_argument(
        "--observations-only",
        action="store_true",
        help="Ingest observations only (skip catalog)",
    )
    return parser.parse_args(argv)


async def run_once(providers: Iterable[str], *, catalog: bool, observations: bool) -> None:
    for provider in providers:
        runner = RUNNERS.get(provider.lower().strip())
        if not runner:
            logger.warning("Unknown provider %s", provider)
            continue
        try:
            await runner(catalog=catalog, observations=observations)
        except Exception:
            logger.exception("Collector run failed", extra={"provider": provider})


async def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(level=os.getenv("AURUM_COLLECTOR_LOG_LEVEL", "INFO"))
    args = parse_args(argv)
    providers = [p for p in (part.strip() for part in args.providers.split(",")) if p]
    if not providers:
        logger.error("No providers selected")
        return 1
    run_catalog = True
    run_observations = True
    if args.catalog_only and args.observations_only:
        logger.error("Cannot use --catalog-only and --observations-only together")
        return 1
    if args.catalog_only:
        run_observations = False
    if args.observations_only:
        run_catalog = False
    if args.loop:
        interval = max(60, args.interval)
        logger.info("Starting collector loop", extra={"interval_seconds": interval, "providers": providers})
        while True:
            await run_once(providers, catalog=run_catalog, observations=run_observations)
            time.sleep(interval)
    else:
        await run_once(providers, catalog=run_catalog, observations=run_observations)
    return 0


if __name__ == "__main__":
    import asyncio
    sys.exit(asyncio.run(main()))
