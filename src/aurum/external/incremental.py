"""Incremental data processing for external providers."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aurum.external.collect import ExternalCollector
from aurum.external.collect.base import CollectorContext
from aurum.external.collect.checkpoints import PostgresCheckpointStore
from aurum.external.providers import (
    load_eia_dataset_configs,
    load_fred_dataset_configs,
    load_noaa_dataset_configs,
    load_worldbank_dataset_configs,
    EiaApiClient,
    EiaCollector,
    FredApiClient,
    FredCollector,
    NoaaApiClient,
    NoaaCollector,
    NoaaDatasetConfig,
    NoaaRateLimiter,
    DailyQuota,
    WorldBankApiClient,
    WorldBankCollector,
)
# Reuse helper builders from the batch runner
from aurum.external.runner import (
    _build_http_collector,
    _build_kafka_collector,
    _build_checkpoint_store,
)

logger = logging.getLogger(__name__)

# Kafka topics for incremental updates
INCREMENTAL_CATALOG_TOPIC = "aurum.ext.series_catalog.upsert.incremental.v1"
INCREMENTAL_OBS_TOPIC = "aurum.ext.timeseries.obs.incremental.v1"

# Incremental processing configuration
DEFAULT_INCREMENTAL_WINDOW_HOURS = 24  # Process last 24 hours by default
DEFAULT_UPDATE_FREQUENCY_MINUTES = 240  # Update every 4 hours


class IncrementalConfig:
    """Configuration for incremental processing."""

    def __init__(
        self,
        provider: str,
        window_hours: int = DEFAULT_INCREMENTAL_WINDOW_HOURS,
        update_frequency_minutes: int = DEFAULT_UPDATE_FREQUENCY_MINUTES,
        max_records_per_batch: int = 1000,
        continue_on_error: bool = True
    ):
        self.provider = provider
        self.window_hours = window_hours
        self.update_frequency_minutes = update_frequency_minutes
        self.max_records_per_batch = max_records_per_batch
        self.continue_on_error = continue_on_error


class IncrementalProcessor:
    """Processor for incremental external data updates."""

    def __init__(self, config: IncrementalConfig):
        self.config = config
        self._checkpoint_store = _build_checkpoint_store()
        self._context = CollectorContext()

    async def run_incremental_update(self, provider: str) -> Dict[str, Any]:
        """Run incremental update for a provider.

        Args:
            provider: External data provider name

        Returns:
            Update results with statistics
        """
        start_time = datetime.now()
        logger.info(
            "Starting incremental update",
            extra={
                "provider": provider,
                "window_hours": self.config.window_hours
            }
        )

        try:
            # Create collectors
            catalog_collector = _build_kafka_collector(
                f"{provider}-incremental-catalog",
                INCREMENTAL_CATALOG_TOPIC,
                "ExtSeriesCatalogUpsertV1.avsc"
            )
            obs_collector = _build_kafka_collector(
                f"{provider}-incremental-obs",
                INCREMENTAL_OBS_TOPIC,
                "ExtTimeseriesObsV1.avsc"
            )

            # Create provider-specific processor
            processor = await self._create_provider_processor(
                provider,
                catalog_collector,
                obs_collector
            )

            # Run incremental processing
            results = await self._execute_incremental_update(processor)

            duration = (datetime.now() - start_time).total_seconds()

            logger.info(
                "Incremental update completed",
                extra={
                    "provider": provider,
                    "duration_seconds": duration,
                    "results": results
                }
            )

            return {
                "status": "success",
                "provider": provider,
                "duration_seconds": duration,
                "results": results
            }

        except Exception as e:
            logger.error(
                "Incremental update failed",
                extra={
                    "provider": provider,
                    "error": str(e)
                }
            )
            raise

    async def _create_provider_processor(self, provider: str, catalog_collector: ExternalCollector, obs_collector: ExternalCollector) -> Any:
        """Create provider-specific incremental processor."""
        if provider.lower() == "eia":
            return await self._create_eia_processor(catalog_collector, obs_collector)
        elif provider.lower() == "fred":
            return await self._create_fred_processor(catalog_collector, obs_collector)
        elif provider.lower() == "noaa":
            return await self._create_noaa_processor(catalog_collector, obs_collector)
        elif provider.lower() == "worldbank":
            return await self._create_worldbank_processor(catalog_collector, obs_collector)
        else:
            raise ValueError(f"Unsupported provider: {provider}")

    async def _create_eia_processor(self, catalog_collector: ExternalCollector, obs_collector: ExternalCollector) -> Any:
        """Create EIA incremental processor."""
        return _EiaIncrementalProcessor(catalog_collector, obs_collector, self._checkpoint_store)

    async def _create_fred_processor(self, catalog_collector: ExternalCollector, obs_collector: ExternalCollector) -> Any:
        """Create FRED incremental processor."""
        return _FredIncrementalProcessor(catalog_collector, obs_collector, self._checkpoint_store)

    async def _create_noaa_processor(self, catalog_collector: ExternalCollector, obs_collector: ExternalCollector) -> Any:
        """Create NOAA incremental processor."""
        return _NoaaIncrementalProcessor(catalog_collector, obs_collector, self._checkpoint_store)

    async def _create_worldbank_processor(self, catalog_collector: ExternalCollector, obs_collector: ExternalCollector) -> Any:
        """Create WorldBank incremental processor."""
        return _WorldBankIncrementalProcessor(catalog_collector, obs_collector, self._checkpoint_store)

    async def _execute_incremental_update(self, processor: Any) -> Dict[str, Any]:
        """Execute the incremental update process (provider-specific)."""
        return await processor.run(window_hours=self.config.window_hours, max_records=self.config.max_records_per_batch)


class _StubProviderProcessor:
    """Minimal provider processor stub to enable pipeline wiring and testing."""

    def __init__(self, name: str, catalog_collector: ExternalCollector, obs_collector: ExternalCollector, checkpoint_store: Any):
        self.name = name
        self.catalog = catalog_collector
        self.obs = obs_collector
        self.checkpoints = checkpoint_store

    async def run(self, window_hours: int, max_records: int) -> Dict[str, Any]:
        # Placeholder: In a real implementation, query provider for series updated since checkpoint
        # and emit zero or more records. We just report zero processed here.
        return {
            "provider": self.name,
            "window_hours": window_hours,
            "records_processed": 0,
            "records_updated": 0,
            "records_created": 0,
            "errors": 0,
            "warnings": 0,
        }


class _FredIncrementalProcessor:
    """Incremental processor for FRED that loads configured datasets and emits updates.

    Uses the same HTTP/Kafka helpers as the batch runner and respects checkpoints
    so only new observations are emitted.
    """

    def __init__(
        self,
        catalog_collector: ExternalCollector,
        obs_collector: ExternalCollector,
        checkpoint_store: Any,
    ) -> None:
        self.catalog = catalog_collector
        self.obs = obs_collector
        self.checkpoints = checkpoint_store

    async def run(self, *, window_hours: int, max_records: int) -> Dict[str, Any]:
        import os
        from datetime import datetime, timedelta, timezone

        api_key = os.getenv("FRED_API_KEY")
        base_url = os.getenv("FRED_API_BASE_URL", "https://api.stlouisfed.org/")

        # Build a lightweight HTTP client for FRED
        http = _build_http_collector("fred-http", base_url)
        api_client = FredApiClient(http, api_key=api_key or "")

        datasets = load_fred_dataset_configs()

        total_catalog = 0
        total_obs = 0
        errors = 0

        window_start = datetime.now(timezone.utc) - timedelta(hours=max(1, window_hours))

        for ds in datasets:
            try:
                collector = FredCollector(
                    ds,
                    api_client=api_client,
                    catalog_collector=self.catalog,
                    observation_collector=self.obs,
                    checkpoint_store=self.checkpoints,
                )
                # Always sync catalog in incremental runs to pick up metadata changes
                total_catalog += collector.sync_catalog()
                # Ingest observations within the configured window
                total_obs += collector.ingest_observations(start=window_start)
            except Exception:
                errors += 1

        # Flush collectors to ensure delivery
        try:
            self.catalog.flush()
        except Exception:
            pass
        try:
            self.obs.flush()
        except Exception:
            pass

        return {
            "provider": "fred",
            "datasets": len(datasets),
            "catalog_records": total_catalog,
            "observation_records": total_obs,
            "errors": errors,
        }


class _EiaIncrementalProcessor:
    """Incremental processor for EIA datasets.

    Loads configured datasets, respects per-series checkpoints, and emits
    catalog + observation records within a recent window.
    """

    def __init__(
        self,
        catalog_collector: ExternalCollector,
        obs_collector: ExternalCollector,
        checkpoint_store: Any,
    ) -> None:
        self.catalog = catalog_collector
        self.obs = obs_collector
        self.checkpoints = checkpoint_store

    async def run(self, *, window_hours: int, max_records: int) -> Dict[str, Any]:
        import os
        from datetime import datetime, timedelta, timezone

        api_key = os.getenv("EIA_API_KEY")
        base_url = os.getenv("EIA_API_BASE_URL", "https://api.eia.gov/v2/")

        http = _build_http_collector("eia-http", base_url)
        api_client = EiaApiClient(http, api_key=api_key or "")

        datasets = load_eia_dataset_configs()

        total_catalog = 0
        total_obs = 0
        errors = 0

        window_start = datetime.now(timezone.utc) - timedelta(hours=max(1, window_hours))

        for ds in datasets:
            try:
                collector = EiaCollector(
                    ds,
                    api_client=api_client,
                    catalog_collector=self.catalog,
                    observation_collector=self.obs,
                    checkpoint_store=self.checkpoints,
                )
                total_catalog += collector.sync_catalog()
                total_obs += collector.ingest_observations(start=window_start)
            except Exception:
                errors += 1

        try:
            self.catalog.flush()
        except Exception:
            pass
        try:
            self.obs.flush()
        except Exception:
            pass

        return {
            "provider": "eia",
            "datasets": len(datasets),
            "catalog_records": total_catalog,
            "observation_records": total_obs,
            "errors": errors,
        }


class _NoaaIncrementalProcessor:
    """Incremental processor for NOAA CDO datasets with rate-limit and quota support."""

    def __init__(
        self,
        catalog_collector: ExternalCollector,
        obs_collector: ExternalCollector,
        checkpoint_store: Any,
    ) -> None:
        self.catalog = catalog_collector
        self.obs = obs_collector
        self.checkpoints = checkpoint_store

    async def run(self, *, window_hours: int, max_records: int) -> Dict[str, Any]:
        import os
        from datetime import datetime, timedelta, timezone

        token = os.getenv("NOAA_GHCND_TOKEN")
        base_url = os.getenv("NOAA_API_BASE_URL", "https://www.ncdc.noaa.gov/cdo-web/api/v2")
        rate = float(os.getenv("NOAA_RATE_LIMIT_RPS", "5"))
        daily_quota = os.getenv("NOAA_DAILY_QUOTA")
        quota_limit = int(daily_quota) if daily_quota else 0

        http = _build_http_collector("noaa-http", base_url)
        rate_limiter = NoaaRateLimiter(rate_per_sec=rate)
        quota = DailyQuota(limit=quota_limit) if quota_limit else None
        api_client = NoaaApiClient(http, token=token or "", rate_limiter=rate_limiter, quota=quota, base_url=base_url)

        datasets = load_noaa_dataset_configs()

        total_catalog = 0
        total_obs = 0
        errors = 0

        window_start = datetime.now(timezone.utc) - timedelta(hours=max(1, window_hours))

        for ds in datasets:
            try:
                collector = NoaaCollector(
                    ds,
                    api_client=api_client,
                    catalog_collector=self.catalog,
                    observation_collector=self.obs,
                    checkpoint_store=self.checkpoints,
                )
                total_catalog += collector.sync_catalog()
                # NOAA collector takes day windows; pass start and let collector deduce proper range
                total_obs += collector.ingest_observations(start=window_start)
            except Exception:
                errors += 1

        try:
            self.catalog.flush()
        except Exception:
            pass
        try:
            self.obs.flush()
        except Exception:
            pass

        return {
            "provider": "noaa",
            "datasets": len(datasets),
            "catalog_records": total_catalog,
            "observation_records": total_obs,
            "errors": errors,
        }


class _WorldBankIncrementalProcessor:
    """Incremental processor for World Bank indicators."""

    def __init__(
        self,
        catalog_collector: ExternalCollector,
        obs_collector: ExternalCollector,
        checkpoint_store: Any,
    ) -> None:
        self.catalog = catalog_collector
        self.obs = obs_collector
        self.checkpoints = checkpoint_store

    async def run(self, *, window_hours: int, max_records: int) -> Dict[str, Any]:
        import os

        base_url = os.getenv("WORLD_BANK_API_BASE_URL", "https://api.worldbank.org/v2")
        http = _build_http_collector("worldbank-http", base_url)
        api_client = WorldBankApiClient(http, base_url=base_url)

        datasets = load_worldbank_dataset_configs()

        total_catalog = 0
        total_obs = 0
        errors = 0

        for ds in datasets:
            try:
                collector = WorldBankCollector(
                    ds,
                    api_client=api_client,
                    catalog_collector=self.catalog,
                    observation_collector=self.obs,
                    checkpoint_store=self.checkpoints,
                )
                total_catalog += collector.sync_catalog()
                total_obs += collector.ingest_observations()
            except Exception:
                errors += 1

        try:
            self.catalog.flush()
        except Exception:
            pass
        try:
            self.obs.flush()
        except Exception:
            pass

        return {
            "provider": "worldbank",
            "datasets": len(datasets),
            "catalog_records": total_catalog,
            "observation_records": total_obs,
            "errors": errors,
        }


# Convenience function for DAGs
async def run_incremental_update(
    provider: str,
    vault_addr: str,
    vault_token: str,
    window_hours: Optional[int] = None
) -> Dict[str, Any]:
    """Run incremental update for a provider.

    Args:
        provider: External data provider (eia, fred, noaa, worldbank)
        vault_addr: Vault address for secrets
        vault_token: Vault token for authentication
        window_hours: Optional window size in hours (overrides default)

    Returns:
        Update results dictionary
    """
    config = IncrementalConfig(
        provider=provider,
        window_hours=window_hours or DEFAULT_INCREMENTAL_WINDOW_HOURS
    )

    processor = IncrementalProcessor(config)
    return await processor.run_incremental_update(provider)
