"""Backfill orchestration for external providers."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from aurum.external.collect import CollectorConfig, ExternalCollector
from aurum.external.collect.base import (
    CollectorContext,
    RateLimitConfig,
    RetryConfig,
)
from aurum.external.collect.checkpoints import CheckpointStore, PostgresCheckpointStore
from aurum.external.providers.fred import (
    FredApiClient,
    FredCollector,
    FredDatasetConfig,
    load_fred_dataset_configs,
)

logger = logging.getLogger(__name__)

# Backfill topics mirror the incremental pipeline but isolate historical loads
BACKFILL_CATALOG_TOPIC = "aurum.ext.series_catalog.upsert.backfill.v1"
BACKFILL_OBS_TOPIC = "aurum.ext.timeseries.obs.backfill.v1"

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "external_backfill_config.json"
UTC = timezone.utc


def _parse_datetime(value: Optional[str]) -> datetime:
    """Parse ISO8601 strings into timezone-aware datetimes."""
    if not value:
        raise ValueError("Date value is required for backfill configuration")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class ProviderBackfillConfig:
    """Declarative backfill settings for a provider."""

    name: str
    datasets: Tuple[str, ...]
    start_date: datetime
    end_date: datetime
    priority: str = "medium"
    max_workers: int = 4
    batch_size: int = 1000
    rate_limit_rps: float = 2.0
    daily_quota: Optional[int] = None

    @property
    def slug(self) -> str:
        return self.name.lower()


@dataclass(frozen=True)
class BackfillSchedulingConfig:
    max_concurrent_backfills: int = 2
    priority_order: Tuple[str, ...] = ("high", "medium", "low")
    retry_failed_datasets: bool = True
    resume_from_checkpoint: bool = True


@dataclass(frozen=True)
class BackfillQualityConfig:
    run_validation: bool = False
    validation_threshold: float = 1.0
    require_lineage_tracking: bool = False
    generate_quality_reports: bool = False


@dataclass(frozen=True)
class BackfillPerformanceConfig:
    memory_limit_gb: Optional[int] = None
    cpu_limit: Optional[int] = None
    timeout_hours: Optional[int] = None
    progress_reporting_interval_minutes: Optional[int] = None


@dataclass(frozen=True)
class BackfillSettings:
    providers: Tuple[ProviderBackfillConfig, ...]
    scheduling: BackfillSchedulingConfig
    quality: BackfillQualityConfig
    performance: BackfillPerformanceConfig


def load_backfill_settings(path: Path | None = None) -> BackfillSettings:
    """Load external backfill settings from JSON."""
    config_path = path or DEFAULT_CONFIG_PATH
    payload = json.loads(Path(config_path).read_text(encoding="utf-8"))

    provider_settings: List[ProviderBackfillConfig] = []
    for entry in payload.get("providers", []):
        provider_settings.append(
            ProviderBackfillConfig(
                name=entry["name"],
                datasets=tuple(entry.get("datasets", [])),
                start_date=_parse_datetime(entry.get("start_date")),
                end_date=_parse_datetime(entry.get("end_date")),
                priority=entry.get("priority", "medium"),
                max_workers=int(entry.get("max_workers", 4)),
                batch_size=int(entry.get("batch_size", 1000)),
                rate_limit_rps=float(entry.get("rate_limit_rps", 2.0)),
                daily_quota=entry.get("daily_quota"),
            )
        )

    scheduling_payload = payload.get("scheduling", {})
    scheduling = BackfillSchedulingConfig(
        max_concurrent_backfills=int(scheduling_payload.get("max_concurrent_backfills", 2)),
        priority_order=tuple(scheduling_payload.get("priority_order", ["high", "medium", "low"])),
        retry_failed_datasets=bool(scheduling_payload.get("retry_failed_datasets", True)),
        resume_from_checkpoint=bool(scheduling_payload.get("resume_from_checkpoint", True)),
    )

    quality_payload = payload.get("quality", {})
    quality = BackfillQualityConfig(
        run_validation=bool(quality_payload.get("run_validation", False)),
        validation_threshold=float(quality_payload.get("validation_threshold", 1.0)),
        require_lineage_tracking=bool(quality_payload.get("require_lineage_tracking", False)),
        generate_quality_reports=bool(quality_payload.get("generate_quality_reports", False)),
    )

    performance_payload = payload.get("performance", {})
    performance = BackfillPerformanceConfig(
        memory_limit_gb=performance_payload.get("memory_limit_gb"),
        cpu_limit=performance_payload.get("cpu_limit"),
        timeout_hours=performance_payload.get("timeout_hours"),
        progress_reporting_interval_minutes=performance_payload.get("progress_reporting_interval_minutes"),
    )

    return BackfillSettings(
        providers=tuple(provider_settings),
        scheduling=scheduling,
        quality=quality,
        performance=performance,
    )


@dataclass
class BackfillResult:
    provider: str
    dataset: str
    status: str
    catalog_records: int = 0
    observation_records: int = 0
    errors: int = 0
    error_message: Optional[str] = None
    duration_seconds: float = 0.0


@asynccontextmanager
async def _semaphore(sem: asyncio.Semaphore):
    await sem.acquire()
    try:
        yield
    finally:
        sem.release()


class BackfillRunner:
    """Coordinate provider-specific backfills based on configuration."""

    def __init__(
        self,
        settings: BackfillSettings,
        *,
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        self.settings = settings
        self._checkpoint_store = checkpoint_store

    @classmethod
    def from_file(cls, path: Path | None = None) -> "BackfillRunner":
        return cls(load_backfill_settings(path))

    async def run(self, providers: Optional[Sequence[str]] = None) -> List[BackfillResult]:
        target = {p.lower() for p in providers} if providers else None
        jobs = self._plan_jobs(target)
        if not jobs:
            return []

        concurrency = max(1, self.settings.scheduling.max_concurrent_backfills)
        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            asyncio.create_task(self._run_job(provider_cfg, dataset, semaphore))
            for provider_cfg, dataset in jobs
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        materialized: List[BackfillResult] = []
        for result in results:
            if isinstance(result, BackfillResult):
                materialized.append(result)
            else:
                logger.exception("Backfill job failed", exc_info=result)
        return materialized

    def _plan_jobs(
        self, target: Optional[set[str]]
    ) -> List[Tuple[ProviderBackfillConfig, str]]:
        groups: Dict[str, List[Tuple[ProviderBackfillConfig, str]]] = {}
        for cfg in self.settings.providers:
            if target and cfg.slug not in target:
                continue
            for dataset in cfg.datasets:
                groups.setdefault(cfg.priority.lower(), []).append((cfg, dataset))

        ordered: List[Tuple[ProviderBackfillConfig, str]] = []
        for priority in self.settings.scheduling.priority_order:
            ordered.extend(groups.get(priority.lower(), []))
        # Append any priorities not listed explicitly at the end to avoid starvation
        listed = {p.lower() for p in self.settings.scheduling.priority_order}
        for priority, entries in groups.items():
            if priority not in listed:
                ordered.extend(entries)
        return ordered

    async def _run_job(
        self,
        provider_cfg: ProviderBackfillConfig,
        dataset: str,
        semaphore: asyncio.Semaphore,
    ) -> BackfillResult:
        async with _semaphore(semaphore):
            manager = BackfillManager(
                provider_cfg,
                dataset,
                checkpoint_store=self._checkpoint_store,
                resume_from_checkpoint=self.settings.scheduling.resume_from_checkpoint,
            )
            try:
                return await manager.run()
            except Exception as exc:  # pragma: no cover - logged and surfaced to caller
                logger.exception(
                    "Backfill execution failed",
                    extra={"provider": provider_cfg.name, "dataset": dataset},
                )
                if not self.settings.scheduling.retry_failed_datasets:
                    raise
                return BackfillResult(
                    provider=provider_cfg.slug,
                    dataset=dataset,
                    status="failed",
                    errors=1,
                    error_message=str(exc),
                )


class BackfillManager:
    """Execute a backfill for a single provider/dataset pair."""

    _fred_datasets: Optional[List[FredDatasetConfig]] = None

    def __init__(
        self,
        provider_cfg: ProviderBackfillConfig,
        dataset: str,
        *,
        checkpoint_store: CheckpointStore | None = None,
        resume_from_checkpoint: bool = True,
    ) -> None:
        self.provider_cfg = provider_cfg
        self.dataset = dataset
        self.resume_from_checkpoint = resume_from_checkpoint
        self._context = CollectorContext()
        self._checkpoint_store = checkpoint_store or _build_checkpoint_store()

    async def run(self) -> BackfillResult:
        start_ts = datetime.now(tz=UTC)
        logger.info(
            "Starting backfill",
            extra={
                "provider": self.provider_cfg.slug,
                "dataset": self.dataset,
                "start_date": self.provider_cfg.start_date.isoformat(),
                "end_date": self.provider_cfg.end_date.isoformat(),
            },
        )

        catalog_collector = _build_kafka_collector(
            f"{self.provider_cfg.slug}-backfill-catalog",
            BACKFILL_CATALOG_TOPIC,
            "ExtSeriesCatalogUpsertV1.avsc",
            context=self._context,
        )
        obs_collector = _build_kafka_collector(
            f"{self.provider_cfg.slug}-backfill-obs",
            BACKFILL_OBS_TOPIC,
            "ExtTimeseriesObsV1.avsc",
            context=self._context,
        )

        if not self.resume_from_checkpoint:
            self._clear_checkpoint()

        try:
            collector = await self._create_provider_collector(
                catalog_collector,
                obs_collector,
            )
            stats = await self._execute_backfill(collector)
            status = "success"
        except Exception as exc:
            logger.exception(
                "Backfill job failed",
                extra={"provider": self.provider_cfg.slug, "dataset": self.dataset},
            )
            status = "failed"
            stats = {"errors": 1, "catalog_records": 0, "observation_records": 0}
            error_message = str(exc)
        else:
            error_message = None
        finally:
            # Ensure collectors drain network buffers even on failure
            for collector in (catalog_collector, obs_collector):
                try:
                    collector.flush()
                except Exception:  # pragma: no cover - defensive
                    pass
                finally:
                    collector.close()

        duration = (datetime.now(tz=UTC) - start_ts).total_seconds()
        return BackfillResult(
            provider=self.provider_cfg.slug,
            dataset=self.dataset,
            status=status,
            catalog_records=stats.get("catalog_records", 0),
            observation_records=stats.get("observation_records", 0),
            errors=stats.get("errors", 0),
            error_message=error_message,
            duration_seconds=duration,
        )

    def _clear_checkpoint(self) -> None:
        provider_key = self.provider_cfg.slug.upper()
        try:
            self._checkpoint_store.delete(provider_key, self.dataset)
        except Exception:  # pragma: no cover - store failures should not abort run
            logger.warning(
                "Failed to clear checkpoint before backfill",
                exc_info=True,
                extra={"provider": provider_key, "dataset": self.dataset},
            )

    async def _create_provider_collector(
        self,
        catalog_collector: ExternalCollector,
        obs_collector: ExternalCollector,
    ) -> Any:
        slug = self.provider_cfg.slug
        if slug == "fred":
            return self._create_fred_collector(catalog_collector, obs_collector)
        raise ValueError(f"Unsupported provider for backfill: {slug}")

    async def _execute_backfill(self, collector: Any) -> Dict[str, int]:
        errors = 0
        catalog_records = 0
        observation_records = 0

        try:
            catalog_records = collector.sync_catalog()
        except Exception:
            errors += 1
            logger.exception(
                "Catalog sync failed",
                extra={"provider": self.provider_cfg.slug, "dataset": self.dataset},
            )

        try:
            observation_records = collector.ingest_observations(
                start=self.provider_cfg.start_date,
                end=self.provider_cfg.end_date,
            )
        except Exception:
            errors += 1
            logger.exception(
                "Observation ingestion failed",
                extra={"provider": self.provider_cfg.slug, "dataset": self.dataset},
            )

        return {
            "catalog_records": catalog_records,
            "observation_records": observation_records,
            "errors": errors,
        }

    def _create_fred_collector(
        self,
        catalog_collector: ExternalCollector,
        obs_collector: ExternalCollector,
    ) -> FredCollector:
        datasets = self._hydrate_fred_datasets()
        dataset_cfg = next((cfg for cfg in datasets if cfg.series_id == self.dataset), None)
        if not dataset_cfg:
            raise ValueError(f"Unknown FRED dataset: {self.dataset}")

        if dataset_cfg.page_limit != self.provider_cfg.batch_size:
            dataset_cfg = replace(dataset_cfg, page_limit=self.provider_cfg.batch_size)

        api_key = os.getenv("FRED_API_KEY")
        if not api_key:
            raise RuntimeError("FRED_API_KEY must be set for backfill")
        base_url = os.getenv("FRED_API_BASE_URL", "https://api.stlouisfed.org/")
        http_collector = _build_http_collector(
            "fred-http",
            base_url,
            rate_limit=self.provider_cfg.rate_limit_rps,
            context=self._context,
        )
        api_client = FredApiClient(http_collector, api_key=api_key)
        return FredCollector(
            dataset_cfg,
            api_client=api_client,
            catalog_collector=catalog_collector,
            observation_collector=obs_collector,
            checkpoint_store=self._checkpoint_store,
        )

    @classmethod
    def _hydrate_fred_datasets(cls) -> List[FredDatasetConfig]:
        if cls._fred_datasets is None:
            cls._fred_datasets = load_fred_dataset_configs()
        return cls._fred_datasets


def _load_schema(name: str) -> Dict[str, object]:
    schema_dir = Path(
        os.getenv(
            "AURUM_SCHEMA_DIR",
            Path(__file__).resolve().parents[3] / "kafka" / "schemas",
        )
    )
    schema_path = schema_dir / name
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    return json.loads(schema_path.read_text(encoding="utf-8"))


def _build_kafka_collector(
    provider: str,
    topic: str,
    schema_name: str,
    *,
    context: CollectorContext,
) -> ExternalCollector:
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")
    if not schema_registry_url:
        raise RuntimeError("SCHEMA_REGISTRY_URL must be set for backfill emission")
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
    )
    return ExternalCollector(config, context=context)


def _build_http_collector(
    provider: str,
    base_url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    rate_limit: Optional[float] = None,
    context: CollectorContext,
) -> ExternalCollector:
    rate_cfg = RateLimitConfig(rate=rate_limit, burst=max(1.0, rate_limit)) if rate_limit else None
    config = CollectorConfig(
        provider=provider,
        base_url=base_url,
        kafka_topic=f"{provider}.noop",
        default_headers=headers or {},
        retry=RetryConfig(max_attempts=5, backoff_factor=0.5, max_backoff_seconds=30.0),
        rate_limit=rate_cfg,
    )
    return ExternalCollector(config, context=context)


def _build_checkpoint_store() -> PostgresCheckpointStore:
    dsn = os.getenv(
        "AURUM_COLLECTOR_CHECKPOINT_DSN",
        os.getenv("AURUM_APP_DB_DSN", "postgresql://aurum:aurum@postgres:5432/aurum"),
    )
    return PostgresCheckpointStore(dsn=dsn)

