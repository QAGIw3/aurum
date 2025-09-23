"""Warehouse maintenance jobs for Timescale, ClickHouse, and Iceberg."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Sequence

import asyncpg

try:  # optional dependency for ClickHouse operations
    from clickhouse_driver import Client as ClickHouseClient  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    ClickHouseClient = None  # type: ignore

from aurum.core import AurumSettings
from aurum.telemetry.context import log_structured

from ..api.database.trino_client import get_trino_client

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimescaleHypertableConfig:
    """Maintenance configuration for a Timescale hypertable."""

    name: str
    compress_after: timedelta
    retention: timedelta


@dataclass(frozen=True)
class ClickHouseTableConfig:
    """Maintenance configuration for a ClickHouse MergeTree table."""

    name: str
    ttl_days: int
    timestamp_column: str = "timestamp"

    @property
    def ttl_expression(self) -> str:
        return f"{self.timestamp_column} + INTERVAL {self.ttl_days} DAY"


@dataclass(frozen=True)
class IcebergTableConfig:
    """Maintenance configuration for an Iceberg table."""

    fully_qualified_name: str  # catalog.schema.table
    snapshot_retention_days: int = 7


@dataclass
class WarehouseMaintenanceConfig:
    """Aggregated configuration for warehouse maintenance jobs."""

    timescale_tables: Sequence[TimescaleHypertableConfig] = field(default_factory=tuple)
    timescale_interval_seconds: int = 3600
    clickhouse_tables: Sequence[ClickHouseTableConfig] = field(default_factory=tuple)
    clickhouse_interval_seconds: int = 7200
    iceberg_tables: Sequence[IcebergTableConfig] = field(default_factory=tuple)
    iceberg_interval_seconds: int = 21600

    trino_concurrency: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_settings(cls, settings: AurumSettings) -> "WarehouseMaintenanceConfig":
        """Build configuration from Aurum settings and environment overrides."""
        default_timescale = (
            TimescaleHypertableConfig(
                name="aurum.market.curve_observation",
                compress_after=timedelta(days=7),
                retention=timedelta(days=90),
            ),
            TimescaleHypertableConfig(
                name="aurum.market.scenario_output",
                compress_after=timedelta(days=7),
                retention=timedelta(days=60),
            ),
            TimescaleHypertableConfig(
                name="aurum.market.ppa_valuation",
                compress_after=timedelta(days=14),
                retention=timedelta(days=120),
            ),
        )

        default_clickhouse = (
            ClickHouseTableConfig(name="aurum_logs", ttl_days=45, timestamp_column="timestamp"),
        )

        default_iceberg = (
            IcebergTableConfig(
                fully_qualified_name=f"{settings.trino.catalog}.{settings.trino.database_schema}.scenario_runs",
                snapshot_retention_days=14,
            ),
            IcebergTableConfig(
                fully_qualified_name=f"{settings.trino.catalog}.{settings.trino.database_schema}.curve_cache",
                snapshot_retention_days=7,
            ),
        )

        concurrency_cfg = (
            settings.api.concurrency.model_dump() if hasattr(settings.api, "concurrency") else {}
        )

        return cls(
            timescale_tables=default_timescale,
            clickhouse_tables=default_clickhouse,
            iceberg_tables=default_iceberg,
            timescale_interval_seconds=3600,
            clickhouse_interval_seconds=7200,
            iceberg_interval_seconds=21600,
            trino_concurrency=concurrency_cfg,
        )


@dataclass
class WarehouseMaintenanceMetrics:
    """Runtime metrics for warehouse maintenance tasks."""

    last_run: Optional[datetime] = None
    timescale_chunks_compressed: int = 0
    timescale_chunks_dropped: int = 0
    timescale_failures: int = 0
    iceberg_tables_compacted: int = 0
    iceberg_metadata_pruned: int = 0
    iceberg_failures: int = 0
    clickhouse_tables_optimized: int = 0
    clickhouse_ttl_updates: int = 0
    clickhouse_failures: int = 0
    clickhouse_runs_skipped: int = 0


class TimescaleMaintenanceJob:
    """Periodic TimescaleDB maintenance for compression and retention."""

    def __init__(
        self,
        dsn: str,
        tables: Sequence[TimescaleHypertableConfig],
        interval_seconds: int,
        metrics: WarehouseMaintenanceMetrics,
    ) -> None:
        self._dsn = dsn
        self._tables = tables
        self._interval = max(300, interval_seconds)
        self._metrics = metrics
        self._pool: Optional[asyncpg.Pool] = None
        self._task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()

    async def start(self) -> None:
        if not self._tables:
            LOGGER.info("Timescale maintenance disabled - no tables configured")
            return

        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5, command_timeout=60)
        self._task = asyncio.create_task(self._run_loop(), name="timescale-maintenance")

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def run_once(self) -> None:
        if not self._tables:
            return
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5, command_timeout=60)
        await self._maintain_all()

    async def _run_loop(self) -> None:
        assert self._pool is not None
        while not self._shutdown.is_set():
            try:
                await self._maintain_all()
            except Exception as exc:  # pragma: no cover - defensive
                self._metrics.timescale_failures += 1
                LOGGER.error("Timescale maintenance loop failed: %s", exc, exc_info=True)
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                continue

    async def _maintain_all(self) -> None:
        assert self._pool is not None
        for table_cfg in self._tables:
            await self._compress_chunks(table_cfg)
            await self._drop_old_chunks(table_cfg)

    async def _compress_chunks(self, table_cfg: TimescaleHypertableConfig) -> None:
        assert self._pool is not None
        query = (
            "SELECT compress_chunk(chunk) "
            "FROM show_chunks($1::regclass, older_than => $2) AS chunk"
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, table_cfg.name, table_cfg.compress_after)
            compressed = len(rows)
            self._metrics.timescale_chunks_compressed += compressed
            if compressed:
                log_structured(
                    "info",
                    "timescale_chunks_compressed",
                    table=table_cfg.name,
                    chunks=compressed,
                    older_than_days=int(table_cfg.compress_after.total_seconds() // 86400),
                )

    async def _drop_old_chunks(self, table_cfg: TimescaleHypertableConfig) -> None:
        assert self._pool is not None
        query = "SELECT drop_chunks($1::regclass, $2)"
        async with self._pool.acquire() as conn:
            dropped = await conn.fetchval(query, table_cfg.name, table_cfg.retention)
            if dropped:
                self._metrics.timescale_chunks_dropped += 1
                log_structured(
                    "info",
                    "timescale_chunks_dropped",
                    table=table_cfg.name,
                    retention_days=int(table_cfg.retention.total_seconds() // 86400),
                )


class IcebergMaintenanceJob:
    """Periodic Iceberg maintenance using Trino."""

    def __init__(
        self,
        settings: AurumSettings,
        tables: Sequence[IcebergTableConfig],
        interval_seconds: int,
        metrics: WarehouseMaintenanceMetrics,
        concurrency_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._settings = settings
        self._tables = tables
        self._interval = max(1800, interval_seconds)
        self._metrics = metrics
        self._task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()
        self._concurrency = concurrency_config or {}

    async def start(self) -> None:
        if not self._tables:
            LOGGER.info("Iceberg maintenance disabled - no tables configured")
            return
        self._task = asyncio.create_task(self._run_loop(), name="iceberg-maintenance")

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def run_once(self) -> None:
        if not self._tables:
            return
        await self._maintain_all()

    async def _run_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self._maintain_all()
            except Exception as exc:  # pragma: no cover - defensive
                self._metrics.iceberg_failures += 1
                LOGGER.error("Iceberg maintenance loop failed: %s", exc, exc_info=True)
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                continue

    async def _maintain_all(self) -> None:
        client = get_trino_client(concurrency_config=self._concurrency)
        for table_cfg in self._tables:
            await self._compact_table(client, table_cfg)
            await self._prune_metadata(client, table_cfg)

    async def _compact_table(self, client, table_cfg: IcebergTableConfig) -> None:
        query = f"ALTER TABLE {table_cfg.fully_qualified_name} EXECUTE optimize"
        await client.execute_query(query)
        self._metrics.iceberg_tables_compacted += 1
        log_structured("info", "iceberg_table_compacted", table=table_cfg.fully_qualified_name)

    async def _prune_metadata(self, client, table_cfg: IcebergTableConfig) -> None:
        retention = table_cfg.snapshot_retention_days
        query = (
            f"ALTER TABLE {table_cfg.fully_qualified_name} EXECUTE expire_snapshots("\
            f"retention_threshold => INTERVAL '{retention}' DAY)"
        )
        await client.execute_query(query)
        self._metrics.iceberg_metadata_pruned += 1
        log_structured(
            "info",
            "iceberg_metadata_pruned",
            table=table_cfg.fully_qualified_name,
            retention_days=retention,
        )


class ClickHouseMaintenanceJob:
    """Periodic ClickHouse maintenance for partition optimization and TTL enforcement."""

    def __init__(
        self,
        settings: AurumSettings,
        tables: Sequence[ClickHouseTableConfig],
        interval_seconds: int,
        metrics: WarehouseMaintenanceMetrics,
    ) -> None:
        self._settings = settings
        self._tables = tables
        self._interval = max(1800, interval_seconds)
        self._metrics = metrics
        self._task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()
        self._client: Optional[Any] = None
        self._driver_available = ClickHouseClient is not None

    async def start(self) -> None:
        if not self._tables:
            LOGGER.info("ClickHouse maintenance disabled - no tables configured")
            return
        if not self._driver_available:
            LOGGER.warning("clickhouse-driver not installed; skipping ClickHouse maintenance")
            self._metrics.clickhouse_runs_skipped += 1
            return
        loop = asyncio.get_running_loop()
        self._client = await loop.run_in_executor(
            None,
            lambda: ClickHouseClient(
                host=self._settings.data_backend.clickhouse_host,
                port=self._settings.data_backend.clickhouse_port,
                user=self._settings.data_backend.clickhouse_user,
                password=self._settings.data_backend.clickhouse_password,
                database=self._settings.data_backend.clickhouse_database,
            ),
        )
        self._task = asyncio.create_task(self._run_loop(), name="clickhouse-maintenance")

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._client and hasattr(self._client, "disconnect"):
            self._client.disconnect()
            self._client = None

    async def run_once(self) -> None:
        if not self._tables:
            return
        if not self._driver_available:
            self._metrics.clickhouse_runs_skipped += 1
            LOGGER.debug("Skipping ClickHouse maintenance run - clickhouse-driver unavailable")
            return
        if self._client is None and ClickHouseClient is not None:
            loop = asyncio.get_running_loop()
            self._client = await loop.run_in_executor(
                None,
                lambda: ClickHouseClient(
                    host=self._settings.data_backend.clickhouse_host,
                    port=self._settings.data_backend.clickhouse_port,
                    user=self._settings.data_backend.clickhouse_user,
                    password=self._settings.data_backend.clickhouse_password,
                    database=self._settings.data_backend.clickhouse_database,
                ),
            )
        await self._maintain_all()

    async def _run_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self._maintain_all()
            except Exception as exc:  # pragma: no cover - defensive
                self._metrics.clickhouse_failures += 1
                LOGGER.error("ClickHouse maintenance loop failed: %s", exc, exc_info=True)
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                continue

    async def _maintain_all(self) -> None:
        if self._client is None:
            return
        for table_cfg in self._tables:
            await self._optimize_table(table_cfg)
            await self._enforce_ttl(table_cfg)

    async def _optimize_table(self, table_cfg: ClickHouseTableConfig) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: self._client.execute(f"OPTIMIZE TABLE {table_cfg.name} FINAL"),
        )
        self._metrics.clickhouse_tables_optimized += 1
        log_structured("info", "clickhouse_table_optimized", table=table_cfg.name)

    async def _enforce_ttl(self, table_cfg: ClickHouseTableConfig) -> None:
        loop = asyncio.get_running_loop()

        database, table = self._split_table_name(table_cfg.name)

        def _get_current_ttl() -> Optional[str]:
            rows = self._client.execute(
                """
                SELECT ttl_expression
                FROM system.tables
                WHERE database = %(database)s AND name = %(table)s
                """,
                {"database": database, "table": table},
            )
            return rows[0][0] if rows else None

        current_ttl = await loop.run_in_executor(None, _get_current_ttl)
        expected = table_cfg.ttl_expression

        if current_ttl == expected:
            return

        def _apply_ttl() -> None:
            fq_table = table_cfg.name if "." in table_cfg.name else f"{database}.{table}"
            self._client.execute(f"ALTER TABLE {fq_table} MODIFY TTL {expected}")

        await loop.run_in_executor(None, _apply_ttl)
        self._metrics.clickhouse_ttl_updates += 1
        log_structured(
            "info",
            "clickhouse_ttl_updated",
            table=table_cfg.name,
            ttl_expression=expected,
        )

    def _split_table_name(self, name: str) -> tuple[str, str]:
        if "." in name:
            database, table = name.split(".", 1)
        else:
            database = self._settings.data_backend.clickhouse_database
            table = name
        return database, table


class WarehouseMaintenanceCoordinator:
    """Coordinates all warehouse maintenance jobs."""

    def __init__(self, settings: AurumSettings, config: Optional[WarehouseMaintenanceConfig] = None) -> None:
        self.settings = settings
        self.config = config or WarehouseMaintenanceConfig.from_settings(settings)
        self.metrics = WarehouseMaintenanceMetrics()
        self._timescale_job = TimescaleMaintenanceJob(
            dsn=settings.database.timescale_dsn,
            tables=self.config.timescale_tables,
            interval_seconds=self.config.timescale_interval_seconds,
            metrics=self.metrics,
        )
        self._iceberg_job = IcebergMaintenanceJob(
            settings=settings,
            tables=self.config.iceberg_tables,
            interval_seconds=self.config.iceberg_interval_seconds,
            metrics=self.metrics,
            concurrency_config=self.config.trino_concurrency,
        )
        self._clickhouse_job = ClickHouseMaintenanceJob(
            settings=settings,
            tables=self.config.clickhouse_tables,
            interval_seconds=self.config.clickhouse_interval_seconds,
            metrics=self.metrics,
        )

    async def start(self) -> None:
        await self._timescale_job.start()
        await self._iceberg_job.start()
        await self._clickhouse_job.start()
        self.metrics.last_run = datetime.utcnow()

    async def stop(self) -> None:
        await self._timescale_job.stop()
        await self._iceberg_job.stop()
        await self._clickhouse_job.stop()

    async def run_once(self) -> None:
        """Execute one maintenance cycle for all jobs (primarily for tests)."""
        await self.run_timescale_once()
        await self.run_iceberg_once()
        await self.run_clickhouse_once()
        self.metrics.last_run = datetime.utcnow()

    async def run_timescale_once(self) -> None:
        if self._timescale_job:
            await self._timescale_job.run_once()
            self.metrics.last_run = datetime.utcnow()

    async def run_iceberg_once(self) -> None:
        if self._iceberg_job:
            await self._iceberg_job.run_once()
            self.metrics.last_run = datetime.utcnow()

    async def run_clickhouse_once(self) -> None:
        if self._clickhouse_job:
            await self._clickhouse_job.run_once()
            self.metrics.last_run = datetime.utcnow()


__all__ = [
    "WarehouseMaintenanceConfig",
    "WarehouseMaintenanceMetrics",
    "WarehouseMaintenanceCoordinator",
    "TimescaleHypertableConfig",
    "ClickHouseTableConfig",
    "IcebergTableConfig",
]
