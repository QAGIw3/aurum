"""Database partitioning strategies for performance optimization."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

try:  # optional dependency
    from opentelemetry import trace  # type: ignore
except Exception:  # pragma: no cover - fallback noop tracer
    import types
    from contextlib import nullcontext

    class _NoopTracer:
        def start_as_current_span(self, name: str):
            return nullcontext()

    trace = types.SimpleNamespace(get_tracer=lambda name: _NoopTracer())  # type: ignore

from ..telemetry.context import get_request_id

LOGGER = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass
class PartitionConfig:
    """Configuration for database partitioning."""
    partition_type: str = "time"  # time, hash, range
    partition_column: str = "asof_date"
    partition_interval: str = "month"  # day, week, month, quarter, year
    retention_period: int = 365  # days
    precreate_partitions: int = 3  # months ahead
    max_partitions: int = 100


@dataclass
class PartitionMetrics:
    """Metrics for partitioning performance."""
    partitions_created: int = 0
    partitions_dropped: int = 0
    partitions_optimized: int = 0
    queries_optimized: int = 0
    total_optimization_time: float = 0.0


class PartitionManager(ABC):
    """Abstract base class for partition management."""

    def __init__(self, config: PartitionConfig):
        self.config = config
        self.metrics = PartitionMetrics()

    async def optimize_table(self, table_name: str) -> None:
        """Optimize a table using partitioning strategies."""
        with trace.get_tracer(__name__).start_as_current_span("partition_manager.optimize_table") as span:
            span.set_attribute("table.name", table_name)

            try:
                # Check if partitioning is needed
                needs_partitioning = await self._needs_partitioning(table_name)

                if needs_partitioning:
                    await self._create_partitions(table_name)
                    await self._optimize_existing_partitions(table_name)
                    await self._cleanup_old_partitions(table_name)

                span.set_attribute("partition.result", "optimized")

            except Exception as exc:
                LOGGER.error(f"Partition optimization failed for {table_name}: {exc}")
                span.set_attribute("partition.result", "failed")

    async def get_partition_info(self, table_name: str) -> Dict[str, Any]:
        """Get partitioning information for a table."""
        return await self._get_partition_info_impl(table_name)

    @abstractmethod
    async def _needs_partitioning(self, table_name: str) -> bool:
        """Check if table needs partitioning."""
        pass

    @abstractmethod
    async def _create_partitions(self, table_name: str) -> None:
        """Create new partitions."""
        pass

    @abstractmethod
    async def _optimize_existing_partitions(self, table_name: str) -> None:
        """Optimize existing partitions."""
        pass

    @abstractmethod
    async def _cleanup_old_partitions(self, table_name: str) -> None:
        """Remove old partitions."""
        pass

    @abstractmethod
    async def _get_partition_info_impl(self, table_name: str) -> Dict[str, Any]:
        """Get partitioning information."""
        pass


class TimescaleDBPartitionManager(PartitionManager):
    """Partition manager for TimescaleDB."""

    def __init__(self, config: PartitionConfig, connection_pool):
        super().__init__(config)
        self.connection_pool = connection_pool

    async def _needs_partitioning(self, table_name: str) -> bool:
        """Check if TimescaleDB hypertable needs optimization."""
        try:
            async with self.connection_pool.acquire() as conn:
                # Check if table is a hypertable
                result = await conn.fetchrow(
                    "SELECT COUNT(*) FROM timescaledb_information.hypertables WHERE hypertable_name = $1",
                    table_name
                )

                if result[0] == 0:
                    return False  # Not a hypertable

                # Check for recent partitions
                result = await conn.fetchrow(
                    """
                    SELECT COUNT(*) FROM timescaledb_information.dimensions
                    WHERE hypertable_name = $1
                    """,
                    table_name
                )

                return result[0] < self.config.precreate_partitions

        except Exception as exc:
            LOGGER.warning(f"Failed to check partitioning needs for {table_name}: {exc}")
            return False

    async def _create_partitions(self, table_name: str) -> None:
        """Create new partitions for TimescaleDB."""
        try:
            # Calculate future partition dates
            future_dates = []
            current_date = datetime.now().date()

            for i in range(self.config.precreate_partitions):
                if self.config.partition_interval == "month":
                    future_date = current_date.replace(day=1) + timedelta(days=32*i)
                    future_date = future_date.replace(day=1)
                elif self.config.partition_interval == "quarter":
                    current_month = (current_date.month - 1) // 3 + 1
                    future_quarter = current_month + i
                    future_year = current_date.year + (future_quarter - 1) // 4
                    future_quarter = ((future_quarter - 1) % 4) + 1
                    future_date = date(future_year, future_quarter * 3 - 2, 1)
                else:
                    future_date = current_date + timedelta(days=30*i)

                future_dates.append(future_date)

            async with self.connection_pool.acquire() as conn:
                for partition_date in future_dates:
                    await conn.execute(
                        "SELECT add_dimension($1, $2, $3, partitioning_func => $4)",
                        table_name,
                        self.config.partition_column,
                        partition_date,
                        "daily"
                    )

            self.metrics.partitions_created += len(future_dates)
            LOGGER.info(f"Created {len(future_dates)} new partitions for {table_name}")

        except Exception as exc:
            LOGGER.error(f"Failed to create partitions for {table_name}: {exc}")

    async def _optimize_existing_partitions(self, table_name: str) -> None:
        """Optimize existing TimescaleDB partitions."""
        try:
            async with self.connection_pool.acquire() as conn:
                # Enable compression on old partitions
                cutoff_date = datetime.now() - timedelta(days=30)

                await conn.execute(
                    """
                    ALTER TABLE $1 SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = $2,
                        timescaledb.compress_orderby = $3
                    )
                    """,
                    table_name,
                    self.config.partition_column,
                    f"{self.config.partition_column} DESC"
                )

                # Compress chunks older than cutoff
                await conn.execute(
                    "SELECT compress_chunk(c) FROM show_chunks($1) c WHERE c::date < $2",
                    table_name,
                    cutoff_date
                )

            self.metrics.partitions_optimized += 1
            LOGGER.info(f"Optimized partitions for {table_name}")

        except Exception as exc:
            LOGGER.error(f"Failed to optimize partitions for {table_name}: {exc}")

    async def _cleanup_old_partitions(self, table_name: str) -> None:
        """Remove old TimescaleDB partitions."""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.config.retention_period)

            async with self.connection_pool.acquire() as conn:
                # Drop chunks older than retention period
                result = await conn.execute(
                    "SELECT drop_chunks($1, $2)",
                    table_name,
                    cutoff_date
                )

            self.metrics.partitions_dropped += 1
            LOGGER.info(f"Cleaned up old partitions for {table_name}")

        except Exception as exc:
            LOGGER.error(f"Failed to cleanup partitions for {table_name}: {exc}")

    async def _get_partition_info_impl(self, table_name: str) -> Dict[str, Any]:
        """Get TimescaleDB partition information."""
        try:
            async with self.connection_pool.acquire() as conn:
                # Get hypertable information
                hypertable_info = await conn.fetchrow(
                    "SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = $1",
                    table_name
                )

                if not hypertable_info:
                    return {"is_hypertable": False}

                # Get dimension information
                dimension_info = await conn.fetch(
                    "SELECT * FROM timescaledb_information.dimensions WHERE hypertable_name = $1",
                    table_name
                )

                # Get chunk information
                chunk_info = await conn.fetch(
                    "SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = $1",
                    table_name
                )

                return {
                    "is_hypertable": True,
                    "hypertable": dict(hypertable_info),
                    "dimensions": [dict(row) for row in dimension_info],
                    "chunks": [dict(row) for row in chunk_info],
                    "chunk_count": len(chunk_info)
                }

        except Exception as exc:
            LOGGER.error(f"Failed to get partition info for {table_name}: {exc}")
            return {"error": str(exc)}


class ClickHousePartitionManager(PartitionManager):
    """Partition manager for ClickHouse."""

    def __init__(self, config: PartitionConfig, connection_pool):
        super().__init__(config)
        self.connection_pool = connection_pool

    async def _needs_partitioning(self, table_name: str) -> bool:
        """Check if ClickHouse table needs partitioning optimization."""
        try:
            async with self.connection_pool.acquire() as conn:
                # Check if table exists and has partitions
                result = await conn.fetchrow(
                    "SELECT COUNT() FROM system.parts WHERE table = $1 AND active = 1",
                    table_name
                )

                return result[0] > self.config.max_partitions

        except Exception as exc:
            LOGGER.warning(f"Failed to check partitioning needs for {table_name}: {exc}")
            return False

    async def _create_partitions(self, table_name: str) -> None:
        """Create new partitions for ClickHouse."""
        try:
            # Calculate future partition ranges
            future_ranges = []
            base_date = datetime.now().date()

            for i in range(self.config.precreate_partitions):
                if self.config.partition_interval == "month":
                    start_date = base_date.replace(day=1) + timedelta(days=32*i)
                    start_date = start_date.replace(day=1)
                    end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                else:
                    start_date = base_date + timedelta(days=30*i)
                    end_date = start_date + timedelta(days=30)

                future_ranges.append((start_date, end_date))

            async with self.connection_pool.acquire() as conn:
                for start_date, end_date in future_ranges:
                    # Check if partition already exists
                    result = await conn.fetchrow(
                        """
                        SELECT COUNT() FROM system.parts
                        WHERE table = $1 AND partition = $2
                        """,
                        table_name,
                        start_date.strftime("%Y-%m-%d")
                    )

                    if result[0] == 0:
                        # Create new partition
                        await conn.execute(
                            f"ALTER TABLE {table_name} ADD PARTITION {start_date.strftime('%Y%m%d')}"
                        )

            self.metrics.partitions_created += len(future_ranges)
            LOGGER.info(f"Created {len(future_ranges)} new partitions for {table_name}")

        except Exception as exc:
            LOGGER.error(f"Failed to create partitions for {table_name}: {exc}")

    async def _optimize_existing_partitions(self, table_name: str) -> None:
        """Optimize existing ClickHouse partitions."""
        try:
            async with self.connection_pool.acquire() as conn:
                # Optimize table
                await conn.execute(f"OPTIMIZE TABLE {table_name} FINAL")

                # Update modification time for statistics
                await conn.execute(
                    "ALTER TABLE $1 MODIFY TTL $2 + INTERVAL $3 DAY",
                    table_name,
                    self.config.partition_column,
                    self.config.retention_period
                )

            self.metrics.partitions_optimized += 1
            LOGGER.info(f"Optimized partitions for {table_name}")

        except Exception as exc:
            LOGGER.error(f"Failed to optimize partitions for {table_name}: {exc}")

    async def _cleanup_old_partitions(self, table_name: str) -> None:
        """Remove old ClickHouse partitions."""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.config.retention_period)

            async with self.connection_pool.acquire() as conn:
                # Drop old partitions
                await conn.execute(
                    """
                    ALTER TABLE $1 DROP PARTITION WHERE $2 < $3
                    """,
                    table_name,
                    self.config.partition_column,
                    cutoff_date.date()
                )

            self.metrics.partitions_dropped += 1
            LOGGER.info(f"Cleaned up old partitions for {table_name}")

        except Exception as exc:
            LOGGER.error(f"Failed to cleanup partitions for {table_name}: {exc}")

    async def _get_partition_info_impl(self, table_name: str) -> Dict[str, Any]:
        """Get ClickHouse partition information."""
        try:
            async with self.connection_pool.acquire() as conn:
                # Get partition information
                partitions = await conn.fetch(
                    """
                    SELECT partition, count() as part_count, sum(rows) as row_count,
                           sum(data_compressed_bytes) as compressed_size,
                           sum(data_uncompressed_bytes) as uncompressed_size
                    FROM system.parts
                    WHERE table = $1 AND active = 1
                    GROUP BY partition
                    ORDER BY partition
                    """,
                    table_name
                )

                return {
                    "table": table_name,
                    "partitions": [dict(row) for row in partitions],
                    "partition_count": len(partitions),
                    "total_rows": sum(row["row_count"] for row in partitions),
                    "total_compressed_size": sum(row["compressed_size"] for row in partitions),
                    "total_uncompressed_size": sum(row["uncompressed_size"] for row in partitions)
                }

        except Exception as exc:
            LOGGER.error(f"Failed to get partition info for {table_name}: {exc}")
            return {"error": str(exc)}


class TrinoPartitionManager(PartitionManager):
    """Partition manager for Trino/Iceberg."""

    def __init__(self, config: PartitionConfig, catalog_name: str = "iceberg"):
        super().__init__(config)
        self.catalog_name = catalog_name

    async def _needs_partitioning(self, table_name: str) -> bool:
        """Check if Trino table needs partitioning optimization."""
        # For Trino/Iceberg, partitioning is handled at table creation
        # This is a simplified check
        return True

    async def _create_partitions(self, table_name: str) -> None:
        """Create partitions for Trino/Iceberg (handled at table creation)."""
        # Iceberg partitioning is typically static
        # This would involve schema evolution if needed
        pass

    async def _optimize_existing_partitions(self, table_name: str) -> None:
        """Optimize existing Trino/Iceberg partitions."""
        try:
            from ..parsers.iceberg_writer import write_to_iceberg

            # Trigger compaction/optimization
            # This is a placeholder for actual optimization logic
            LOGGER.info(f"Optimizing partitions for {table_name}")

        except Exception as exc:
            LOGGER.error(f"Failed to optimize partitions for {table_name}: {exc}")

    async def _cleanup_old_partitions(self, table_name: str) -> None:
        """Remove old Trino/Iceberg partitions."""
        # This would involve table maintenance operations
        # For now, it's handled by retention policies in table properties
        pass

    async def _get_partition_info_impl(self, table_name: str) -> Dict[str, Any]:
        """Get Trino/Iceberg partition information."""
        try:
            from trino.dbapi import connect

            conn = connect(
                host="localhost",  # This should come from config
                port=8080,
                user="aurum",
                catalog=self.catalog_name,
                schema="market"
            )

            cursor = conn.cursor()
            cursor.execute(
                f"SHOW PARTITIONS FROM {table_name}"
            )
            partitions = cursor.fetchall()

            return {
                "table": table_name,
                "catalog": self.catalog_name,
                "partitions": partitions,
                "partition_count": len(partitions)
            }

        except Exception as exc:
            LOGGER.error(f"Failed to get partition info for {table_name}: {exc}")
            return {"error": str(exc)}


# Global partition managers
_timescale_manager: Optional[TimescaleDBPartitionManager] = None
_clickhouse_manager: Optional[ClickHousePartitionManager] = None
_trino_manager: Optional[TrinoPartitionManager] = None


def get_timescale_manager() -> Optional[TimescaleDBPartitionManager]:
    """Get TimescaleDB partition manager."""
    return _timescale_manager


def get_clickhouse_manager() -> Optional[ClickHousePartitionManager]:
    """Get ClickHouse partition manager."""
    return _clickhouse_manager


def get_trino_manager() -> Optional[TrinoPartitionManager]:
    """Get Trino partition manager."""
    return _trino_manager


def initialize_partition_managers() -> None:
    """Initialize global partition managers."""
    global _timescale_manager, _clickhouse_manager, _trino_manager

    config = PartitionConfig()

    # Initialize with mock connection pools for now
    from .connection_pool import get_database_pool, get_trino_pool

    db_pool = get_database_pool()
    if db_pool:
        _timescale_manager = TimescaleDBPartitionManager(config, db_pool)

    trino_pool = get_trino_pool()
    if trino_pool:
        _trino_manager = TrinoPartitionManager(config)

    LOGGER.info("Partition managers initialized")
