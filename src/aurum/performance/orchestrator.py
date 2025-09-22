"""Performance optimization orchestrator for data ingestion and processing."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
from .batching import BatchConfig, BatchManager, get_batch_manager
from .caching import CacheConfig, initialize_caches
from .connection_pool import initialize_connection_pools
from .partitioning import initialize_partition_managers

LOGGER = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass
class PerformanceConfig:
    """Global performance optimization configuration."""
    batch_config: BatchConfig = field(default_factory=BatchConfig)
    cache_config: CacheConfig = field(default_factory=CacheConfig)
    enable_batching: bool = True
    enable_caching: bool = True
    enable_partitioning: bool = True
    enable_connection_pooling: bool = True
    optimization_interval: int = 300  # 5 minutes
    metrics_collection_interval: int = 60  # 1 minute


@dataclass
class PerformanceMetrics:
    """Global performance metrics."""
    total_requests: int = 0
    average_response_time: float = 0.0
    cache_hit_rate: float = 0.0
    batch_processing_rate: float = 0.0
    partition_optimization_rate: float = 0.0
    connection_pool_utilization: float = 0.0
    errors: int = 0
    warnings: int = 0


class PerformanceOrchestrator:
    """Orchestrates all performance optimization strategies."""

    def __init__(self, config: PerformanceConfig):
        self.config = config
        self.metrics = PerformanceMetrics()
        self._optimization_task: Optional[asyncio.Task] = None
        self._metrics_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # Initialize components
        self._initialize_components()

    def _initialize_components(self) -> None:
        """Initialize all performance optimization components."""
        if self.config.enable_batching:
            self.batch_manager = get_batch_manager()

        if self.config.enable_caching:
            initialize_caches(
                redis_config=self.config.cache_config,
                memory_config=self.config.cache_config
            )

        if self.config.enable_connection_pooling:
            initialize_connection_pools()

        if self.config.enable_partitioning:
            initialize_partition_managers()

        LOGGER.info("Performance optimization components initialized")

    async def start(self) -> None:
        """Start the performance orchestrator."""
        LOGGER.info("Starting performance orchestrator")

        # Start background tasks
        if self.config.optimization_interval > 0:
            self._optimization_task = asyncio.create_task(self._optimization_loop())

        if self.config.metrics_collection_interval > 0:
            self._metrics_task = asyncio.create_task(self._metrics_loop())

        LOGGER.info("Performance orchestrator started")

    async def stop(self) -> None:
        """Stop the performance orchestrator."""
        LOGGER.info("Stopping performance orchestrator")

        self._shutdown_event.set()

        if self._optimization_task:
            self._optimization_task.cancel()

        if self._metrics_task:
            self._metrics_task.cancel()

        # Shutdown batch manager
        if hasattr(self, 'batch_manager'):
            await self.batch_manager.shutdown()

        LOGGER.info("Performance orchestrator stopped")

    async def _optimization_loop(self) -> None:
        """Periodic optimization tasks."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.optimization_interval)

                with trace.get_tracer(__name__).start_as_current_span("performance_optimization") as span:
                    await self._run_optimizations()
                    span.set_attribute("optimization.completed", True)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                LOGGER.error(f"Optimization loop failed: {exc}")

    async def _run_optimizations(self) -> None:
        """Run performance optimizations."""
        try:
            # Run partition optimizations
            if self.config.enable_partitioning:
                await self._optimize_partitions()

            # Clean up caches
            if self.config.enable_caching:
                await self._cleanup_caches()

            # Optimize connection pools
            if self.config.enable_connection_pooling:
                await self._optimize_connection_pools()

            LOGGER.debug("Performance optimizations completed")

        except Exception as exc:
            LOGGER.error(f"Performance optimizations failed: {exc}")

    async def _optimize_partitions(self) -> None:
        """Optimize database partitions."""
        from .partitioning import get_timescale_manager, get_clickhouse_manager

        tables_to_optimize = [
            "aurum.market.curve_observation",
            "aurum.market.scenario_output",
            "aurum.market.ppa_valuation"
        ]

        for table in tables_to_optimize:
            try:
                if "timescale" in table.lower():
                    manager = get_timescale_manager()
                elif "clickhouse" in table.lower():
                    manager = get_clickhouse_manager()
                else:
                    continue

                if manager:
                    await manager.optimize_table(table)

            except Exception as exc:
                LOGGER.warning(f"Failed to optimize partitions for {table}: {exc}")

    async def _cleanup_caches(self) -> None:
        """Clean up expired cache entries."""
        try:
            from .caching import get_memory_cache
            cache = get_memory_cache()

            if cache and hasattr(cache, '_cleanup_expired'):
                await cache._cleanup_expired()
                LOGGER.debug("Cache cleanup completed")

        except Exception as exc:
            LOGGER.warning(f"Cache cleanup failed: {exc}")

    async def _optimize_connection_pools(self) -> None:
        """Optimize connection pool usage."""
        try:
            from .connection_pool import get_database_pool, get_trino_pool

            for pool in [get_database_pool(), get_trino_pool()]:
                if pool:
                    pool_metrics = await pool.get_metrics()
                    LOGGER.debug(f"Connection pool metrics: {pool_metrics}")

        except Exception as exc:
            LOGGER.warning(f"Connection pool optimization failed: {exc}")

    async def _metrics_loop(self) -> None:
        """Periodic metrics collection."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.metrics_collection_interval)

                with trace.get_tracer(__name__).start_as_current_span("metrics_collection") as span:
                    await self._collect_metrics()
                    span.set_attribute("metrics.collected", True)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                LOGGER.error(f"Metrics collection failed: {exc}")

    async def _collect_metrics(self) -> None:
        """Collect performance metrics."""
        try:
            # Collect batch metrics
            if hasattr(self, 'batch_manager'):
                batch_metrics = await self.batch_manager.get_all_metrics()
                total_batches = sum(m.batches_processed for m in batch_metrics.values())
                total_items = sum(m.items_processed for m in batch_metrics.values())

                if total_batches > 0:
                    self.metrics.batch_processing_rate = total_batches / (self.config.metrics_collection_interval / 60)

            # Collect cache metrics
            from .caching import get_memory_cache
            cache = get_memory_cache()
            if cache:
                cache_metrics = cache.metrics
                total_requests = cache_metrics.hits + cache_metrics.misses
                if total_requests > 0:
                    self.metrics.cache_hit_rate = cache_metrics.hits / total_requests

            LOGGER.debug(f"Performance metrics: {self.metrics}")

        except Exception as exc:
            LOGGER.warning(f"Metrics collection failed: {exc}")

    def get_performance_report(self) -> Dict[str, Any]:
        """Get comprehensive performance report."""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": {
                "total_requests": self.metrics.total_requests,
                "average_response_time": self.metrics.average_response_time,
                "cache_hit_rate": self.metrics.cache_hit_rate,
                "batch_processing_rate": self.metrics.batch_processing_rate,
                "partition_optimization_rate": self.metrics.partition_optimization_rate,
                "connection_pool_utilization": self.metrics.connection_pool_utilization,
                "errors": self.metrics.errors,
                "warnings": self.metrics.warnings
            },
            "config": {
                "enable_batching": self.config.enable_batching,
                "enable_caching": self.config.enable_caching,
                "enable_partitioning": self.config.enable_partitioning,
                "enable_connection_pooling": self.config.enable_connection_pooling,
                "optimization_interval": self.config.optimization_interval,
                "metrics_collection_interval": self.config.metrics_collection_interval
            },
            "request_id": get_request_id()
        }


# Global orchestrator instance
_orchestrator: Optional[PerformanceOrchestrator] = None


def get_performance_orchestrator() -> Optional[PerformanceOrchestrator]:
    """Get the global performance orchestrator instance."""
    return _orchestrator


def initialize_performance_orchestrator(config: Optional[PerformanceConfig] = None) -> PerformanceOrchestrator:
    """Initialize the global performance orchestrator."""
    global _orchestrator

    if config is None:
        config = PerformanceConfig()

    _orchestrator = PerformanceOrchestrator(config)

    # Start the orchestrator automatically
    asyncio.create_task(_orchestrator.start())

    LOGGER.info("Performance orchestrator initialized and started")
    return _orchestrator


def get_performance_report() -> Dict[str, Any]:
    """Get the current performance report."""
    if _orchestrator:
        return _orchestrator.get_performance_report()
    else:
        return {"error": "Performance orchestrator not initialized"}
