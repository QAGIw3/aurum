"""Batching strategies for optimized data processing."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, TypeVar, Generic

import pandas as pd
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

T = TypeVar('T')

LOGGER = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    max_batch_size: int = 1000
    max_wait_time: float = 5.0  # seconds
    flush_interval: float = 1.0  # seconds
    retry_attempts: int = 3
    retry_delay: float = 1.0  # seconds


@dataclass
class BatchMetrics:
    """Metrics for batch processing performance."""
    batches_processed: int = 0
    items_processed: int = 0
    total_processing_time: float = 0.0
    average_batch_size: float = 0.0
    errors: int = 0
    last_flush_time: Optional[datetime] = None


class BatchProcessor(Generic[T], ABC):
    """Abstract base class for batch processing."""

    def __init__(self, config: BatchConfig):
        self.config = config
        self.batch: List[T] = []
        self.metrics = BatchMetrics()
        self._flush_timer: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    async def add_item(self, item: T) -> None:
        """Add an item to the current batch."""
        with trace.get_tracer(__name__).start_as_current_span("batch_processor.add_item") as span:
            span.set_attribute("batch.size", len(self.batch))
            span.set_attribute("batch.max_size", self.config.max_batch_size)

            self.batch.append(item)

            # Check if we should flush
            if len(self.batch) >= self.config.max_batch_size:
                await self._flush_batch()
            elif self._flush_timer is None:
                self._start_flush_timer()

    async def add_items(self, items: List[T]) -> None:
        """Add multiple items to the batch."""
        with trace.get_tracer(__name__).start_as_current_span("batch_processor.add_items") as span:
            span.set_attribute("items.count", len(items))

            for item in items:
                await self.add_item(item)

    async def flush(self) -> None:
        """Force flush of current batch."""
        await self._flush_batch()

    async def _flush_batch(self) -> None:
        """Internal method to flush the current batch."""
        with trace.get_tracer(__name__).start_as_current_span("batch_processor.flush_batch") as span:
            span.set_attribute("batch.items", len(self.batch))

            if not self.batch:
                return

            try:
                start_time = datetime.now()
                await self._process_batch(self.batch.copy())

                # Update metrics
                self.metrics.batches_processed += 1
                self.metrics.items_processed += len(self.batch)
                self.metrics.last_flush_time = datetime.now()

                processing_time = (datetime.now() - start_time).total_seconds()
                self.metrics.total_processing_time += processing_time

                if self.metrics.batches_processed > 0:
                    self.metrics.average_batch_size = (
                        self.metrics.items_processed / self.metrics.batches_processed
                    )

                LOGGER.info(
                    f"Processed batch of {len(self.batch)} items in {processing_time:.2f}s",
                    extra={"request_id": get_request_id()}
                )

            except Exception as exc:
                self.metrics.errors += 1
                LOGGER.error(
                    f"Batch processing failed: {exc}",
                    extra={"request_id": get_request_id(), "exc_info": True}
                )
                raise
            finally:
                self.batch.clear()
                self._cancel_flush_timer()

    @abstractmethod
    async def _process_batch(self, batch: List[T]) -> None:
        """Process a batch of items. Must be implemented by subclasses."""
        pass

    def _start_flush_timer(self) -> None:
        """Start the flush timer."""
        async def _flush_on_timeout():
            while not self._shutdown_event.is_set():
                await asyncio.sleep(self.config.flush_interval)
                if len(self.batch) > 0:
                    await self._flush_batch()

        self._flush_timer = asyncio.create_task(_flush_on_timeout())

    def _cancel_flush_timer(self) -> None:
        """Cancel the flush timer."""
        if self._flush_timer and not self._flush_timer.done():
            self._flush_timer.cancel()

    async def shutdown(self) -> None:
        """Shutdown the batch processor."""
        self._shutdown_event.set()
        self._cancel_flush_timer()
        await self.flush()

    def get_metrics(self) -> BatchMetrics:
        """Get current metrics."""
        return self.metrics


class DataFrameBatcher:
    """Specialized batcher for pandas DataFrames."""

    def __init__(self, config: BatchConfig):
        self.config = config
        self.batches: List[pd.DataFrame] = []
        self.current_size = 0

    def add_dataframe(self, df: pd.DataFrame) -> None:
        """Add a DataFrame to the current batch."""
        if len(df) + self.current_size > self.config.max_batch_size:
            self._flush_if_needed()

        self.batches.append(df)
        self.current_size += len(df)

    def _flush_if_needed(self) -> Optional[pd.DataFrame]:
        """Flush the current batch if it's ready."""
        if not self.batches:
            return None

        if self.current_size >= self.config.max_batch_size:
            return self._combine_batches()

        return None

    def _combine_batches(self) -> pd.DataFrame:
        """Combine all batches into a single DataFrame."""
        if not self.batches:
            return pd.DataFrame()

        result = pd.concat(self.batches, ignore_index=True)
        self.batches.clear()
        self.current_size = 0
        return result

    def get_combined_batch(self) -> pd.DataFrame:
        """Get the combined batch."""
        return self._combine_batches()


class IcebergBatcher(BatchProcessor[pd.DataFrame]):
    """Batch processor for Iceberg table writes."""

    def __init__(self, config: BatchConfig, table_name: str, **iceberg_kwargs):
        super().__init__(config)
        self.table_name = table_name
        self.iceberg_kwargs = iceberg_kwargs

    async def _process_batch(self, batch: List[pd.DataFrame]) -> None:
        """Process a batch of DataFrames by writing to Iceberg."""
        if not batch:
            return

        # Combine all DataFrames in the batch
        combined_df = pd.concat(batch, ignore_index=True)

        # Write to Iceberg using the existing writer
        from ..parsers.iceberg_writer import write_to_iceberg
        write_to_iceberg(combined_df, table=self.table_name, **self.iceberg_kwargs)


class DatabaseBatcher(BatchProcessor[Dict[str, Any]]):
    """Batch processor for database operations."""

    def __init__(self, config: BatchConfig, connection_factory, table_name: str):
        super().__init__(config)
        self.connection_factory = connection_factory
        self.table_name = table_name

    async def _process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch by inserting to database."""
        if not batch:
            return

        # Convert to DataFrame for bulk insert
        df = pd.DataFrame(batch)

        # Use connection pooling for bulk insert
        async with self.connection_factory() as conn:
            await self._bulk_insert(conn, df)

    async def _bulk_insert(self, conn, df: pd.DataFrame) -> None:
        """Perform bulk insert operation."""
        # Implementation depends on database type
        # This is a placeholder for actual bulk insert logic
        for _, row in df.iterrows():
            await conn.execute(
                f"INSERT INTO {self.table_name} VALUES ({', '.join(['?' for _ in row])})",
                tuple(row.values)
            )


class BatchManager:
    """Manager for multiple batch processors."""

    def __init__(self):
        self.processors: Dict[str, BatchProcessor] = {}
        self._shutdown_event = asyncio.Event()

    def register_processor(self, name: str, processor: BatchProcessor) -> None:
        """Register a batch processor."""
        self.processors[name] = processor
        LOGGER.info(f"Registered batch processor: {name}")

    async def add_to_batch(self, processor_name: str, item: Any) -> None:
        """Add an item to a specific batch processor."""
        if processor_name not in self.processors:
            raise ValueError(f"No processor registered for {processor_name}")

        await self.processors[processor_name].add_item(item)

    async def flush_all(self) -> None:
        """Flush all batch processors."""
        await asyncio.gather(*[proc.flush() for proc in self.processors.values()])

    async def get_all_metrics(self) -> Dict[str, BatchMetrics]:
        """Get metrics from all processors."""
        return {
            name: processor.get_metrics()
            for name, processor in self.processors.items()
        }

    async def shutdown(self) -> None:
        """Shutdown all processors."""
        await asyncio.gather(*[proc.shutdown() for proc in self.processors.values()])


# Global batch manager instance
_batch_manager = BatchManager()


def get_batch_manager() -> BatchManager:
    """Get the global batch manager instance."""
    return _batch_manager
