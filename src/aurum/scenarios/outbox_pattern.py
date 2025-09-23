"""Outbox pattern implementation for reliable scenario state management and event publishing."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
from enum import Enum

from pydantic import BaseModel

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger
from ..kafka.optimized_producer import OptimizedKafkaProducer
from ..database.trino_client import get_trino_client


class OutboxEventType(str, Enum):
    """Types of outbox events."""
    SCENARIO_CREATED = "scenario_created"
    SCENARIO_UPDATED = "scenario_updated"
    SCENARIO_DELETED = "scenario_deleted"
    SCENARIO_RUN_STARTED = "scenario_run_started"
    SCENARIO_RUN_COMPLETED = "scenario_run_completed"
    SCENARIO_RUN_FAILED = "scenario_run_failed"
    SCENARIO_RUN_CANCELLED = "scenario_run_cancelled"


@dataclass
class OutboxEvent:
    """Outbox event for reliable publishing."""
    event_id: str
    event_type: OutboxEventType
    aggregate_id: str  # Scenario ID or Run ID
    aggregate_type: str  # "scenario" or "scenario_run"
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    published_at: Optional[float] = None
    retry_count: int = 0
    max_retries: int = 3
    last_error: Optional[str] = None
    trace_id: Optional[str] = None
    request_id: Optional[str] = None


class OutboxStore(ABC):
    """Abstract interface for outbox storage."""

    @abstractmethod
    async def save_event(self, event: OutboxEvent) -> None:
        """Save event to outbox."""
        pass

    @abstractmethod
    async def get_pending_events(self, limit: int = 100) -> List[OutboxEvent]:
        """Get pending events for publishing."""
        pass

    @abstractmethod
    async def mark_published(self, event_id: str) -> None:
        """Mark event as published."""
        pass

    @abstractmethod
    async def mark_failed(self, event_id: str, error: str) -> None:
        """Mark event as failed."""
        pass

    @abstractmethod
    async def cleanup_old_events(self, older_than_days: int = 30) -> int:
        """Clean up old events."""
        pass


class DatabaseOutboxStore(OutboxStore):
    """Database-backed outbox store using PostgreSQL."""

    def __init__(self, database_client=None):
        self.database_client = database_client or get_trino_client()
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

    async def save_event(self, event: OutboxEvent) -> None:
        """Save event to outbox table."""
        try:
            # This would insert into a PostgreSQL outbox table
            # For now, implement as a placeholder
            query = """
            INSERT INTO outbox_events (
                event_id, event_type, aggregate_id, aggregate_type,
                payload, metadata, created_at, retry_count, max_retries,
                trace_id, request_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """

            await self.database_client.execute(query, [
                event.event_id,
                event.event_type.value,
                event.aggregate_id,
                event.aggregate_type,
                json.dumps(event.payload),
                json.dumps(event.metadata),
                event.created_at,
                event.retry_count,
                event.max_retries,
                event.trace_id,
                event.request_id
            ])

            self.metrics.increment_counter("outbox_events_saved")

        except Exception as e:
            self.metrics.increment_counter("outbox_save_errors")
            self.logger.error(f"Error saving outbox event: {e}")
            raise

    async def get_pending_events(self, limit: int = 100) -> List[OutboxEvent]:
        """Get pending events for publishing."""
        try:
            query = """
            SELECT event_id, event_type, aggregate_id, aggregate_type,
                   payload, metadata, created_at, published_at, retry_count,
                   max_retries, last_error, trace_id, request_id
            FROM outbox_events
            WHERE published_at IS NULL
            AND (retry_count < max_retries OR max_retries = 0)
            ORDER BY created_at ASC
            LIMIT %s
            """

            rows = await self.database_client.fetch(query, [limit])

            events = []
            for row in rows:
                events.append(OutboxEvent(
                    event_id=row['event_id'],
                    event_type=OutboxEventType(row['event_type']),
                    aggregate_id=row['aggregate_id'],
                    aggregate_type=row['aggregate_type'],
                    payload=json.loads(row['payload']),
                    metadata=json.loads(row['metadata']) if row['metadata'] else {},
                    created_at=row['created_at'],
                    published_at=row['published_at'],
                    retry_count=row['retry_count'],
                    max_retries=row['max_retries'],
                    last_error=row['last_error'],
                    trace_id=row['trace_id'],
                    request_id=row['request_id']
                ))

            self.metrics.gauge("outbox_pending_events", len(events))
            return events

        except Exception as e:
            self.metrics.increment_counter("outbox_fetch_errors")
            self.logger.error(f"Error fetching outbox events: {e}")
            return []

    async def mark_published(self, event_id: str) -> None:
        """Mark event as published."""
        try:
            query = """
            UPDATE outbox_events
            SET published_at = %s, last_error = NULL
            WHERE event_id = %s
            """

            await self.database_client.execute(query, [time.time(), event_id])
            self.metrics.increment_counter("outbox_events_published")

        except Exception as e:
            self.metrics.increment_counter("outbox_mark_published_errors")
            self.logger.error(f"Error marking event as published: {e}")

    async def mark_failed(self, event_id: str, error: str) -> None:
        """Mark event as failed."""
        try:
            query = """
            UPDATE outbox_events
            SET retry_count = retry_count + 1, last_error = %s
            WHERE event_id = %s
            """

            await self.database_client.execute(query, [error, event_id])
            self.metrics.increment_counter("outbox_events_failed")

        except Exception as e:
            self.metrics.increment_counter("outbox_mark_failed_errors")
            self.logger.error(f"Error marking event as failed: {e}")

    async def cleanup_old_events(self, older_than_days: int = 30) -> int:
        """Clean up old events."""
        try:
            cutoff_time = time.time() - (older_than_days * 24 * 60 * 60)

            query = """
            DELETE FROM outbox_events
            WHERE published_at IS NOT NULL
            AND published_at < %s
            """

            result = await self.database_client.execute(query, [cutoff_time])
            cleaned_count = result.rowcount if hasattr(result, 'rowcount') else 0

            self.metrics.increment_counter("outbox_events_cleaned", cleaned_count)
            self.logger.info(f"Cleaned up {cleaned_count} old outbox events")

            return cleaned_count

        except Exception as e:
            self.metrics.increment_counter("outbox_cleanup_errors")
            self.logger.error(f"Error cleaning up old events: {e}")
            return 0


class OutboxPublisher:
    """Publish outbox events to Kafka."""

    def __init__(self, kafka_producer: OptimizedKafkaProducer, topic: str = "aurum.scenarios"):
        self.kafka_producer = kafka_producer
        self.topic = topic
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

    async def publish_event(self, event: OutboxEvent) -> bool:
        """Publish an event to Kafka."""
        try:
            # Create Kafka message with metadata
            message = {
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "aggregate_id": event.aggregate_id,
                "aggregate_type": event.aggregate_type,
                "payload": event.payload,
                "metadata": event.metadata,
                "created_at": event.created_at,
                "trace_id": event.trace_id,
                "request_id": event.request_id,
                "source": "outbox"
            }

            # Add headers for trace propagation
            headers = {}
            if event.trace_id:
                headers['trace_id'] = event.trace_id
            if event.request_id:
                headers['request_id'] = event.request_id

            # Send to Kafka
            await self.kafka_producer.produce(
                topic=self.topic,
                value=message,
                key=event.aggregate_id,
                headers=headers
            )

            self.metrics.increment_counter("outbox_kafka_published")
            return True

        except Exception as e:
            self.metrics.increment_counter("outbox_kafka_errors")
            self.logger.error(f"Error publishing event to Kafka: {e}")
            return False


class OutboxManager:
    """Manage the outbox pattern for scenarios."""

    def __init__(
        self,
        outbox_store: OutboxStore,
        kafka_producer: OptimizedKafkaProducer,
        topic: str = "aurum.scenarios"
    ):
        self.outbox_store = outbox_store
        self.kafka_producer = kafka_producer
        self.topic = topic
        self.publisher = OutboxPublisher(kafka_producer, topic)
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

        # Background tasks
        self._dispatcher_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

    async def initialize(self):
        """Initialize the outbox manager."""
        self._running = True
        self._dispatcher_task = asyncio.create_task(self._event_dispatcher())
        self._cleanup_task = asyncio.create_task(self._cleanup_worker())

    async def close(self):
        """Close the outbox manager."""
        self._running = False

        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            try:
                await self._dispatcher_task
            except asyncio.CancelledError:
                pass

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Process any remaining events
        await self._process_pending_events()

    async def add_event(
        self,
        event_type: OutboxEventType,
        aggregate_id: str,
        aggregate_type: str,
        payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> str:
        """Add an event to the outbox."""
        event = OutboxEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            payload=payload,
            metadata=metadata or {},
            trace_id=trace_id,
            request_id=request_id
        )

        await self.outbox_store.save_event(event)
        self.metrics.increment_counter("outbox_events_added")

        return event.event_id

    async def _event_dispatcher(self):
        """Background task to dispatch pending events."""
        while self._running:
            try:
                await asyncio.sleep(1.0)  # Check every second

                if not self._running:
                    break

                await self._process_pending_events()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in event dispatcher: {e}")
                await asyncio.sleep(5.0)  # Backoff on error

    async def _process_pending_events(self):
        """Process pending events from outbox."""
        try:
            events = await self.outbox_store.get_pending_events(limit=10)

            for event in events:
                success = await self.publisher.publish_event(event)

                if success:
                    await self.outbox_store.mark_published(event.event_id)
                else:
                    # Increment retry count
                    event.retry_count += 1
                    if event.retry_count >= event.max_retries:
                        await self.outbox_store.mark_failed(
                            event.event_id,
                            f"Max retries exceeded ({event.retry_count})"
                        )
                    else:
                        await self.outbox_store.mark_failed(
                            event.event_id,
                            "Publishing failed"
                        )

        except Exception as e:
            self.logger.error(f"Error processing pending events: {e}")

    async def _cleanup_worker(self):
        """Background task to clean up old events."""
        while self._running:
            try:
                await asyncio.sleep(3600)  # Clean up every hour

                if not self._running:
                    break

                cleaned = await self.outbox_store.cleanup_old_events()
                if cleaned > 0:
                    self.logger.info(f"Cleaned up {cleaned} old outbox events")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in cleanup worker: {e}")

    async def get_stats(self) -> Dict[str, Any]:
        """Get outbox statistics."""
        return {
            "running": self._running,
            "topic": self.topic,
            "publisher_active": True,  # Would check actual status
        }


class ScenarioOutboxManager:
    """Specialized outbox manager for scenario operations."""

    def __init__(self, outbox_manager: OutboxManager):
        self.outbox_manager = outbox_manager
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

    async def scenario_created(
        self,
        scenario_id: str,
        scenario_data: Dict[str, Any],
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> str:
        """Record scenario creation event."""
        event_id = await self.outbox_manager.add_event(
            event_type=OutboxEventType.SCENARIO_CREATED,
            aggregate_id=scenario_id,
            aggregate_type="scenario",
            payload={
                "scenario_id": scenario_id,
                "scenario_data": scenario_data,
                "created_at": time.time()
            },
            metadata={"action": "create"},
            trace_id=trace_id,
            request_id=request_id
        )

        self.logger.info(f"Recorded scenario creation event: {scenario_id}")
        return event_id

    async def scenario_updated(
        self,
        scenario_id: str,
        updates: Dict[str, Any],
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> str:
        """Record scenario update event."""
        event_id = await self.outbox_manager.add_event(
            event_type=OutboxEventType.SCENARIO_UPDATED,
            aggregate_id=scenario_id,
            aggregate_type="scenario",
            payload={
                "scenario_id": scenario_id,
                "updates": updates,
                "updated_at": time.time()
            },
            metadata={"action": "update"},
            trace_id=trace_id,
            request_id=request_id
        )

        self.logger.info(f"Recorded scenario update event: {scenario_id}")
        return event_id

    async def scenario_run_started(
        self,
        scenario_id: str,
        run_id: str,
        run_config: Dict[str, Any],
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> str:
        """Record scenario run start event."""
        event_id = await self.outbox_manager.add_event(
            event_type=OutboxEventType.SCENARIO_RUN_STARTED,
            aggregate_id=run_id,
            aggregate_type="scenario_run",
            payload={
                "scenario_id": scenario_id,
                "run_id": run_id,
                "run_config": run_config,
                "started_at": time.time()
            },
            metadata={"scenario_id": scenario_id},
            trace_id=trace_id,
            request_id=request_id
        )

        self.logger.info(f"Recorded scenario run start event: {run_id}")
        return event_id

    async def scenario_run_completed(
        self,
        scenario_id: str,
        run_id: str,
        results: Dict[str, Any],
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> str:
        """Record scenario run completion event."""
        event_id = await self.outbox_manager.add_event(
            event_type=OutboxEventType.SCENARIO_RUN_COMPLETED,
            aggregate_id=run_id,
            aggregate_type="scenario_run",
            payload={
                "scenario_id": scenario_id,
                "run_id": run_id,
                "results": results,
                "completed_at": time.time()
            },
            metadata={"scenario_id": scenario_id},
            trace_id=trace_id,
            request_id=request_id
        )

        self.logger.info(f"Recorded scenario run completion event: {run_id}")
        return event_id

    async def scenario_run_failed(
        self,
        scenario_id: str,
        run_id: str,
        error: str,
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> str:
        """Record scenario run failure event."""
        event_id = await self.outbox_manager.add_event(
            event_type=OutboxEventType.SCENARIO_RUN_FAILED,
            aggregate_id=run_id,
            aggregate_type="scenario_run",
            payload={
                "scenario_id": scenario_id,
                "run_id": run_id,
                "error": error,
                "failed_at": time.time()
            },
            metadata={"scenario_id": scenario_id, "error": True},
            trace_id=trace_id,
            request_id=request_id
        )

        self.logger.info(f"Recorded scenario run failure event: {run_id}")
        return event_id


# Factory functions
async def create_outbox_manager(
    database_client=None,
    kafka_producer: Optional[OptimizedKafkaProducer] = None,
    topic: str = "aurum.scenarios"
) -> OutboxManager:
    """Create an outbox manager with default components."""
    outbox_store = DatabaseOutboxStore(database_client)

    if kafka_producer is None:
        kafka_producer = OptimizedKafkaProducer()

    manager = OutboxManager(outbox_store, kafka_producer, topic)
    await manager.initialize()

    return manager


__all__ = [
    "OutboxEventType",
    "OutboxEvent",
    "OutboxStore",
    "DatabaseOutboxStore",
    "OutboxPublisher",
    "OutboxManager",
    "ScenarioOutboxManager",
    "create_outbox_manager",
]
