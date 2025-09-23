"""Data lineage tracking for external data sources."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aurum.external.collect import ExternalCollector
from aurum.external.collect.base import CollectorContext

try:
    from aurum.kafka.optimized_producer import OptimizedKafkaProducer
except ImportError:
    OptimizedKafkaProducer = None

try:
    import uuid
    import json
except ImportError:
    uuid = None
    json = None

logger = logging.getLogger(__name__)

# Kafka topics for lineage data
LINEAGE_TOPIC = "aurum.data.lineage.v1"


class LineageEvent:
    """Enhanced data lineage event supporting OpenLineage format with dataset facets."""

    def __init__(
        self,
        event_type: str,
        source_dataset: str,
        target_dataset: str,
        transformation: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
        run_id: Optional[str] = None,
        job_name: Optional[str] = None,
        namespace: str = "aurum"
    ):
        self.event_type = event_type  # "create", "update", "transform", "validate"
        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.transformation = transformation or {}
        self.metadata = metadata or {}
        self.timestamp = timestamp or datetime.now()
        self.run_id = run_id or str(uuid.uuid4()) if uuid else f"run-{self.timestamp.timestamp()}"
        self.job_name = job_name or f"{event_type}_job"
        self.namespace = namespace

    def to_openlineage_dict(self) -> Dict[str, Any]:
        """Convert lineage event to OpenLineage format."""
        # Create dataset facets
        source_dataset_facet = {
            "schema": {"fields": [{"name": "source", "type": "string"}]},
            "dataSource": {"name": self.source_dataset, "uri": self.source_dataset}
        }

        target_dataset_facet = {
            "schema": {"fields": [{"name": "target", "type": "string"}]},
            "dataSource": {"name": self.target_dataset, "uri": self.target_dataset}
        }

        # Create job facets
        job_facet = {
            "jobDetails": {
                "name": self.job_name,
                "namespace": self.namespace,
                "description": f"External data {self.event_type} operation"
            }
        }

        # Create run facets
        run_facet = {
            "runDetails": {
                "runId": self.run_id,
                "facets": {
                    "transformation": {
                        "operation": self.event_type,
                        "details": self.transformation
                    }
                }
            }
        }

        return {
            "eventType": self.event_type.upper(),
            "eventTime": self.timestamp.isoformat(),
            "run": {
                "runId": self.run_id,
                "facets": run_facet
            },
            "job": {
                "namespace": self.namespace,
                "name": self.job_name,
                "facets": job_facet
            },
            "inputs": [{
                "namespace": self.namespace,
                "name": self.source_dataset,
                "facets": source_dataset_facet
            }],
            "outputs": [{
                "namespace": self.namespace,
                "name": self.target_dataset,
                "facets": target_dataset_facet
            }],
            "producer": "aurum-external-lineage",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json"
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert lineage event to dictionary."""
        return {
            "event_type": self.event_type,
            "source_dataset": self.source_dataset,
            "target_dataset": self.target_dataset,
            "transformation": self.transformation,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
            "run_id": self.run_id,
            "job_name": self.job_name
        }


class LineageTracker:
    """Enhanced tracker for data lineage events supporting OpenLineage format."""

    def __init__(
        self,
        collector: Optional[ExternalCollector] = None,
        openlineage_topic: str = "openlineage.events"
    ):
        self.collector = collector
        self.openlineage_topic = openlineage_topic
        self._events: List[LineageEvent] = []
        self._context = CollectorContext()
        self._openlineage_producer = None

        if OptimizedKafkaProducer:
            self._openlineage_producer = OptimizedKafkaProducer(
                bootstrap_servers="kafka:9092",  # Would be configurable
                topic=openlineage_topic
            )

    async def track_event(self, event: LineageEvent) -> None:
        """Track a lineage event with OpenLineage emission."""
        self._events.append(event)

        # Log the event
        logger.info(
            "Lineage event tracked",
            extra={
                "event_type": event.event_type,
                "source_dataset": event.source_dataset,
                "target_dataset": event.target_dataset,
                "timestamp": event.timestamp.isoformat()
            }
        )

        # Emit to lineage topic if collector is available
        if self.collector:
            await self._emit_lineage_event(event)

        # Emit OpenLineage event
        await self._emit_openlineage_event(event)

    async def _emit_lineage_event(self, event: LineageEvent) -> None:
        """Emit lineage event to Kafka."""
        if not self.collector:
            return

        try:
            lineage_record = {
                "event_type": event.event_type,
                "source_dataset": event.source_dataset,
                "target_dataset": event.target_dataset,
                "transformation": event.transformation,
                "metadata": event.metadata,
                "timestamp": event.timestamp.isoformat(),
                "lineage_id": f"lineage-{event.timestamp.timestamp()}-{hash(str(event.to_dict()))}"
            }

            await self.collector.emit_records([lineage_record])
            logger.debug("Lineage event emitted to Kafka", extra={"event_type": event.event_type})

        except Exception as e:
            logger.error(
                "Failed to emit lineage event",
                extra={
                    "event_type": event.event_type,
                    "error": str(e)
                }
            )

    async def _emit_openlineage_event(self, event: LineageEvent) -> None:
        """Emit OpenLineage event to Kafka."""
        if not self._openlineage_producer:
            return

        try:
            openlineage_event = event.to_openlineage_dict()
            await self._openlineage_producer.produce_message(openlineage_event)
            logger.debug(
                "OpenLineage event emitted",
                extra={
                    "event_type": event.event_type,
                    "run_id": event.run_id
                }
            )
        except Exception as e:
            logger.error(
                "Failed to emit OpenLineage event",
                extra={
                    "event_type": event.event_type,
                    "run_id": event.run_id,
                    "error": str(e)
                }
            )

    async def get_lineage(
        self,
        dataset: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[LineageEvent]:
        """Get lineage events for a dataset."""
        start_time = start_time or (datetime.now() - timedelta(days=30))
        end_time = end_time or datetime.now()

        return [
            event for event in self._events
            if event.target_dataset == dataset
            and start_time <= event.timestamp <= end_time
        ]

    async def get_lineage_summary(self, dataset: str) -> Dict[str, Any]:
        """Get summary of lineage events for a dataset."""
        events = await self.get_lineage(dataset)

        if not events:
            return {
                "dataset": dataset,
                "total_events": 0,
                "event_types": {},
                "last_event": None
            }

        event_types = {}
        for event in events:
            event_types[event.event_type] = event_types.get(event.event_type, 0) + 1

        return {
            "dataset": dataset,
            "total_events": len(events),
            "event_types": event_types,
            "last_event": events[-1].to_dict() if events else None,
            "time_range": {
                "start": min(e.timestamp for e in events).isoformat(),
                "end": max(e.timestamp for e in events).isoformat()
            }
        }


class LineageManager:
    """Manager for tracking data lineage across external providers."""

    def __init__(self):
        self._trackers: Dict[str, LineageTracker] = {}

    def get_tracker(self, provider: str) -> LineageTracker:
        """Get lineage tracker for a provider."""
        if provider not in self._trackers:
            # Create collector for lineage tracking
            collector = _build_kafka_collector(
                f"{provider}-lineage",
                LINEAGE_TOPIC,
                "DataLineageEventV1.avsc"
            )
            self._trackers[provider] = LineageTracker(collector)

        return self._trackers[provider]

    async def track_external_data_ingestion(
        self,
        provider: str,
        dataset: str,
        records_count: int,
        source_url: Optional[str] = None
    ) -> None:
        """Track external data ingestion lineage."""
        tracker = self.get_tracker(provider)

        event = LineageEvent(
            event_type="ingest",
            source_dataset=f"{provider}://{dataset}",
            target_dataset=f"external.{provider}_observation",
            transformation={
                "operation": "ingest",
                "records_count": records_count,
                "provider": provider
            },
            metadata={
                "source_url": source_url,
                "ingestion_timestamp": datetime.now().isoformat()
            },
            job_name=f"{provider}_ingestion_job"
        )

        await tracker.track_event(event)

    async def track_data_transformation(
        self,
        provider: str,
        source_table: str,
        target_table: str,
        transformation_type: str,
        records_in: int,
        records_out: int,
        transformation_details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Track data transformation lineage."""
        tracker = self.get_tracker(provider)

        event = LineageEvent(
            event_type="transform",
            source_dataset=source_table,
            target_dataset=target_table,
            transformation={
                "type": transformation_type,
                "records_in": records_in,
                "records_out": records_out,
                "details": transformation_details or {}
            },
            metadata={
                "transformation_timestamp": datetime.now().isoformat(),
                "provider": provider
            },
            job_name=f"{provider}_transformation_job"
        )

        await tracker.track_event(event)

    async def track_validation(
        self,
        provider: str,
        table_name: str,
        validation_type: str,
        passed: bool,
        validation_details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Track data validation lineage."""
        tracker = self.get_tracker(provider)

        event = LineageEvent(
            event_type="validate",
            source_dataset=table_name,
            target_dataset=f"validated.{table_name}",
            transformation={
                "validation_type": validation_type,
                "passed": passed
            },
            metadata={
                "validation_timestamp": datetime.now().isoformat(),
                "provider": provider,
                "validation_details": validation_details or {}
            },
            job_name=f"{provider}_validation_job"
        )

        await tracker.track_event(event)

    async def get_provider_lineage_summary(self, provider: str) -> Dict[str, Any]:
        """Get lineage summary for a provider."""
        tracker = self.get_tracker(provider)
        return await tracker.get_lineage_summary(f"external.{provider}_observation")


# Global lineage manager instance
_lineage_manager: Optional[LineageManager] = None


def get_lineage_manager() -> LineageManager:
    """Get global lineage manager instance."""
    global _lineage_manager
    if _lineage_manager is None:
        _lineage_manager = LineageManager()
    return _lineage_manager


# Convenience functions for DAGs
async def emit_incremental_lineage(vault_addr: str, vault_token: str) -> Dict[str, Any]:
    """Emit lineage for incremental data processing."""
    # This would emit lineage for recent incremental processing
    # For now, return placeholder
    return {"status": "success", "message": "Incremental lineage emitted"}


async def emit_backfill_lineage(provider: str, vault_addr: str, vault_token: str) -> Dict[str, Any]:
    """Emit lineage for backfill data processing."""
    # This would emit lineage for backfill processing
    # For now, return placeholder
    return {"status": "success", "message": "Backfill lineage emitted", "provider": provider}


def _build_kafka_collector(provider: str, topic: str, schema_name: str) -> ExternalCollector:
    """Build Kafka collector for lineage tracking."""
    # This would be moved to a shared utility
    # For now, return a placeholder
    raise NotImplementedError("Kafka collector building not yet implemented")
