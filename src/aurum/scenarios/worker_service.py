"""Scenario worker service that processes requests from the queue.

This service runs as a separate process/service that:
- Consumes scenario requests from Kafka topics
- Processes them with proper error handling and retries
- Produces results to output topics
- Handles backpressure and circuit breaker logic
"""

from __future__ import annotations

import asyncio
import json
import signal
from typing import Any, Dict, Optional

from ..telemetry.context import log_structured
from .event_pipeline import ScenarioEventPipeline, ScenarioEventPipelineConfig
from .kafka_client import create_kafka_producer, create_kafka_consumer
from .queue_manager import ScenarioQueueManager, RetryPolicy


class ScenarioWorkerService:
    """Service that processes scenario requests from Kafka queues."""

    def __init__(
        self,
        kafka_producer,
        kafka_consumer,
        worker_id: str = None,
        max_concurrent_requests: int = 10,
        event_pipeline: ScenarioEventPipeline | None = None,
    ):
        self.worker_id = worker_id or f"worker-{id(self)}"
        self.kafka_producer = kafka_producer
        self.kafka_consumer = kafka_consumer
        self.max_concurrent_requests = max_concurrent_requests
        self.event_pipeline = event_pipeline

        self.queue_manager = ScenarioQueueManager(
            kafka_producer=kafka_producer,
            kafka_consumer=kafka_consumer,
            max_concurrent_requests=max_concurrent_requests,
        )

        self.running = False
        self.shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the worker service."""
        self.running = True

        log_structured(
            "info",
            "scenario_worker_starting",
            worker_id=self.worker_id,
            max_concurrent_requests=self.max_concurrent_requests,
            event_pipeline_enabled=bool(self.event_pipeline),
        )

        # Set up signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._signal_handler)

        # Start consuming requests
        await self.queue_manager.start_consuming()

        # Keep the service running
        try:
            while self.running and not self.shutdown_event.is_set():
                await asyncio.sleep(1)

                # Log status periodically
                if self.running:
                    status = await self.queue_manager.get_queue_status()
                    log_structured(
                        "debug",
                        "worker_status",
                        worker_id=self.worker_id,
                        **status
                    )

        except Exception as exc:
            log_structured(
                "error",
                "worker_service_error",
                worker_id=self.worker_id,
                error=str(exc),
            )
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the worker service."""
        if not self.running:
            return

        self.running = False
        log_structured("info", "scenario_worker_stopping", worker_id=self.worker_id)

        # Cancel all processing tasks
        for task in self.queue_manager.processing_tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        if self.queue_manager.processing_tasks:
            await asyncio.gather(*self.queue_manager.processing_tasks, return_exceptions=True)

        self.shutdown_event.set()
        log_structured("info", "scenario_worker_stopped", worker_id=self.worker_id)

    def _signal_handler(self) -> None:
        """Handle shutdown signals."""
        log_structured("info", "shutdown_signal_received", worker_id=self.worker_id)
        self.running = False
        self.shutdown_event.set()


async def run_worker_service(
    kafka_config: Dict[str, Any],
    worker_id: Optional[str] = None,
    max_concurrent_requests: int = 10,
) -> None:
    """Run a scenario worker service instance."""

    bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")
    consumer_group = kafka_config.get("consumer_group", "scenario-workers")
    topics = kafka_config.get("topics", ["aurum.scenario.request.v1", "aurum.scenario.request.retry.v1"])

    # Create async Kafka clients
    kafka_producer = create_kafka_producer(bootstrap_servers)
    kafka_consumer = create_kafka_consumer(
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group,
        topics=topics,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        max_poll_records=100,
    )

    event_pipeline_cfg = kafka_config.get("event_pipeline")
    event_pipeline: ScenarioEventPipeline | None = None
    if event_pipeline_cfg:
        pipeline_config = ScenarioEventPipelineConfig(
            lifecycle_topic=event_pipeline_cfg.get("lifecycle_topic", "aurum.scenario.output.v1"),
            default_schema_subject=event_pipeline_cfg.get("schema_subject", "aurum.scenario.output.v1-value"),
            consumer_group=event_pipeline_cfg.get(
                "consumer_group",
                f"{consumer_group}-pilot",
            ),
            batch_size=int(event_pipeline_cfg.get("batch_size", 100)),
            poll_interval=float(event_pipeline_cfg.get("poll_interval", 1.0)),
            bootstrap_servers=None if event_pipeline_cfg.get("in_memory", False) else bootstrap_servers,
            in_memory=bool(event_pipeline_cfg.get("in_memory", False)),
            dlq_topic=event_pipeline_cfg.get("dlq_topic"),
        )
        event_pipeline = ScenarioEventPipeline(config=pipeline_config)

        async def _log_lifecycle_event(message: Any) -> None:
            try:
                payload = message.value if hasattr(message, "value") else message
            except Exception:
                payload = None
            log_structured(
                "info",
                "scenario_event_consumed",
                worker_id=worker_id or "scenario-worker",
                topic=getattr(message, "topic", pipeline_config.lifecycle_topic),
                payload=payload,
            )

        event_pipeline.register_lifecycle_handler(_log_lifecycle_event)

    worker = ScenarioWorkerService(
        kafka_producer=kafka_producer,
        kafka_consumer=kafka_consumer,
        worker_id=worker_id,
        max_concurrent_requests=max_concurrent_requests,
        event_pipeline=event_pipeline,
    )

    try:
        await kafka_producer.start()
        await kafka_consumer.start()
        if event_pipeline is not None:
            await event_pipeline.start()
        await worker.start()
    except KeyboardInterrupt:
        log_structured("info", "worker_service_interrupted")
    except Exception as exc:
        log_structured("error", "worker_service_crashed", error=str(exc))
    finally:
        await worker.stop()
        if event_pipeline is not None:
            await event_pipeline.stop()
        await kafka_producer.stop()
        await kafka_consumer.stop()
