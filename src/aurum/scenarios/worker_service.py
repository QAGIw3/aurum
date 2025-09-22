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
from .queue_manager import ScenarioQueueManager, RetryPolicy


class ScenarioWorkerService:
    """Service that processes scenario requests from Kafka queues."""

    def __init__(
        self,
        kafka_producer,
        kafka_consumer,
        worker_id: str = None,
        max_concurrent_requests: int = 10,
    ):
        self.worker_id = worker_id or f"worker-{id(self)}"
        self.kafka_producer = kafka_producer
        self.kafka_consumer = kafka_consumer
        self.max_concurrent_requests = max_concurrent_requests

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

    # In a real implementation, these would be properly configured Kafka clients
    # For now, this is a placeholder
    kafka_producer = None  # Replace with actual Kafka producer
    kafka_consumer = None  # Replace with actual Kafka consumer

    worker = ScenarioWorkerService(
        kafka_producer=kafka_producer,
        kafka_consumer=kafka_consumer,
        worker_id=worker_id,
        max_concurrent_requests=max_concurrent_requests,
    )

    try:
        await worker.start()
    except KeyboardInterrupt:
        log_structured("info", "worker_service_interrupted")
    except Exception as exc:
        log_structured("error", "worker_service_crashed", error=str(exc))
    finally:
        await worker.stop()
