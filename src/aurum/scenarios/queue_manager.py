"""Scenario queue manager with backpressure, retries, and dead-letter support.

This module provides a robust queueing system for scenario processing that includes:
- Backpressure handling to prevent worker overload
- Automatic retries with exponential backoff
- Dead-letter queue for permanently failed requests
- Priority-based processing
- Idempotency support
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

import aiohttp
from pydantic import BaseModel, Field

from ..telemetry.context import get_correlation_id, get_tenant_id, get_user_id, log_structured


class QueueMetrics(BaseModel):
    """Metrics for queue monitoring."""

    requests_queued: int = Field(default=0, description="Number of requests currently queued")
    requests_processing: int = Field(default=0, description="Number of requests being processed")
    requests_retried: int = Field(default=0, description="Total requests that have been retried")
    requests_failed: int = Field(default=0, description="Total requests that have failed")
    requests_dead_lettered: int = Field(default=0, description="Total requests sent to dead letter queue")
    avg_processing_time: float = Field(default=0.0, description="Average processing time in seconds")
    last_backpressure_event: Optional[datetime] = Field(None, description="Timestamp of last backpressure event")


class BackpressureStrategy:
    """Backpressure strategies for queue management."""

    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    CIRCUIT_BREAKER = "circuit_breaker"


class RetryPolicy:
    """Retry policy configuration."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay_seconds: float = 1.0,
        max_delay_seconds: float = 300.0,
        backoff_multiplier: float = 2.0,
        strategy: str = BackpressureStrategy.EXPONENTIAL_BACKOFF,
    ):
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.backoff_multiplier = backoff_multiplier
        self.strategy = strategy

    def calculate_delay(self, retry_count: int) -> float:
        """Calculate delay for a given retry count."""
        if self.strategy == BackpressureStrategy.EXPONENTIAL_BACKOFF:
            delay = self.base_delay_seconds * (self.backoff_multiplier ** retry_count)
        elif self.strategy == BackpressureStrategy.LINEAR_BACKOFF:
            delay = self.base_delay_seconds * retry_count
        else:
            delay = self.base_delay_seconds

        return min(delay, self.max_delay_seconds)


class ScenarioQueueManager:
    """Manager for scenario processing queue with backpressure and retry support."""

    def __init__(
        self,
        kafka_producer,
        kafka_consumer,
        max_concurrent_requests: int = 100,
        backpressure_threshold: float = 0.9,
        circuit_breaker_failure_threshold: int = 10,
        circuit_breaker_reset_timeout: int = 300,  # 5 minutes
    ):
        self.kafka_producer = kafka_producer
        self.kafka_consumer = kafka_consumer
        self.max_concurrent_requests = max_concurrent_requests
        self.backpressure_threshold = backpressure_threshold

        # Circuit breaker state
        self.circuit_breaker_failure_threshold = circuit_breaker_failure_threshold
        self.circuit_breaker_reset_timeout = circuit_breaker_reset_timeout
        self.consecutive_failures = 0
        self.circuit_breaker_open = False
        self.circuit_breaker_last_failure = None

        # Processing state
        self.processing_requests: Set[str] = set()
        self.retry_queue: Dict[str, Any] = {}
        self.dead_letter_queue: List[Dict[str, Any]] = []

        # Metrics
        self.metrics = QueueMetrics()

        # Processing tasks
        self.processing_tasks: Set[asyncio.Task] = set()

    async def submit_request(
        self,
        scenario_id: str,
        tenant_id: str,
        requested_by: str,
        request_data: Dict[str, Any],
        priority: str = "normal",
        timeout_minutes: int = 60,
        max_retries: int = 3,
        idempotency_key: Optional[str] = None,
        worker_requirements: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Submit a scenario request to the queue."""

        # Check circuit breaker
        if self.circuit_breaker_open:
            if self._should_reset_circuit_breaker():
                await self._reset_circuit_breaker()
            else:
                raise RuntimeError("Circuit breaker is open - too many consecutive failures")

        request_id = str(uuid4())

        # Create enhanced request with retry metadata
        enhanced_request = {
            "request_id": request_id,
            "scenario_id": scenario_id,
            "tenant_id": tenant_id,
            "requested_by": requested_by,
            "asof_date": request_data.get("asof_date"),
            "curve_def_ids": request_data.get("curve_def_ids", []),
            "assumptions": request_data.get("assumptions", []),
            "submitted_ts": int(time.time() * 1_000_000),  # microseconds
            "priority": priority,
            "timeout_minutes": timeout_minutes,
            "max_retries": max_retries,
            "retry_count": 0,
            "idempotency_key": idempotency_key,
            "backpressure_strategy": BackpressureStrategy.EXPONENTIAL_BACKOFF,
            "worker_requirements": worker_requirements or {},
            "metadata": {
                "submitted_via": "queue_manager",
                "correlation_id": get_correlation_id(),
            }
        }

        # Check backpressure
        if len(self.processing_requests) >= int(self.max_concurrent_requests * self.backpressure_threshold):
            await self._handle_backpressure(enhanced_request)
        else:
            await self._send_to_kafka(enhanced_request, "aurum.scenario.request.v1")

        self.metrics.requests_queued += 1
        log_structured(
            "info",
            "scenario_request_queued",
            request_id=request_id,
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            priority=priority,
            max_retries=max_retries,
        )

        return request_id

    async def process_request(self, request: Dict[str, Any]) -> bool:
        """Process a scenario request with retry logic."""

        request_id = request["request_id"]

        # Check if request is already being processed
        if request_id in self.processing_requests:
            log_structured("warning", "request_already_processing", request_id=request_id)
            return False

        # Check idempotency
        if await self._is_duplicate_request(request):
            log_structured("info", "duplicate_request_ignored", request_id=request_id)
            return True

        self.processing_requests.add(request_id)
        self.metrics.requests_processing += 1

        try:
            # Simulate request processing (replace with actual processing logic)
            success = await self._execute_scenario_request(request)

            if success:
                await self._send_to_kafka(request, "aurum.scenario.output.v1")
                self.consecutive_failures = 0
                self.metrics.requests_queued -= 1
            else:
                await self._handle_request_failure(request)

        except Exception as exc:
            log_structured(
                "error",
                "request_processing_exception",
                request_id=request_id,
                error=str(exc),
            )
            await self._handle_request_failure(request)
        finally:
            self.processing_requests.discard(request_id)
            self.metrics.requests_processing -= 1

        return True

    async def retry_request(self, request: Dict[str, Any]) -> None:
        """Retry a failed request with backoff."""

        request_id = request["request_id"]
        retry_count = request["retry_count"] + 1

        if retry_count > request["max_retries"]:
            await self._send_to_dead_letter(request)
            return

        # Update request with retry metadata
        request["retry_count"] = retry_count
        request["parent_request_id"] = request_id
        request["request_id"] = str(uuid4())

        # Calculate retry delay
        retry_policy = RetryPolicy()
        delay = retry_policy.calculate_delay(retry_count)
        request["retry_delay_seconds"] = delay

        self.metrics.requests_retried += 1

        # Schedule retry with delay
        asyncio.create_task(self._delayed_retry(request, delay))

        log_structured(
            "info",
            "request_scheduled_for_retry",
            original_request_id=request_id,
            new_request_id=request["request_id"],
            retry_count=retry_count,
            delay_seconds=delay,
        )

    async def _delayed_retry(self, request: Dict[str, Any], delay: float) -> None:
        """Execute delayed retry."""
        await asyncio.sleep(delay)
        await self._send_to_kafka(request, "aurum.scenario.request.retry.v1")

    async def _handle_backpressure(self, request: Dict[str, Any]) -> None:
        """Handle backpressure by applying circuit breaker or queuing."""
        log_structured(
            "warning",
            "backpressure_detected",
            current_requests=len(self.processing_requests),
            max_requests=self.max_concurrent_requests,
        )

        # For now, just reject the request. In a real implementation,
        # this could implement more sophisticated backpressure strategies
        raise RuntimeError("System under backpressure - request rejected")

    async def _handle_request_failure(self, request: Dict[str, Any]) -> None:
        """Handle failed request with retry logic."""
        self.consecutive_failures += 1

        if self.consecutive_failures >= self.circuit_breaker_failure_threshold:
            await self._open_circuit_breaker()

        await self.retry_request(request)

    async def _open_circuit_breaker(self) -> None:
        """Open the circuit breaker."""
        self.circuit_breaker_open = True
        self.circuit_breaker_last_failure = datetime.utcnow()

        log_structured(
            "error",
            "circuit_breaker_opened",
            consecutive_failures=self.consecutive_failures,
            threshold=self.circuit_breaker_failure_threshold,
        )

    async def _reset_circuit_breaker(self) -> None:
        """Reset the circuit breaker."""
        self.circuit_breaker_open = False
        self.consecutive_failures = 0

        log_structured("info", "circuit_breaker_reset")

    def _should_reset_circuit_breaker(self) -> bool:
        """Check if circuit breaker should be reset."""
        if not self.circuit_breaker_last_failure:
            return True

        elapsed = (datetime.utcnow() - self.circuit_breaker_last_failure).total_seconds()
        return elapsed >= self.circuit_breaker_reset_timeout

    async def _send_to_kafka(self, request: Dict[str, Any], topic: str) -> None:
        """Send request to Kafka topic."""
        try:
            await self.kafka_producer.send_and_wait(
                topic,
                key=request["request_id"].encode(),
                value=json.dumps(request).encode(),
            )
        except Exception as exc:
            log_structured(
                "error",
                "kafka_send_failed",
                topic=topic,
                request_id=request["request_id"],
                error=str(exc),
            )
            raise

    async def _send_to_dead_letter(self, request: Dict[str, Any]) -> None:
        """Send permanently failed request to dead letter queue."""
        self.dead_letter_queue.append({
            "request": request,
            "failed_at": datetime.utcnow(),
            "reason": "max_retries_exceeded",
        })

        # Also send to Kafka DLQ topic
        await self._send_to_kafka(request, "aurum.scenario.request.dlq.v1")

        self.metrics.requests_dead_lettered += 1

        log_structured(
            "error",
            "request_sent_to_dead_letter",
            request_id=request["request_id"],
            retry_count=request["retry_count"],
        )

    async def _is_duplicate_request(self, request: Dict[str, Any]) -> bool:
        """Check if request is a duplicate based on idempotency key."""
        if not request.get("idempotency_key"):
            return False

        # In a real implementation, this would check against a persistent store
        return request["idempotency_key"] in self.retry_queue

    async def _execute_scenario_request(self, request: Dict[str, Any]) -> bool:
        """Execute the actual scenario request processing."""
        # This is a placeholder - replace with actual scenario processing logic
        request_id = request["request_id"]

        log_structured(
            "info",
            "processing_scenario_request",
            request_id=request_id,
            scenario_id=request["scenario_id"],
        )

        # Simulate processing time and occasional failures
        await asyncio.sleep(1)

        # Simulate 90% success rate
        import random
        success = random.random() < 0.9

        if success:
            log_structured("info", "scenario_request_completed", request_id=request_id)
        else:
            log_structured("warning", "scenario_request_failed", request_id=request_id)

        return success

    async def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status and metrics."""
        return {
            "processing_requests": len(self.processing_requests),
            "retry_queue_size": len(self.retry_queue),
            "dead_letter_queue_size": len(self.dead_letter_queue),
            "circuit_breaker_open": self.circuit_breaker_open,
            "consecutive_failures": self.consecutive_failures,
            "max_concurrent_requests": self.max_concurrent_requests,
            "metrics": self.metrics.dict(),
        }

    async def start_consuming(self) -> None:
        """Start consuming requests from Kafka topics."""
        # Start consumer for main request topic
        asyncio.create_task(self._consume_topic("aurum.scenario.request.v1"))

        # Start consumer for retry topic
        asyncio.create_task(self._consume_topic("aurum.scenario.request.retry.v1"))

    async def _consume_topic(self, topic: str) -> None:
        """Consume messages from a Kafka topic."""
        try:
            async for message in self.kafka_consumer:
                if message.topic == topic:
                    try:
                        request = json.loads(message.value.decode())
                        await self.process_request(request)
                    except Exception as exc:
                        log_structured(
                            "error",
                            "message_processing_failed",
                            topic=topic,
                            offset=message.offset,
                            error=str(exc),
                        )
        except Exception as exc:
            log_structured("error", "consumer_error", topic=topic, error=str(exc))
