"""Auto-reforecast orchestration service with event-driven triggers.

This service provides:
- Event-driven forecast triggers via Kafka topics
- Debounce and backpressure controls
- Short-horizon forecast re-runs when new data arrives
- Integration with feature store and forecasting service
- Automatic forecast invalidation and regeneration
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict, deque
from enum import Enum
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Callable, Awaitable
from uuid import uuid4, UUID

from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager
from .feature_store_service import get_feature_store_service

try:
    from ...api.v2.forecasting import (
        ForecastRequest,
        ForecastType,
        QuantileLevel,
        ForecastInterval,
    )
except Exception:  # pragma: no cover - fallback for test environments
    class ForecastType(str, Enum):
        """Fallback forecast type enumeration when full API module is unavailable."""

        LOAD = "load"
        PRICE = "price"

    class ForecastInterval(str, Enum):
        """Fallback forecast interval enumeration."""

        HOURLY = "hourly"

    class QuantileLevel(str, Enum):
        """Fallback quantile levels."""

        P10 = "P10"
        P50 = "P50"
        P90 = "P90"

    class ForecastRequest(BaseModel):
        """Lightweight stand-in for the primary ForecastRequest model."""

        forecast_type: ForecastType
        target_variable: str
        geography: str
        start_date: datetime
        end_date: datetime
        quantiles: List[QuantileLevel] = Field(default_factory=list)
        interval: ForecastInterval = ForecastInterval.HOURLY


class TriggerCondition(BaseModel):
    """Condition that triggers forecast re-run."""

    data_source: str  # "weather", "load", "price", "iso"
    geography: str = "US"
    threshold_type: str  # "absolute", "percentage", "volatility"
    threshold_value: float
    lookback_hours: int = 24
    min_change_required: bool = True
    fields: Optional[List[str]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DebounceConfig(BaseModel):
    """Configuration for forecast debouncing."""

    enabled: bool = True
    window_seconds: int = 300  # 5 minutes debounce window
    max_concurrent_triggers: int = 5
    priority_threshold: float = 0.5  # Only trigger for high-priority events


class BackpressureConfig(BaseModel):
    """Configuration for backpressure control."""

    enabled: bool = True
    max_queue_size: int = 1000
    max_processing_rate_per_second: int = 10
    queue_timeout_seconds: int = 60
    drop_low_priority_on_pressure: bool = True
    priority_threshold: float = 0.5


class ForecastTrigger(BaseModel):
    """Individual forecast trigger configuration."""

    trigger_id: str
    name: str
    description: str
    conditions: List[TriggerCondition]
    forecast_config: ForecastRequest
    enabled: bool = True
    priority: float = 1.0  # Higher values = higher priority
    cooldown_minutes: int = 30  # Minimum time between triggers
    last_triggered: Optional[datetime] = None
    trigger_count: int = 0


class TriggerEvent(BaseModel):
    """Event that triggers forecast re-run."""

    event_id: str
    trigger_id: str
    data_source: str
    geography: str
    timestamp: datetime
    data_changes: Dict[str, float]  # Field name -> change magnitude
    priority_score: float = 1.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ReforcastJob(BaseModel):
    """Job for re-running forecasts."""

    job_id: str
    trigger_event: TriggerEvent
    forecast_request: ForecastRequest
    priority: float
    created_at: datetime
    scheduled_for: datetime
    status: str = "pending"  # pending, processing, completed, failed
    attempts: int = 0
    max_attempts: int = 3
    error_message: Optional[str] = None


class AutoReforecastService:
    """Service for orchestrating automatic forecast re-runs."""

    def __init__(
        self,
        debounce_config: Optional[DebounceConfig] = None,
        backpressure_config: Optional[BackpressureConfig] = None,
        kafka_config: Optional[Dict[str, Any]] = None
    ):
        """Initialize auto-reforecast service.

        Args:
            debounce_config: Debounce configuration
            backpressure_config: Backpressure configuration
            kafka_config: Kafka configuration for event consumption
        """
        self.debounce_config = debounce_config or DebounceConfig()
        self.backpressure_config = backpressure_config or BackpressureConfig()
        self.kafka_config = kafka_config or {}

        # Trigger management
        self.triggers: Dict[str, ForecastTrigger] = {}
        self.active_triggers: Set[str] = set()

        # Event processing
        self.event_queue: asyncio.Queue[TriggerEvent] = asyncio.Queue(
            maxsize=self.backpressure_config.max_queue_size
        )
        self.processing_semaphore = asyncio.Semaphore(
            self.debounce_config.max_concurrent_triggers
        )

        # Job tracking
        self.pending_jobs: Dict[str, ReforcastJob] = {}
        self.processing_jobs: Dict[str, ReforcastJob] = {}
        self.completed_jobs: Dict[str, ReforcastJob] = {}
        self._jobs_by_trigger: Dict[str, Set[str]] = defaultdict(set)

        # Rate limiting
        self.trigger_timestamps: Dict[str, deque] = defaultdict(deque)
        self.processing_rate_limiter: Dict[str, datetime] = {}
        self._processed_event_ids: Dict[str, datetime] = {}
        self._processed_event_window = timedelta(hours=6)
        self._trigger_event_history: deque = deque(maxlen=500)
        self._job_audit_log: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._job_audit_entry_limit = 50
        self._retry_backoff_base_seconds = 30
        self._retry_backoff_max_seconds = 900

        # Kafka consumer (would be initialized)
        self.kafka_consumer = None
        self._kafka_task: Optional[asyncio.Task] = None

        # Background tasks
        self._event_processor_task: Optional[asyncio.Task] = None
        self._job_processor_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        self.logger = logging.getLogger(__name__)
        self.telemetry = get_telemetry_facade()

        # Optional persistence hooks
        self._job_repository = None  # type: ignore[var-annotated]
        self._tenant_resolver: Optional[Callable[["TriggerEvent"], Optional[str]]] = None

        self.logger.info("Auto-reforecast service initialized")

    async def start(self) -> None:
        """Start the auto-reforecast service."""
        self.telemetry.info("Starting auto-reforecast service")

        try:
            # Start Kafka consumer if configured
            if self.kafka_config.get("enabled", False):
                await self._start_kafka_consumer()

            # Start background processors
            self._event_processor_task = asyncio.create_task(self._event_processor_loop())
            self._job_processor_task = asyncio.create_task(self._job_processor_loop())
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

            self.telemetry.info("Auto-reforecast service started successfully")

        except Exception as e:
            self.telemetry.error("Failed to start auto-reforecast service", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the auto-reforecast service."""
        self.telemetry.info("Stopping auto-reforecast service")

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel background tasks
        tasks = []
        if self._event_processor_task:
            tasks.append(self._event_processor_task)
        if self._job_processor_task:
            tasks.append(self._job_processor_task)
        if self._cleanup_task:
            tasks.append(self._cleanup_task)
        if self._kafka_task:
            tasks.append(self._kafka_task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Close Kafka consumer
        if self.kafka_consumer:
            await self.kafka_consumer.close()

        self.telemetry.info("Auto-reforecast service stopped")

    async def _start_kafka_consumer(self) -> None:
        """Start Kafka consumer for event ingestion."""
        try:
            from aiokafka import AIOKafkaConsumer

            topics = self.kafka_config.get("topics", [
                "weather.updates",
                "load.updates",
                "price.updates",
                "iso.updates"
            ])

            self.kafka_consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=self.kafka_config.get("bootstrap_servers", ["localhost:9092"]),
                group_id=self.kafka_config.get("group_id", "auto-reforecast-service"),
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )

            await self.kafka_consumer.start()

            # Start consumer task
            self._kafka_task = asyncio.create_task(self._kafka_consumer_loop())

            self.telemetry.info("Kafka consumer started", topics=topics)

        except Exception as e:
            self.telemetry.error("Failed to start Kafka consumer", error=str(e))
            raise

    async def _kafka_consumer_loop(self) -> None:
        """Background task to consume Kafka events."""
        if not self.kafka_consumer:
            return

        try:
            async for message in self.kafka_consumer:
                try:
                    # Parse event
                    event_data = message.value
                    trigger_event = self._parse_kafka_event(event_data, message.topic)

                    if trigger_event:
                        await self._process_trigger_event(trigger_event)

                except Exception as e:
                    self.telemetry.error("Failed to process Kafka message", error=str(e))

        except asyncio.CancelledError:
            self.telemetry.info("Kafka consumer loop cancelled")
        except Exception as e:
            self.telemetry.error("Kafka consumer loop failed", error=str(e))

    def _parse_kafka_event(self, event_data: Dict[str, Any], topic: str) -> Optional[TriggerEvent]:
        """Parse Kafka message into trigger event."""
        try:
            # Extract basic event info
            event_id = event_data.get("event_id", str(uuid4()))
            data_source = topic.split(".")[0]  # weather, load, price, iso
            geography = event_data.get("geography", "US")
            timestamp = datetime.fromisoformat(event_data.get("timestamp", datetime.utcnow().isoformat()))

            # Calculate data changes
            data_changes = {}
            current_values = event_data.get("current_values", {})
            previous_values = event_data.get("previous_values", {})

            for field, current_value in current_values.items():
                previous_value = previous_values.get(field)
                if previous_value is not None:
                    change = abs(current_value - previous_value)
                    if previous_value != 0:
                        change_pct = change / abs(previous_value)
                        data_changes[field] = max(change, change_pct)  # Use larger magnitude
                    else:
                        data_changes[field] = change

            # Calculate priority score
            max_change = max(data_changes.values()) if data_changes else 0.0
            priority_score = min(max_change * 10, 1.0)  # Scale to 0-1

            return TriggerEvent(
                event_id=event_id,
                trigger_id="",  # Will be determined by matching conditions
                data_source=data_source,
                geography=geography,
                timestamp=timestamp,
                data_changes=data_changes,
                priority_score=priority_score,
                metadata=event_data.get("metadata", {})
            )

        except Exception as e:
            self.telemetry.error("Failed to parse Kafka event", error=str(e))
            return None

    async def _process_trigger_event(self, event: TriggerEvent) -> None:
        """Process incoming trigger event."""
        try:
            # Find matching triggers
            matching_triggers = self._find_matching_triggers(event)

            for trigger in matching_triggers:
                priority_score = max(event.priority_score, trigger.priority)

                if not self._should_trigger(trigger, event, priority_score):
                    self._log_trigger_event(
                        event,
                        status="skipped",
                        reason="debounce_filter",
                        trigger=trigger
                    )
                    continue

                triggered_event = TriggerEvent(
                    event_id=f"{event.event_id}_{trigger.trigger_id}",
                    trigger_id=trigger.trigger_id,
                    data_source=event.data_source,
                    geography=event.geography,
                    timestamp=event.timestamp,
                    data_changes=event.data_changes,
                    priority_score=priority_score,
                    metadata=dict(event.metadata)
                )

                if self._is_duplicate_event(triggered_event):
                    self._log_trigger_event(
                        triggered_event,
                        status="duplicate",
                        reason="idempotency_guard",
                        trigger=trigger
                    )
                    continue

                if self._has_active_job(trigger.trigger_id):
                    self._log_trigger_event(
                        triggered_event,
                        status="skipped",
                        reason="rate_limit_guard",
                        trigger=trigger
                    )
                    continue

                queued = False

                try:
                    self.event_queue.put_nowait(triggered_event)
                    queued = True

                except asyncio.QueueFull:
                    if self.backpressure_config.drop_low_priority_on_pressure and (
                        priority_score < self.backpressure_config.priority_threshold
                    ):
                        self.telemetry.warning(
                            "Dropping low-priority trigger event due to queue pressure",
                            trigger_id=trigger.trigger_id,
                            priority=priority_score
                        )
                        self._log_trigger_event(
                            triggered_event,
                            status="dropped",
                            reason="queue_pressure",
                            trigger=trigger
                        )
                        continue

                    try:
                        await asyncio.wait_for(
                            self.event_queue.put(triggered_event),
                            timeout=self.backpressure_config.queue_timeout_seconds
                        )
                        queued = True
                    except asyncio.TimeoutError:
                        self.telemetry.error(
                            "Failed to queue trigger event - timeout",
                            trigger_id=trigger.trigger_id
                        )
                        self._log_trigger_event(
                            triggered_event,
                            status="dropped",
                            reason="queue_timeout",
                            trigger=trigger
                        )
                        continue

                if queued:
                    self._mark_event_processed(triggered_event)
                    self._record_trigger_activity(trigger, triggered_event)

                    self.telemetry.info(
                        "Trigger event queued",
                        trigger_id=trigger.trigger_id,
                        event_id=triggered_event.event_id,
                        priority=triggered_event.priority_score
                    )

                    self._log_trigger_event(
                        triggered_event,
                        status="queued",
                        trigger=trigger
                    )

        except Exception as e:
            self.telemetry.error("Failed to process trigger event", error=str(e))

    def _find_matching_triggers(self, event: TriggerEvent) -> List[ForecastTrigger]:
        """Find triggers that match the event."""
        matching = []

        for trigger in self.triggers.values():
            if not trigger.enabled:
                continue

            # Check if trigger conditions match event
            if self._event_matches_trigger_conditions(event, trigger):
                matching.append(trigger)

        return matching

    def _event_matches_trigger_conditions(self, event: TriggerEvent, trigger: ForecastTrigger) -> bool:
        """Check if event matches trigger conditions."""
        for condition in trigger.conditions:
            if condition.data_source != event.data_source:
                continue

            if condition.geography != event.geography:
                continue

            field_filter = condition.fields or condition.metadata.get("fields")

            if field_filter:
                relevant_changes = [
                    change
                    for field, change in event.data_changes.items()
                    if field in field_filter
                ]
            else:
                relevant_changes = list(event.data_changes.values())

            if not relevant_changes:
                continue

            max_change = max(relevant_changes)

            if condition.threshold_type == "absolute":
                if max_change >= condition.threshold_value:
                    return True
            elif condition.threshold_type == "percentage":
                if max_change >= condition.threshold_value:
                    return True
            elif condition.threshold_type == "volatility":
                # Check volatility over lookback period
                if self._check_volatility_threshold(event, condition):
                    return True

        return False

    def _check_volatility_threshold(self, event: TriggerEvent, condition: TriggerCondition) -> bool:
        """Check if volatility threshold is met."""
        # In real implementation, would calculate volatility over lookback period
        # For now, use simplified logic
        return any(change > condition.threshold_value for change in event.data_changes.values())

    def _should_trigger(
        self,
        trigger: ForecastTrigger,
        event: TriggerEvent,
        priority_score: Optional[float] = None
    ) -> bool:
        """Check if trigger should fire based on debounce, cooldown, and priority."""
        if not self.debounce_config.enabled:
            return True

        effective_priority = priority_score if priority_score is not None else max(
            event.priority_score,
            trigger.priority
        )

        if effective_priority < self.debounce_config.priority_threshold:
            return False

        now = datetime.utcnow()
        trigger_key = f"{trigger.trigger_id}:{event.data_source}:{event.geography}"

        # Check cooldown
        if trigger.last_triggered:
            cooldown_end = trigger.last_triggered + timedelta(minutes=trigger.cooldown_minutes)
            if now < cooldown_end:
                return False

        # Check debounce window and concurrent triggers
        window_start = now - timedelta(seconds=self.debounce_config.window_seconds)
        timestamps = self.trigger_timestamps[trigger_key]

        while timestamps and timestamps[0] < window_start:
            timestamps.popleft()

        if len(timestamps) >= self.debounce_config.max_concurrent_triggers:
            return False

        return True

    def _record_trigger_activity(self, trigger: ForecastTrigger, event: TriggerEvent) -> None:
        """Record a trigger firing for debounce tracking."""

        trigger_key = f"{trigger.trigger_id}:{event.data_source}:{event.geography}"
        timestamps = self.trigger_timestamps[trigger_key]

        window_start = datetime.utcnow() - timedelta(seconds=self.debounce_config.window_seconds)
        while timestamps and timestamps[0] < window_start:
            timestamps.popleft()

        timestamps.append(datetime.utcnow())

    def _log_trigger_event(
        self,
        event: TriggerEvent,
        *,
        status: str,
        reason: Optional[str] = None,
        trigger: Optional[ForecastTrigger] = None
    ) -> None:
        """Track trigger event outcomes for auditing."""

        record = {
            "event_id": event.event_id,
            "trigger_id": event.trigger_id,
            "trigger_name": trigger.name if trigger else None,
            "data_source": event.data_source,
            "geography": event.geography,
            "priority": event.priority_score,
            "status": status,
            "reason": reason,
            "event_timestamp": event.timestamp,
            "recorded_at": datetime.utcnow(),
        }

        self._trigger_event_history.append(record)

    def _is_duplicate_event(self, event: TriggerEvent) -> bool:
        """Check whether the trigger event was already processed recently."""

        last_seen = self._processed_event_ids.get(event.event_id)
        if not last_seen:
            return False

        return datetime.utcnow() - last_seen < self._processed_event_window

    def _mark_event_processed(self, event: TriggerEvent) -> None:
        """Remember processed event for idempotency with eviction."""

        self._processed_event_ids[event.event_id] = datetime.utcnow()

        if len(self._processed_event_ids) > self._trigger_event_history.maxlen:
            # Prune oldest entries beyond audit window size
            cutoff = datetime.utcnow() - self._processed_event_window
            self._processed_event_ids = {
                event_id: seen_at
                for event_id, seen_at in self._processed_event_ids.items()
                if seen_at >= cutoff
            }

    def _has_active_job(self, trigger_id: str) -> bool:
        """Determine if there is already an active job for the trigger."""

        jobs = self._jobs_by_trigger.get(trigger_id)
        return bool(jobs)

    def _release_job_from_trigger(self, job: ReforcastJob) -> None:
        """Remove job tracking for the trigger when lifecycle completes."""

        jobs = self._jobs_by_trigger.get(job.trigger_event.trigger_id)
        if not jobs:
            return

        jobs.discard(job.job_id)
        if not jobs:
            self._jobs_by_trigger.pop(job.trigger_event.trigger_id, None)

    def _record_job_audit(
        self,
        job: ReforcastJob,
        *,
        status: str,
        detail: Optional[str] = None
    ) -> None:
        """Append audit metadata for a job lifecycle transition."""

        entry = {
            "job_id": job.job_id,
            "trigger_id": job.trigger_event.trigger_id,
            "status": status,
            "detail": detail,
            "attempt": job.attempts,
            "timestamp": datetime.utcnow(),
        }

        history = self._job_audit_log[job.job_id]
        history.append(entry)

        if len(history) > self._job_audit_entry_limit:
            del history[: len(history) - self._job_audit_entry_limit]

    def _compute_backoff_delay(self, attempt: int) -> timedelta:
        """Compute exponential backoff delay for a retry attempt."""

        attempt = max(attempt, 1)
        delay_seconds = min(
            self._retry_backoff_base_seconds * (2 ** (attempt - 1)),
            self._retry_backoff_max_seconds
        )
        return timedelta(seconds=delay_seconds)

    async def _event_processor_loop(self) -> None:
        """Background task to process trigger events."""
        while not self._shutdown_event.is_set():
            try:
                # Wait for event with timeout
                try:
                    event = await asyncio.wait_for(
                        self.event_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # Process event
                await self._process_event_for_reforecast(event)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.error("Event processor loop failed", error=str(e))

    async def _process_event_for_reforecast(self, event: TriggerEvent) -> None:
        """Process trigger event and create reforecast jobs."""
        try:
            # Find matching triggers
            matching_triggers = [
                trigger for trigger in self.triggers.values()
                if trigger.trigger_id == event.trigger_id and trigger.enabled
            ]

            for trigger in matching_triggers:
                if self._has_active_job(trigger.trigger_id):
                    self._log_trigger_event(
                        event,
                        status="skipped",
                        reason="rate_limit_guard",
                        trigger=trigger
                    )
                    continue

                # Create reforecast job
                job = ReforcastJob(
                    job_id=str(uuid4()),
                    trigger_event=event,
                    forecast_request=trigger.forecast_config,
                    priority=trigger.priority,
                    created_at=datetime.utcnow(),
                    scheduled_for=datetime.utcnow()
                )

                self.pending_jobs[job.job_id] = job
                self._jobs_by_trigger[trigger.trigger_id].add(job.job_id)
                self._record_job_audit(job, status="queued")

                # Persist to repository if configured
                await self._maybe_persist_job(job, event)

                # Update trigger stats
                trigger.last_triggered = event.timestamp
                trigger.trigger_count += 1

                self.telemetry.info(
                    "Reforecast job created",
                    job_id=job.job_id,
                    trigger_id=trigger.trigger_id,
                    priority=job.priority
                )

        except Exception as e:
            self.telemetry.error("Failed to process event for reforecast", error=str(e))

    async def _job_processor_loop(self) -> None:
        """Background task to process reforecast jobs."""
        while not self._shutdown_event.is_set():
            try:
                # Find highest priority pending job
                if not self.pending_jobs:
                    await asyncio.sleep(1.0)
                    continue

                # Sort jobs by priority and creation time
                sorted_jobs = sorted(
                    self.pending_jobs.values(),
                    key=lambda j: (j.priority, j.created_at),
                    reverse=True
                )

                job = sorted_jobs[0]

                # Check rate limiting
                if not self._can_process_job(job):
                    await asyncio.sleep(0.1)
                    continue

                # Process job
                async with self.processing_semaphore:
                    await self._execute_reforecast_job(job)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.error("Job processor loop failed", error=str(e))

    def _can_process_job(self, job: ReforcastJob) -> bool:
        """Check if job can be processed based on rate limiting."""
        if not self.backpressure_config.enabled:
            return True

        if job.scheduled_for and job.scheduled_for > datetime.utcnow():
            return False

        # Check processing rate
        rate_key = f"processing_rate:{job.forecast_request.forecast_type.value}"
        last_processed = self.processing_rate_limiter.get(rate_key)

        if last_processed:
            time_since_last = (datetime.utcnow() - last_processed).total_seconds()
            min_interval = 1.0 / self.backpressure_config.max_processing_rate_per_second

            if time_since_last < min_interval:
                return False

        return True

    async def _execute_reforecast_job(self, job: ReforcastJob) -> None:
        """Execute a reforecast job."""
        try:
            self.pending_jobs.pop(job.job_id, None)
            self.processing_jobs[job.job_id] = job

            job.status = "processing"
            job.attempts += 1
            job.error_message = None

            # Persist status transition and audit
            now = datetime.utcnow()
            await self._maybe_update_job_status(
                job,
                status="processing",
                started_at=now,
                attempts=job.attempts
            )
            self._record_job_audit(job, status="processing")

            self.telemetry.info(
                "Executing reforecast job",
                job_id=job.job_id,
                forecast_type=job.forecast_request.forecast_type.value,
                attempt=job.attempts
            )

            # Update rate limiter
            rate_key = f"processing_rate:{job.forecast_request.forecast_type.value}"
            self.processing_rate_limiter[rate_key] = datetime.utcnow()

            # Generate new forecast
            forecast_service = get_feature_store_service()

            # Create forecast request with updated parameters
            forecast_request = job.forecast_request.copy()
            forecast_request.start_date = datetime.utcnow()
            forecast_request.end_date = forecast_request.start_date + timedelta(hours=24)

            # Generate features and forecast
            features, _ = await forecast_service.get_features_for_modeling(
                start_date=forecast_request.start_date,
                end_date=forecast_request.end_date,
                geography=forecast_request.geography,
                target_variable=forecast_request.target_variable,
                scenario_id=job.trigger_event.metadata.get("scenario_id") if job.trigger_event.metadata else None
            )

            # In real implementation, would use ML model to generate forecast
            await asyncio.sleep(0.1)

            # Cache the forecast
            cache_manager = get_unified_cache_manager()
            cache_key = f"forecast:{job.job_id}"
            await cache_manager.set(
                cache_key,
                {
                    "forecast_id": job.job_id,
                    "trigger_event_id": job.trigger_event.event_id,
                    "generated_at": datetime.utcnow().isoformat(),
                    "forecast_type": forecast_request.forecast_type.value,
                    "feature_count": len(features)
                },
                ttl_seconds=3600
            )

            # Invalidate related forecasts
            await self._invalidate_related_forecasts(job.forecast_request)

            job.status = "completed"

            completion_time = datetime.utcnow()
            await self._maybe_update_job_status(
                job,
                status="completed",
                completed_at=completion_time,
                attempts=job.attempts
            )
            self._record_job_audit(job, status="completed")

            self.telemetry.info(
                "Reforecast job completed successfully",
                job_id=job.job_id,
                forecast_type=forecast_request.forecast_type.value
            )

            # Record metrics
            self.telemetry.increment_counter("reforecasts_completed", category=MetricCategory.BUSINESS)
            self.telemetry.record_histogram(
                "reforecast_processing_time",
                (completion_time - job.created_at).total_seconds(),
                category=MetricCategory.PERFORMANCE
            )

            self._release_job_from_trigger(job)
            self.completed_jobs[job.job_id] = job

        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)

            failure_time = datetime.utcnow()
            await self._maybe_update_job_status(
                job,
                status="failed",
                completed_at=failure_time,
                attempts=job.attempts,
                error_message=str(e)
            )
            self._record_job_audit(job, status="failed", detail=str(e))

            self.telemetry.error(
                "Reforecast job failed",
                job_id=job.job_id,
                error=str(e),
                attempt=job.attempts
            )

            if job.attempts < job.max_attempts:
                delay = self._compute_backoff_delay(job.attempts)
                job.scheduled_for = datetime.utcnow() + delay
                job.status = "pending"
                self.pending_jobs[job.job_id] = job
                self.completed_jobs.pop(job.job_id, None)
                self._record_job_audit(
                    job,
                    status="retry_scheduled",
                    detail=f"in_{int(delay.total_seconds())}s"
                )
            else:
                self._release_job_from_trigger(job)
                self.completed_jobs[job.job_id] = job

                self.telemetry.increment_counter(
                    "reforecasts_failed",
                    category=MetricCategory.RELIABILITY
                )

        finally:
            self.processing_jobs.pop(job.job_id, None)

    async def _invalidate_related_forecasts(self, forecast_request: ForecastRequest) -> None:
        """Invalidate related forecasts that may be affected by new data."""
        cache_manager = get_unified_cache_manager()

        # Invalidate forecasts for the same geography and forecast type
        pattern = f"forecast:*:{forecast_request.geography}:{forecast_request.forecast_type.value}"
        invalidated_count = await cache_manager.invalidate_pattern(pattern)

        if invalidated_count > 0:
            self.telemetry.info(
                "Invalidated related forecasts",
                pattern=pattern,
                count=invalidated_count
            )

    async def _cleanup_loop(self) -> None:
        """Background task to clean up old data."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)  # Clean up every 5 minutes

                # Clean up old completed jobs
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                old_jobs = [
                    job_id for job_id, job in self.completed_jobs.items()
                    if job.created_at < cutoff_time
                ]

                for job_id in old_jobs:
                    self.completed_jobs.pop(job_id)

                # Clean up old trigger timestamps
                cutoff_time = datetime.utcnow() - timedelta(hours=1)
                for key, timestamps in self.trigger_timestamps.items():
                    self.trigger_timestamps[key] = deque([
                        ts for ts in timestamps if ts > cutoff_time
                    ])

                # Clean up rate limiter timestamps
                cutoff_time = datetime.utcnow() - timedelta(minutes=1)
                self.processing_rate_limiter = {
                    key: ts for key, ts in self.processing_rate_limiter.items()
                    if ts > cutoff_time
                }

                # Clean up processed event ids outside idempotency window
                cutoff_time = datetime.utcnow() - self._processed_event_window
                self._processed_event_ids = {
                    event_id: ts
                    for event_id, ts in self._processed_event_ids.items()
                    if ts > cutoff_time
                }

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.telemetry.error("Cleanup loop failed", error=str(e))

    def add_trigger(self, trigger: ForecastTrigger) -> None:
        """Add a forecast trigger.

        Args:
            trigger: Forecast trigger configuration
        """
        self.triggers[trigger.trigger_id] = trigger

        if trigger.enabled:
            self.active_triggers.add(trigger.trigger_id)

        self.telemetry.info(
            "Added forecast trigger",
            trigger_id=trigger.trigger_id,
            name=trigger.name,
            priority=trigger.priority
        )

    def remove_trigger(self, trigger_id: str) -> bool:
        """Remove a forecast trigger.

        Args:
            trigger_id: Trigger identifier

        Returns:
            True if trigger was removed
        """
        if trigger_id in self.triggers:
            trigger = self.triggers.pop(trigger_id)
            self.active_triggers.discard(trigger_id)

            self.telemetry.info("Removed forecast trigger", trigger_id=trigger_id)
            return True

        return False

    def enable_trigger(self, trigger_id: str) -> bool:
        """Enable a forecast trigger.

        Args:
            trigger_id: Trigger identifier

        Returns:
            True if trigger was enabled
        """
        if trigger_id in self.triggers:
            self.triggers[trigger_id].enabled = True
            self.active_triggers.add(trigger_id)

            self.telemetry.info("Enabled forecast trigger", trigger_id=trigger_id)
            return True

        return False

    def disable_trigger(self, trigger_id: str) -> bool:
        """Disable a forecast trigger.

        Args:
            trigger_id: Trigger identifier

        Returns:
            True if trigger was disabled
        """
        if trigger_id in self.triggers:
            self.triggers[trigger_id].enabled = False
            self.active_triggers.discard(trigger_id)

            self.telemetry.info("Disabled forecast trigger", trigger_id=trigger_id)
            return True

        return False

    def get_trigger_stats(self) -> Dict[str, Any]:
        """Get trigger statistics.

        Returns:
            Trigger statistics
        """
        trigger_stats = {}
        for trigger_id, trigger in self.triggers.items():
            trigger_stats[trigger_id] = {
                "name": trigger.name,
                "enabled": trigger.enabled,
                "priority": trigger.priority,
                "trigger_count": trigger.trigger_count,
                "last_triggered": trigger.last_triggered.isoformat() if trigger.last_triggered else None,
                "conditions": len(trigger.conditions)
            }

        return {
            "total_triggers": len(self.triggers),
            "active_triggers": len(self.active_triggers),
            "pending_jobs": len(self.pending_jobs),
            "processing_jobs": len(self.processing_jobs),
            "completed_jobs": len(self.completed_jobs),
            "queue_size": self.event_queue.qsize(),
            "trigger_details": trigger_stats
        }

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health information.

        Returns:
            Health information
        """
        return {
            "service": "auto_reforecast",
            "status": "healthy" if not self._shutdown_event.is_set() else "stopping",
            "kafka_consumer": self.kafka_consumer is not None,
            "event_queue_size": self.event_queue.qsize(),
            "max_queue_size": self.backpressure_config.max_queue_size,
            "pending_jobs": len(self.pending_jobs),
            "processing_jobs": len(self.processing_jobs),
            "debounce_enabled": self.debounce_config.enabled,
            "backpressure_enabled": self.backpressure_config.enabled
        }

    # API Methods for trigger management
    async def list_triggers(
        self,
        enabled_only: bool = False,
        limit: int = 50,
        offset: int = 0
    ) -> List[ForecastTrigger]:
        """List forecast triggers with optional filtering and pagination."""

        triggers = list(self.triggers.values())

        if enabled_only:
            triggers = [trigger for trigger in triggers if trigger.enabled]

        return triggers[offset:offset + limit]

    async def get_trigger(self, trigger_id: str) -> Optional[ForecastTrigger]:
        """Get a specific forecast trigger by identifier."""

        return self.triggers.get(trigger_id)

    async def create_trigger(self, trigger: ForecastTrigger) -> ForecastTrigger:
        """Register a new forecast trigger."""

        self.add_trigger(trigger)
        return trigger

    async def update_trigger(self, trigger: ForecastTrigger) -> ForecastTrigger:
        """Update an existing forecast trigger."""

        if trigger.trigger_id not in self.triggers:
            raise ValueError(f"Trigger {trigger.trigger_id} not found")

        current_trigger = self.triggers[trigger.trigger_id]

        if current_trigger.enabled and not trigger.enabled:
            self.active_triggers.discard(trigger.trigger_id)
        elif not current_trigger.enabled and trigger.enabled:
            self.active_triggers.add(trigger.trigger_id)

        self.triggers[trigger.trigger_id] = trigger

        self.telemetry.info(
            "Trigger updated",
            trigger_id=trigger.trigger_id,
            trigger_name=trigger.name
        )

        return trigger

    async def delete_trigger(self, trigger_id: str) -> bool:
        """Delete a forecast trigger by identifier."""

        return self.remove_trigger(trigger_id)

    async def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[ReforcastJob]:
        """List reforecast jobs with optional status filtering."""

        job_index: Dict[str, ReforcastJob] = {}

        def maybe_add(job: Optional[ReforcastJob]) -> None:
            if not job:
                return

            if status is not None and job.status != status:
                return

            job_index[job.job_id] = job

        for job in self.pending_jobs.values():
            maybe_add(job)

        for job in self.processing_jobs.values():
            maybe_add(job)

        for job in self.completed_jobs.values():
            maybe_add(job)

        jobs = sorted(job_index.values(), key=lambda job: job.created_at, reverse=True)

        return jobs[offset:offset + limit]

    async def list_trigger_events(
        self,
        trigger_id: Optional[str] = None,
        limit: int = 50,
        since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Return recent trigger event audit log entries."""

        results: List[Dict[str, Any]] = []

        for record in reversed(self._trigger_event_history):
            if trigger_id and record.get("trigger_id") != trigger_id:
                continue

            recorded_at: datetime = record.get("recorded_at", datetime.utcnow())
            if since and recorded_at < since:
                continue

            event_timestamp = record.get("event_timestamp")

            results.append(
                {
                    "event_id": record.get("event_id"),
                    "trigger_id": record.get("trigger_id"),
                    "trigger_name": record.get("trigger_name"),
                    "data_source": record.get("data_source"),
                    "geography": record.get("geography"),
                    "priority": record.get("priority"),
                    "status": record.get("status"),
                    "reason": record.get("reason"),
                    "event_timestamp": event_timestamp.isoformat() if isinstance(event_timestamp, datetime) else event_timestamp,
                    "recorded_at": recorded_at.isoformat(),
                }
            )

            if len(results) >= limit:
                break

        return results

    async def get_job_audit(
        self,
        job_id: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Return job lifecycle audit information."""

        def serialize(entry: Dict[str, Any]) -> Dict[str, Any]:
            timestamp = entry.get("timestamp")
            return {
                "job_id": entry.get("job_id"),
                "trigger_id": entry.get("trigger_id"),
                "status": entry.get("status"),
                "detail": entry.get("detail"),
                "attempt": entry.get("attempt"),
                "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
            }

        if job_id:
            entries = self._job_audit_log.get(job_id, [])
            return [serialize(entry) for entry in entries[-limit:]]

        aggregated: List[Dict[str, Any]] = []
        for current_job_id, entries in self._job_audit_log.items():
            for entry in entries:
                if job_id and current_job_id != job_id:
                    continue
                aggregated.append(entry)

        aggregated.sort(key=lambda entry: entry.get("timestamp", datetime.utcnow()), reverse=True)

        return [serialize(entry) for entry in aggregated[:limit]]

    async def get_debounce_config(self) -> DebounceConfig:
        """Return the current debounce configuration."""

        return self.debounce_config

    async def update_debounce_config(self, config: DebounceConfig) -> DebounceConfig:
        """Update debounce configuration and adjust processing limits."""

        previous_config = self.debounce_config
        self.debounce_config = config

        if config.max_concurrent_triggers != previous_config.max_concurrent_triggers:
            self.processing_semaphore = asyncio.Semaphore(config.max_concurrent_triggers)

        old_config_data = (
            previous_config.model_dump()
            if hasattr(previous_config, "model_dump")
            else previous_config.dict()
        )
        new_config_data = (
            config.model_dump()
            if hasattr(config, "model_dump")
            else config.dict()
        )

        self.telemetry.info(
            "Debounce config updated",
            old_config=old_config_data,
            new_config=new_config_data
        )

        return self.debounce_config

    # Persistence wiring
    def set_job_repository(self, job_repository, tenant_resolver: Optional[Callable[[TriggerEvent], Optional[str]]] = None) -> None:
        """Configure job repository and tenant resolver for persistence."""
        self._job_repository = job_repository
        if tenant_resolver is not None:
            self._tenant_resolver = tenant_resolver
        elif self._tenant_resolver is None:
            # Default resolver: look for 'tenant_id' in event metadata
            def _default_resolver(event: TriggerEvent) -> Optional[str]:
                tenant = event.metadata.get("tenant_id") if event.metadata else None
                return str(tenant) if tenant else None
            self._tenant_resolver = _default_resolver

    async def _maybe_persist_job(self, job: ReforcastJob, event: TriggerEvent) -> None:
        repo = getattr(self, "_job_repository", None)
        resolver = self._tenant_resolver
        if not repo or not resolver:
            return
        try:
            tenant_id = resolver(event)
            if not tenant_id:
                return
            await repo.enqueue_job(UUID(tenant_id), job)  # type: ignore[arg-type]
        except Exception as e:
            self.telemetry.warning(
                "job_persistence_enqueue_failed",
                job_id=job.job_id,
                error=str(e)
            )

    async def _maybe_update_job_status(
        self,
        job: ReforcastJob,
        *,
        status: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        attempts: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        repo = getattr(self, "_job_repository", None)
        resolver = self._tenant_resolver
        if not repo or not resolver:
            return
        try:
            tenant_id = resolver(job.trigger_event)
            if not tenant_id:
                return
            await repo.update_status(
                UUID(tenant_id),
                UUID(job.job_id),
                status=status,
                started_at=started_at,
                completed_at=completed_at,
                attempts=attempts,
                error_message=error_message,
            )
        except Exception as e:
            self.telemetry.warning(
                "job_persistence_update_failed",
                job_id=job.job_id,
                error=str(e)
            )


# Global auto-reforecast service instance
_auto_reforecast_service: Optional[AutoReforecastService] = None


def get_auto_reforecast_service(
    debounce_config: Optional[DebounceConfig] = None,
    backpressure_config: Optional[BackpressureConfig] = None,
    kafka_config: Optional[Dict[str, Any]] = None
) -> AutoReforecastService:
    """Get the global auto-reforecast service instance.

    Args:
        debounce_config: Optional debounce configuration
        backpressure_config: Optional backpressure configuration
        kafka_config: Optional Kafka configuration

    Returns:
        Auto-reforecast service instance
    """
    global _auto_reforecast_service

    if _auto_reforecast_service is None:
        _auto_reforecast_service = AutoReforecastService(
            debounce_config=debounce_config,
            backpressure_config=backpressure_config,
            kafka_config=kafka_config
        )

    return _auto_reforecast_service


# Convenience functions for common operations
async def trigger_forecast_rerun(
    data_source: str,
    geography: str = "US",
    forecast_type: ForecastType = ForecastType.LOAD,
    target_variable: str = "load_mw",
    trigger_reason: str = "data_update"
) -> Optional[str]:
    """Manually trigger a forecast re-run.

    Args:
        data_source: Source of the data change
        geography: Geographic scope
        forecast_type: Type of forecast to trigger
        target_variable: Variable to forecast
        trigger_reason: Reason for trigger

    Returns:
        Job ID if trigger was successful
    """
    service = get_auto_reforecast_service()

    # Create trigger event
    event = TriggerEvent(
        event_id=str(uuid4()),
        trigger_id="manual_trigger",
        data_source=data_source,
        geography=geography,
        timestamp=datetime.utcnow(),
        data_changes={"manual_trigger": 1.0},
        priority_score=1.0,
        metadata={"trigger_reason": trigger_reason}
    )

    # Process event
    await service._process_trigger_event(event)

    # Return first pending job ID
    if service.pending_jobs:
        return next(iter(service.pending_jobs.keys()))

    return None


def create_weather_trigger(
    geography: str = "US",
    threshold_percentage: float = 5.0,
    forecast_horizon_hours: int = 24
) -> ForecastTrigger:
    """Create a trigger for weather data changes.

    Args:
        geography: Geographic scope
        threshold_percentage: Percentage change threshold
        forecast_horizon_hours: Forecast horizon in hours

    Returns:
        Forecast trigger configuration
    """
    tracked_fields = ["temperature", "humidity", "wind_speed", "precipitation"]

    conditions = [
        TriggerCondition(
            data_source="weather",
            geography=geography,
            threshold_type="percentage",
            threshold_value=threshold_percentage / 100.0,
            fields=tracked_fields,
            metadata={
                "fields": tracked_fields,
                "unit": "percentage",
                "description": "Weather variance trigger"
            }
        )
    ]

    start_time = datetime.utcnow()
    forecast_config = ForecastRequest(
        forecast_type=ForecastType.LOAD,
        target_variable="load_mw",
        geography=geography,
        start_date=start_time,
        end_date=start_time + timedelta(hours=forecast_horizon_hours),
        quantiles=[QuantileLevel.P10, QuantileLevel.P50, QuantileLevel.P90],
        interval=ForecastInterval.HOURLY
    )

    return ForecastTrigger(
        trigger_id=str(uuid4()),
        name=f"Weather trigger - {geography}",
        description=f"Trigger forecast when weather changes by {threshold_percentage}% in {geography}",
        conditions=conditions,
        forecast_config=forecast_config,
        priority=0.8
    )


def create_price_trigger(
    geography: str = "US",
    threshold_absolute: float = 10.0,
    forecast_horizon_hours: int = 48
) -> ForecastTrigger:
    """Create a trigger for price data changes.

    Args:
        geography: Geographic scope
        threshold_absolute: Absolute price change threshold
        forecast_horizon_hours: Forecast horizon in hours

    Returns:
        Forecast trigger configuration
    """
    tracked_fields = ["lmp_price", "marginal_cost"]

    conditions = [
        TriggerCondition(
            data_source="price",
            geography=geography,
            threshold_type="absolute",
            threshold_value=threshold_absolute,
            fields=tracked_fields,
            metadata={
                "fields": tracked_fields,
                "unit": "USD",
                "description": "Price shock trigger"
            }
        )
    ]

    start_time = datetime.utcnow()
    forecast_config = ForecastRequest(
        forecast_type=ForecastType.PRICE,
        target_variable="lmp_price",
        geography=geography,
        start_date=start_time,
        end_date=start_time + timedelta(hours=forecast_horizon_hours),
        quantiles=[QuantileLevel.P10, QuantileLevel.P50, QuantileLevel.P90],
        interval=ForecastInterval.HOURLY
    )

    return ForecastTrigger(
        trigger_id=str(uuid4()),
        name=f"Price trigger - {geography}",
        description=f"Trigger forecast when price changes by ${threshold_absolute} in {geography}",
        conditions=conditions,
        forecast_config=forecast_config,
        priority=0.9
    )
