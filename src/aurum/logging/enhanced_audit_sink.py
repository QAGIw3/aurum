"""Enhanced audit sink with buffering, batching, backoff, connection pooling, and circuit breakers."""

from __future__ import annotations

import asyncio
import json
import time
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable
from collections import deque

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger
from ..common.circuit_breaker import CircuitBreakerConfig, get_circuit_breaker
from ..common.queue_monitor import QueueConfig, get_queue_monitor


@dataclass
class EnhancedAuditSinkConfig:
    """Configuration for enhanced audit sink."""
    name: str = "default"
    max_buffer_size: int = 1000  # Maximum events to buffer
    batch_size: int = 50  # Batch size for sending
    flush_interval_seconds: float = 5.0  # Flush interval when batch not full
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0
    connection_pool_size: int = 5
    queue_warning_threshold: float = 0.8
    queue_critical_threshold: float = 0.95
    alert_callback: Optional[Callable] = None


class EnhancedAuditSink:
    """Enhanced audit sink with comprehensive reliability features."""

    def __init__(self, config: EnhancedAuditSinkConfig):
        self.config = config
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

        # Buffering and batching
        self._buffer: deque = deque(maxlen=config.max_buffer_size)
        self._buffer_lock = threading.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._flush_event = asyncio.Event()

        # State
        self._running = False
        self._total_sent = 0
        self._total_failed = 0
        self._last_flush_time = time.time()

        # Circuit breaker and queue monitoring
        self._circuit_breaker = get_circuit_breaker(
            f"audit_sink_{config.name}",
            CircuitBreakerConfig(
                failure_threshold=config.circuit_breaker_threshold,
                recovery_timeout=config.circuit_breaker_timeout,
                alert_callback=config.alert_callback,
                name=f"audit_sink_{config.name}"
            )
        )

        self._queue_monitor = None  # Will be set by subclass

    async def initialize(self):
        """Initialize the enhanced audit sink."""
        self._running = True
        self._flush_task = asyncio.create_task(self._flush_worker())
        await self._initialize_subclass()

    async def close(self):
        """Close the enhanced audit sink."""
        self._running = False

        if self._flush_task:
            self._flush_event.set()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self._flush(force=True)

        await self._close_subclass()

    async def emit(self, event: Dict[str, Any]):
        """Emit an audit event."""
        if not self._running:
            return

        # Check circuit breaker
        if self._circuit_breaker.is_open():
            self._total_failed += 1
            self._update_metrics()
            return

        # Check queue capacity
        if self._queue_monitor and self._queue_monitor.should_reject():
            self._total_failed += 1
            self._update_metrics()
            return

        # Add to buffer
        with self._buffer_lock:
            self._buffer.append(event)

        self._update_metrics()

        # Trigger flush if buffer is full
        if len(self._buffer) >= self.config.batch_size:
            self._flush_event.set()

    async def _flush_worker(self):
        """Background worker to flush events."""
        while self._running:
            try:
                # Wait for flush signal or timeout
                try:
                    await asyncio.wait_for(
                        self._flush_event.wait(),
                        timeout=self.config.flush_interval_seconds
                    )
                except asyncio.TimeoutError:
                    # Timeout - flush anyway
                    pass

                if not self._running:
                    break

                # Flush events
                await self._flush()

                # Clear flush event
                self._flush_event.clear()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in flush worker: {e}")
                await asyncio.sleep(1.0)

    async def _flush(self, force: bool = False):
        """Flush buffered events."""
        events_to_send = []

        # Get events from buffer
        with self._buffer_lock:
            if not self._buffer:
                return

            if force:
                # Send all events
                events_to_send = list(self._buffer)
                self._buffer.clear()
            else:
                # Send up to batch size
                batch_size = min(self.config.batch_size, len(self._buffer))
                events_to_send = [self._buffer.popleft() for _ in range(batch_size)]

        if not events_to_send:
            return

        # Attempt to send
        success = await self._send_batch(events_to_send)

        if success:
            self._total_sent += len(events_to_send)
            self._circuit_breaker.record_success()
        else:
            self._total_failed += len(events_to_send)
            self._circuit_breaker.record_failure()

            # Put events back in buffer (at front for retry)
            with self._buffer_lock:
                for event in reversed(events_to_send):
                    if len(self._buffer) < self.config.max_buffer_size:
                        self._buffer.appendleft(event)
                    else:
                        # Buffer full, drop oldest events
                        self._buffer.pop()
                        self._buffer.appendleft(event)

        self._last_flush_time = time.time()
        self._update_metrics()

    async def _send_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Send a batch of events. Override in subclass."""
        raise NotImplementedError

    async def _initialize_subclass(self):
        """Initialize subclass-specific components."""
        pass

    async def _close_subclass(self):
        """Close subclass-specific components."""
        pass

    def _update_metrics(self):
        """Update Prometheus metrics."""
        buffer_size = len(self._buffer)
        self.metrics.gauge(f"audit_sink_buffer_size_{self.config.name}", buffer_size)
        self.metrics.gauge(f"audit_sink_total_sent_{self.config.name}", self._total_sent)
        self.metrics.gauge(f"audit_sink_total_failed_{self.config.name}", self._total_failed)
        self.metrics.histogram(f"audit_sink_batch_size_{self.config.name}", len(self._buffer))
        self.metrics.gauge(f"audit_sink_last_flush_seconds_{self.config.name}",
                          time.time() - self._last_flush_time)

    def get_stats(self) -> Dict[str, Any]:
        """Get audit sink statistics."""
        return {
            "name": self.config.name,
            "buffer_size": len(self._buffer),
            "total_sent": self._total_sent,
            "total_failed": self._total_failed,
            "circuit_breaker_state": self._circuit_breaker.get_state(),
            "last_flush_time": self._last_flush_time,
            "running": self._running
        }


class EnhancedKafkaAuditSink(EnhancedAuditSink):
    """Enhanced Kafka audit sink with buffering and reliability."""

    def __init__(self, config: EnhancedAuditSinkConfig, kafka_config: Dict[str, Any]):
        super().__init__(config)
        self.kafka_config = kafka_config
        self._producer = None
        self._producer_lock = threading.Lock()

    async def _initialize_subclass(self):
        """Initialize Kafka producer."""
        try:
            from confluent_kafka import Producer

            producer_config = {
                "bootstrap.servers": self.kafka_config.get("bootstrap_servers", "localhost:9092"),
                "client.id": f"enhanced-audit-sink-{self.config.name}",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "acks": "all",
                "batch.size": 65536,
                "linger.ms": 200,
                "retry.backoff.ms": 100,
                "retry.backoff.max.ms": 5000,
                "retries": 10,
                "buffer.memory": 134217728,
                **self.kafka_config.get("extra_config", {})
            }

            self._producer = Producer(producer_config)
            self.logger.info(f"Enhanced Kafka audit sink initialized for {self.config.name}")

        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    async def _close_subclass(self):
        """Close Kafka producer."""
        if self._producer:
            try:
                self._producer.flush(30)  # 30 second timeout
            except Exception as e:
                self.logger.error(f"Error flushing Kafka producer: {e}")
            finally:
                self._producer = None

    async def _send_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Send batch to Kafka."""
        if not self._producer:
            return False

        topic = self.kafka_config.get("topic", "aurum.audit")

        try:
            with self._producer_lock:
                for event in events:
                    message = json.dumps(event, separators=(",", ":")).encode("utf-8")
                    self._producer.produce(topic, message)

                # Wait for delivery
                remaining = self._producer.flush(10)  # 10 second timeout

                if remaining > 0:
                    self.logger.warning(f"Kafka flush timeout: {remaining} messages pending")
                    return False

                return True

        except Exception as e:
            self.logger.error(f"Error sending batch to Kafka: {e}")
            return False


class EnhancedClickHouseAuditSink(EnhancedAuditSink):
    """Enhanced ClickHouse audit sink with buffering and reliability."""

    def __init__(self, config: EnhancedAuditSinkConfig, clickhouse_config: Dict[str, Any]):
        super().__init__(config)
        self.clickhouse_config = clickhouse_config
        self._session_pool: List[Any] = []
        self._session_pool_lock = threading.Lock()

    async def _initialize_subclass(self):
        """Initialize ClickHouse connection pool."""
        try:
            import aiohttp

            # Create connection pool
            connector = aiohttp.TCPConnector(
                limit=self.config.connection_pool_size,
                limit_per_host=self.config.connection_pool_size,
                keepalive_timeout=60
            )

            # Create sessions
            for _ in range(self.config.connection_pool_size):
                session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers={"Content-Type": "application/json"}
                )

                if (self.clickhouse_config.get("username") and
                    self.clickhouse_config.get("password")):
                    auth = aiohttp.BasicAuth(
                        self.clickhouse_config["username"],
                        self.clickhouse_config["password"]
                    )
                    session._default_auth = auth

                self._session_pool.append(session)

            self.logger.info(f"Enhanced ClickHouse audit sink initialized for {self.config.name}")

        except Exception as e:
            self.logger.error(f"Failed to initialize ClickHouse sessions: {e}")
            raise

    async def _close_subclass(self):
        """Close ClickHouse sessions."""
        async with self._session_pool_lock:
            for session in self._session_pool:
                await session.close()
            self._session_pool.clear()

    async def _send_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Send batch to ClickHouse."""
        if not self._session_pool:
            return False

        # Get a session from the pool
        session = None
        async with self._session_pool_lock:
            if self._session_pool:
                session = self._session_pool.pop()

        if not session:
            return False

        try:
            # Convert events to JSON Lines format
            json_lines = "\n".join([
                json.dumps(event, separators=(",", ":"))
                for event in events
            ])

            url = f"{self.clickhouse_config['endpoint']}/?query=INSERT INTO {self.clickhouse_config['table']} FORMAT JSONEachRow"

            async with session.post(
                url,
                data=json_lines,
                headers={"Content-Type": "application/x-ndjson"}
            ) as response:
                if response.status == 200:
                    return True
                else:
                    error_text = await response.text()
                    self.logger.error(f"ClickHouse insert failed: {response.status} - {error_text}")
                    return False

        except Exception as e:
            self.logger.error(f"Error sending batch to ClickHouse: {e}")
            return False
        finally:
            # Return session to pool
            async with self._session_pool_lock:
                if session and len(self._session_pool) < self.config.connection_pool_size:
                    self._session_pool.append(session)


class EnhancedFileAuditSink(EnhancedAuditSink):
    """Enhanced file audit sink with buffering and reliability."""

    def __init__(self, config: EnhancedAuditSinkConfig, file_config: Dict[str, Any]):
        super().__init__(config)
        self.file_config = file_config
        self._file_handle = None
        self._file_lock = threading.Lock()

    async def _initialize_subclass(self):
        """Initialize file handle."""
        try:
            log_dir = self.file_config.get("log_dir", "/var/log/aurum")
            import os
            os.makedirs(log_dir, exist_ok=True)

            log_file = f"{log_dir}/audit_{self.config.name}_{int(time.time())}.log"

            self._file_handle = open(log_file, "a", encoding="utf-8")
            self.logger.info(f"Enhanced file audit sink initialized for {self.config.name}")

        except Exception as e:
            self.logger.error(f"Failed to initialize file handle: {e}")
            raise

    async def _close_subclass(self):
        """Close file handle."""
        if self._file_handle:
            with self._file_lock:
                self._file_handle.close()
                self._file_handle = None

    async def _send_batch(self, events: List[Dict[str, Any]]) -> bool:
        """Send batch to file."""
        if not self._file_handle:
            return False

        try:
            with self._file_lock:
                for event in events:
                    json_line = json.dumps(event, separators=(",", ":")) + "\n"
                    self._file_handle.write(json_line)
                    self._file_handle.flush()

                return True

        except Exception as e:
            self.logger.error(f"Error writing batch to file: {e}")
            return False


# Factory function to create enhanced audit sinks
async def create_enhanced_audit_sink(
    sink_type: str,
    config: EnhancedAuditSinkConfig,
    backend_config: Dict[str, Any]
) -> EnhancedAuditSink:
    """Create an enhanced audit sink based on type."""
    if sink_type == "kafka":
        return EnhancedKafkaAuditSink(config, backend_config)
    elif sink_type == "clickhouse":
        return EnhancedClickHouseAuditSink(config, backend_config)
    elif sink_type == "file":
        return EnhancedFileAuditSink(config, backend_config)
    else:
        raise ValueError(f"Unsupported audit sink type: {sink_type}")


__all__ = [
    "EnhancedAuditSinkConfig",
    "EnhancedAuditSink",
    "EnhancedKafkaAuditSink",
    "EnhancedClickHouseAuditSink",
    "EnhancedFileAuditSink",
    "create_enhanced_audit_sink",
]
