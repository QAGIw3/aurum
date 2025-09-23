"""Queue depth monitoring and backpressure management for reliable systems."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Callable

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger


@dataclass
class QueueConfig:
    """Configuration for queue monitoring."""
    max_depth: int = 1000
    warning_threshold: float = 0.7  # 70% of max depth
    critical_threshold: float = 0.9  # 90% of max depth
    alert_callback: Optional[Callable] = None
    name: str = "default"
    monitoring_interval_seconds: int = 10


@dataclass
class QueueStats:
    """Queue statistics."""
    current_depth: int = 0
    max_depth: int = 0
    min_depth: int = 0
    total_enqueued: int = 0
    total_dequeued: int = 0
    total_rejected: int = 0
    total_processed: int = 0
    average_processing_time: float = 0.0
    last_updated: float = 0.0


class QueueMonitor:
    """Monitor queue depth and provide backpressure signals."""

    def __init__(self, config: QueueConfig):
        self.config = config
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

        self.stats = QueueStats()
        self._processing_times: list[float] = []
        self._alert_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Initialize the queue monitor."""
        # Start background monitoring task
        self._alert_task = asyncio.create_task(self._monitor_thresholds())

    async def close(self):
        """Close the queue monitor."""
        if self._alert_task:
            self._alert_task.cancel()
            try:
                await self._alert_task
            except asyncio.CancelledError:
                pass

    async def enqueue(self, item: Any) -> bool:
        """Attempt to enqueue an item."""
        if self.stats.current_depth >= self.config.max_depth:
            self.stats.total_rejected += 1
            self._update_metrics()
            return False

        self.stats.current_depth += 1
        self.stats.total_enqueued += 1
        self.stats.max_depth = max(self.stats.max_depth, self.stats.current_depth)
        self._update_metrics()

        # Check thresholds and alert if needed
        await self._check_thresholds()

        return True

    async def dequeue(self) -> Optional[Any]:
        """Dequeue an item for processing."""
        if self.stats.current_depth <= 0:
            return None

        self.stats.current_depth -= 1
        self.stats.total_dequeued += 1
        self._update_metrics()

        return None  # Actual item would be returned by implementation

    async def start_processing(self) -> float:
        """Mark an item as being processed."""
        return time.time()

    async def finish_processing(self, start_time: float):
        """Mark processing as complete and update stats."""
        processing_time = time.time() - start_time
        self.stats.total_processed += 1

        # Update processing time statistics
        self._processing_times.append(processing_time)
        if len(self._processing_times) > 100:  # Keep last 100 times
            self._processing_times.pop(0)

        self.stats.average_processing_time = sum(self._processing_times) / len(self._processing_times)

        self._update_metrics()

    def get_backpressure_signal(self) -> float:
        """Get backpressure signal (0.0 to 1.0, where 1.0 = maximum backpressure)."""
        utilization = self.stats.current_depth / self.config.max_depth
        return min(1.0, utilization)

    def should_throttle(self) -> bool:
        """Check if requests should be throttled due to high queue depth."""
        return self.stats.current_depth >= (self.config.max_depth * self.config.warning_threshold)

    def should_reject(self) -> bool:
        """Check if requests should be rejected due to critical queue depth."""
        return self.stats.current_depth >= (self.config.max_depth * self.config.critical_threshold)

    async def _check_thresholds(self):
        """Check if thresholds are exceeded and trigger alerts."""
        utilization = self.stats.current_depth / self.config.max_depth

        if utilization >= self.config.critical_threshold:
            await self._trigger_alert(
                "critical",
                f"Queue depth critical: {self.stats.current_depth}/{self.config.max_depth} "
                f"({utilization".1%"})"
            )
        elif utilization >= self.config.warning_threshold:
            await self._trigger_alert(
                "warning",
                f"Queue depth high: {self.stats.current_depth}/{self.config.max_depth} "
                f"({utilization".1%"})"
            )

    async def _trigger_alert(self, level: str, message: str):
        """Trigger an alert for queue issues."""
        if self.config.alert_callback:
            try:
                await self.config.alert_callback({
                    "type": "queue_alert",
                    "queue": self.config.name,
                    "level": level,
                    "message": message,
                    "stats": {
                        "current_depth": self.stats.current_depth,
                        "max_depth": self.config.max_depth,
                        "utilization": self.stats.current_depth / self.config.max_depth,
                        "total_enqueued": self.stats.total_enqueued,
                        "total_processed": self.stats.total_processed,
                        "average_processing_time": self.stats.average_processing_time
                    },
                    "timestamp": int(time.time() * 1000)
                })
            except Exception as e:
                self.logger.error(f"Failed to send queue alert: {e}")

        if level == "critical":
            self.logger.error(f"Queue alert: {message}")
        else:
            self.logger.warning(f"Queue alert: {message}")

    async def _monitor_thresholds(self):
        """Background task to monitor queue thresholds."""
        while True:
            try:
                await asyncio.sleep(self.config.monitoring_interval_seconds)
                await self._check_thresholds()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error monitoring queue thresholds: {e}")

    def _update_metrics(self):
        """Update Prometheus metrics."""
        self.stats.last_updated = time.time()

        self.metrics.gauge(f"queue_depth_{self.config.name}", self.stats.current_depth)
        self.metrics.gauge(f"queue_utilization_{self.config.name}",
                          self.stats.current_depth / self.config.max_depth)
        self.metrics.gauge(f"queue_max_depth_{self.config.name}", self.stats.max_depth)
        self.metrics.gauge(f"queue_total_enqueued_{self.config.name}", self.stats.total_enqueued)
        self.metrics.gauge(f"queue_total_processed_{self.config.name}", self.stats.total_processed)
        self.metrics.gauge(f"queue_total_rejected_{self.config.name}", self.stats.total_rejected)
        self.metrics.histogram(f"queue_processing_time_{self.config.name}",
                              self.stats.average_processing_time)

    def get_stats(self) -> QueueStats:
        """Get queue statistics."""
        return self.stats.copy()


class QueueManager:
    """Manager for multiple queues with centralized monitoring."""

    _instance: Optional['QueueManager'] = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls):
        return cls._instance or super().__new__(cls)

    def __init__(self):
        if hasattr(self, '_initialized'):
            return

        self._queues: Dict[str, QueueMonitor] = {}
        self._metrics = get_metrics_client()
        self._logger = get_logger(__name__)
        self._initialized = True

    async def get_or_create_queue(
        self,
        name: str,
        config: Optional[QueueConfig] = None
    ) -> QueueMonitor:
        """Get or create a queue monitor."""
        if name not in self._queues:
            if config is None:
                config = QueueConfig(name=name)

            queue = QueueMonitor(config)
            await queue.initialize()
            self._queues[name] = queue

        return self._queues[name]

    async def get_queue_stats(self) -> Dict[str, QueueStats]:
        """Get statistics for all queues."""
        return {
            name: queue.get_stats()
            for name, queue in self._queues.items()
        }

    async def get_throttled_queues(self) -> List[str]:
        """Get names of queues that should be throttled."""
        return [
            name for name, queue in self._queues.items()
            if queue.should_throttle()
        ]

    async def get_critical_queues(self) -> List[str]:
        """Get names of queues that should reject requests."""
        return [
            name for name, queue in self._queues.items()
            if queue.should_reject()
        ]

    async def close_all(self):
        """Close all queue monitors."""
        for queue in self._queues.values():
            await queue.close()


# Global queue manager instance
_queue_manager = QueueManager()


async def get_queue_monitor(
    name: str,
    config: Optional[QueueConfig] = None
) -> QueueMonitor:
    """Get or create a queue monitor."""
    return await _queue_manager.get_or_create_queue(name, config)


async def get_queue_stats() -> Dict[str, QueueStats]:
    """Get statistics for all queues."""
    return await _queue_manager.get_queue_stats()


async def get_throttled_queues() -> List[str]:
    """Get names of queues that should be throttled."""
    return await _queue_manager.get_throttled_queues()


async def get_critical_queues() -> List[str]:
    """Get names of queues that should reject requests."""
    return await _queue_manager.get_critical_queues()


__all__ = [
    "QueueConfig",
    "QueueStats",
    "QueueMonitor",
    "QueueManager",
    "get_queue_monitor",
    "get_queue_stats",
    "get_throttled_queues",
    "get_critical_queues",
]
