"""Dataset-level quota and concurrency management system.

This module provides comprehensive quota management across all ISO adapters,
including rate limiting, concurrency control, and usage tracking with alerting.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple, Any
from collections import defaultdict
import threading

from ...observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


@dataclass
class DatasetQuota:
    """Quota configuration for a specific dataset."""

    dataset_id: str
    iso_code: str
    max_records_per_hour: int = 1_000_000
    max_bytes_per_hour: int = 1_000_000_000  # 1GB
    max_concurrent_requests: int = 5
    max_requests_per_minute: int = 60
    max_requests_per_hour: int = 1000
    priority: int = 1  # 1=highest, 10=lowest
    burst_limit: int = 10

    # Usage tracking
    _current_records: int = field(default=0, init=False)
    _current_bytes: int = field(default=0, init=False)
    _current_requests: int = field(default=0, init=False)
    _hour_start: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0), init=False)

    def check_quota(self, record_count: int, byte_count: int, request_count: int = 1) -> Tuple[bool, str]:
        """Check if quota allows the given usage."""
        now = datetime.now(timezone.utc)
        if now.hour != self._hour_start.hour:
            # Reset counters for new hour
            self._current_records = 0
            self._current_bytes = 0
            self._current_requests = 0
            self._hour_start = now.replace(minute=0, second=0, microsecond=0)

        # Check all limits
        if self._current_records + record_count > self.max_records_per_hour:
            return False, f"Record quota exceeded: {self._current_records + record_count}/{self.max_records_per_hour}"

        if self._current_bytes + byte_count > self.max_bytes_per_hour:
            return False, f"Byte quota exceeded: {self._current_bytes + byte_count}/{self.max_bytes_per_hour}"

        if self._current_requests + request_count > self.max_requests_per_hour:
            return False, f"Request quota exceeded: {self._current_requests + request_count}/{self.max_requests_per_hour}"

        return True, "OK"

    def update_usage(self, record_count: int, byte_count: int, request_count: int = 1):
        """Update usage counters."""
        self._current_records += record_count
        self._current_bytes += byte_count
        self._current_requests += request_count

    def get_usage_stats(self) -> Dict[str, Any]:
        """Get current usage statistics."""
        now = datetime.now(timezone.utc)
        hour_key = now.strftime("%Y-%m-%d-%H")

        return {
            "dataset_id": self.dataset_id,
            "iso_code": self.iso_code,
            "hour": hour_key,
            "records_used": self._current_records,
            "records_limit": self.max_records_per_hour,
            "records_percent": (self._current_records / self.max_records_per_hour) * 100 if self.max_records_per_hour > 0 else 0,
            "bytes_used": self._current_bytes,
            "bytes_limit": self.max_bytes_per_hour,
            "bytes_percent": (self._current_bytes / self.max_bytes_per_hour) * 100 if self.max_bytes_per_hour > 0 else 0,
            "requests_used": self._current_requests,
            "requests_limit": self.max_requests_per_hour,
            "requests_percent": (self._current_requests / self.max_requests_per_hour) * 100 if self.max_requests_per_hour > 0 else 0,
            "priority": self.priority
        }


class ConcurrencyController:
    """Manages concurrent requests across datasets."""

    def __init__(self):
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        self._active_requests: Dict[str, Set[asyncio.Task]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def acquire_slot(self, dataset_id: str, max_concurrent: int, timeout: float = 30.0) -> bool:
        """Acquire a concurrency slot for a dataset."""
        async with self._lock:
            if dataset_id not in self._semaphores:
                self._semaphores[dataset_id] = asyncio.Semaphore(max_concurrent)

        try:
            await asyncio.wait_for(
                self._semaphores[dataset_id].acquire(),
                timeout=timeout
            )
            return True
        except asyncio.TimeoutError:
            logger.warning(f"Timeout acquiring concurrency slot for dataset {dataset_id}")
            return False

    def release_slot(self, dataset_id: str):
        """Release a concurrency slot for a dataset."""
        if dataset_id in self._semaphores:
            self._semaphores[dataset_id].release()

    async def get_active_count(self, dataset_id: str) -> int:
        """Get the count of active requests for a dataset."""
        async with self._lock:
            if dataset_id in self._active_requests:
                # Clean up completed tasks
                active_tasks = set()
                for task in self._active_requests[dataset_id]:
                    if not task.done():
                        active_tasks.add(task)
                self._active_requests[dataset_id] = active_tasks
                return len(active_tasks)
            return 0

    def get_concurrency_stats(self) -> Dict[str, Any]:
        """Get concurrency statistics across all datasets."""
        stats = {}
        for dataset_id, semaphore in self._semaphores.items():
            stats[dataset_id] = {
                "max_concurrent": semaphore._value if hasattr(semaphore, '_value') else 'unknown',
                "active_requests": len(self._active_requests[dataset_id])
            }
        return stats


class RateLimiter:
    """Token bucket rate limiter for API requests."""

    def __init__(self, requests_per_minute: int, burst_limit: int):
        self.requests_per_minute = requests_per_minute
        self.burst_limit = burst_limit
        self.tokens_per_second = requests_per_minute / 60.0

        self._tokens: float = burst_limit
        self._last_update = time.time()
        self._lock = threading.Lock()

    def can_make_request(self) -> bool:
        """Check if a request can be made without exceeding rate limits."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update

            # Add tokens based on elapsed time
            self._tokens = min(self.burst_limit, self._tokens + elapsed * self.tokens_per_second)
            self._last_update = now

            if self._tokens >= 1.0:
                return True
            return False

    def record_request(self):
        """Record that a request was made."""
        with self._lock:
            self._tokens = max(0, self._tokens - 1.0)

    def get_tokens_remaining(self) -> float:
        """Get the number of tokens remaining."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update
            tokens = min(self.burst_limit, self._tokens + elapsed * self.tokens_per_second)
            return tokens


class QuotaManager:
    """Main quota management system."""

    def __init__(self):
        self.datasets: Dict[str, DatasetQuota] = {}
        self.concurrency_controller = ConcurrencyController()
        self.rate_limiters: Dict[str, RateLimiter] = {}
        self.metrics = get_metrics_client()

        # Alerting thresholds
        self.alert_thresholds = {
            "records_percent": 80.0,
            "bytes_percent": 80.0,
            "requests_percent": 80.0,
            "concurrency_percent": 80.0
        }

        # Start background monitoring
        self._monitoring_task: Optional[asyncio.Task] = None

    def register_dataset(self, quota: DatasetQuota):
        """Register a dataset quota."""
        self.datasets[quota.dataset_id] = quota

        # Create rate limiter
        self.rate_limiters[quota.dataset_id] = RateLimiter(
            quota.max_requests_per_minute,
            quota.burst_limit
        )

        logger.info(f"Registered quota for dataset {quota.dataset_id}: {quota}")

    async def check_and_acquire_quota(
        self,
        dataset_id: str,
        record_count: int,
        byte_count: int,
        request_count: int = 1
    ) -> Tuple[bool, str]:
        """Check quota and acquire concurrency slot."""
        if dataset_id not in self.datasets:
            logger.warning(f"No quota configured for dataset {dataset_id}")
            return True, "No quota configured"

        quota = self.datasets[dataset_id]

        # Check quota limits
        allowed, reason = quota.check_quota(record_count, byte_count, request_count)
        if not allowed:
            self.metrics.increment_counter("quota_manager.quota_violations")
            return False, reason

        # Check rate limiting
        rate_limiter = self.rate_limiters[dataset_id]
        if not rate_limiter.can_make_request():
            self.metrics.increment_counter("quota_manager.rate_limit_violations")
            return False, "Rate limit exceeded"

        # Acquire concurrency slot
        concurrency_allowed = await self.concurrency_controller.acquire_slot(
            dataset_id,
            quota.max_concurrent_requests
        )

        if not concurrency_allowed:
            self.metrics.increment_counter("quota_manager.concurrency_violations")
            return False, "Concurrency limit exceeded"

        # Record rate-limited request
        rate_limiter.record_request()

        return True, "OK"

    def release_quota(self, dataset_id: str, record_count: int, byte_count: int, request_count: int = 1):
        """Release quota and update usage."""
        if dataset_id in self.datasets:
            quota = self.datasets[dataset_id]
            quota.update_usage(record_count, byte_count, request_count)

            # Release concurrency slot
            self.concurrency_controller.release_slot(dataset_id)

    def get_quota_status(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Get quota status for a dataset."""
        if dataset_id not in self.datasets:
            return None

        quota = self.datasets[dataset_id]
        rate_limiter = self.rate_limiters[dataset_id]

        return {
            **quota.get_usage_stats(),
            "rate_limiter_tokens": rate_limiter.get_tokens_remaining(),
            "active_concurrent_requests": asyncio.run(self.concurrency_controller.get_active_count(dataset_id))
        }

    def get_all_quota_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Get quota status for all datasets."""
        statuses = {}
        for dataset_id in self.datasets:
            status = self.get_quota_status(dataset_id)
            if status:
                statuses[dataset_id] = status
        return statuses

    async def check_alerts(self) -> List[Dict[str, Any]]:
        """Check for quota alerts and return list of alerts."""
        alerts = []

        for dataset_id, quota in self.datasets.items():
            status = self.get_quota_status(dataset_id)
            if not status:
                continue

            # Check usage thresholds
            if status["records_percent"] > self.alert_thresholds["records_percent"]:
                alerts.append({
                    "dataset_id": dataset_id,
                    "alert_type": "records_quota_warning",
                    "message": f"Records usage at {status['records_percent']:.1f}%",
                    "current_usage": status["records_used"],
                    "limit": status["records_limit"],
                    "severity": "warning"
                })

            if status["bytes_percent"] > self.alert_thresholds["bytes_percent"]:
                alerts.append({
                    "dataset_id": dataset_id,
                    "alert_type": "bytes_quota_warning",
                    "message": f"Bytes usage at {status['bytes_percent']:.1f}%",
                    "current_usage": status["bytes_used"],
                    "limit": status["bytes_limit"],
                    "severity": "warning"
                })

            if status["requests_percent"] > self.alert_thresholds["requests_percent"]:
                alerts.append({
                    "dataset_id": dataset_id,
                    "alert_type": "requests_quota_warning",
                    "message": f"Requests usage at {status['requests_percent']".1f"}%",
                    "current_usage": status["requests_used"],
                    "limit": status["requests_limit"],
                    "severity": "warning"
                })

        return alerts

    async def start_monitoring(self):
        """Start background monitoring task."""
        if self._monitoring_task is None:
            self._monitoring_task = asyncio.create_task(self._monitor_quotas())

    async def stop_monitoring(self):
        """Stop background monitoring task."""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            self._monitoring_task = None

    async def _monitor_quotas(self):
        """Background task to monitor quotas and emit alerts."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                alerts = await self.check_alerts()

                for alert in alerts:
                    logger.warning(
                        "Quota alert: %s",
                        alert["message"],
                        extra={
                            "dataset_id": alert["dataset_id"],
                            "alert_type": alert["alert_type"],
                            "severity": alert["severity"]
                        }
                    )

                    # Emit metrics
                    self.metrics.increment_counter(
                        f"quota_manager.{alert['alert_type']}",
                        tags={"dataset_id": alert["dataset_id"]}
                    )

                # Emit general metrics
                all_statuses = self.get_all_quota_statuses()
                for dataset_id, status in all_statuses.items():
                    self.metrics.gauge(
                        f"quota_manager.records_percent.{dataset_id}",
                        status["records_percent"]
                    )
                    self.metrics.gauge(
                        f"quota_manager.bytes_percent.{dataset_id}",
                        status["bytes_percent"]
                    )
                    self.metrics.gauge(
                        f"quota_manager.requests_percent.{dataset_id}",
                        status["requests_percent"]
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in quota monitoring: %s", e)

    def get_concurrency_stats(self) -> Dict[str, Any]:
        """Get concurrency statistics."""
        return self.concurrency_controller.get_concurrency_stats()


# Global quota manager instance
_global_quota_manager: Optional[QuotaManager] = None
_quota_manager_lock = asyncio.Lock()


async def get_quota_manager() -> QuotaManager:
    """Get or create the global quota manager."""
    global _global_quota_manager

    async with _quota_manager_lock:
        if _global_quota_manager is None:
            _global_quota_manager = QuotaManager()
            await _global_quota_manager.start_monitoring()

    return _global_quota_manager


async def configure_dataset_quotas(quotas: List[DatasetQuota]):
    """Configure quotas for multiple datasets."""
    manager = await get_quota_manager()

    for quota in quotas:
        manager.register_dataset(quota)

    logger.info(f"Configured quotas for {len(quotas)} datasets")


# Convenience function to create standard quotas for ISO datasets
def create_standard_iso_quotas() -> List[DatasetQuota]:
    """Create standard quota configurations for all ISOs."""
    quotas = []

    # CAISO quotas
    for data_type in ['lmp', 'load', 'asm']:
        quotas.append(DatasetQuota(
            dataset_id=f"caiso_{data_type}",
            iso_code="CAISO",
            max_records_per_hour=500_000,
            max_bytes_per_hour=500_000_000,
            max_concurrent_requests=3,
            max_requests_per_minute=30,
            priority=1
        ))

    # MISO quotas
    for data_type in ['lmp', 'load', 'generation_mix']:
        quotas.append(DatasetQuota(
            dataset_id=f"miso_{data_type}",
            iso_code="MISO",
            max_records_per_hour=300_000,
            max_bytes_per_hour=300_000_000,
            max_concurrent_requests=5,
            max_requests_per_minute=50,
            priority=2
        ))

    # PJM quotas
    for data_type in ['lmp', 'load', 'generation']:
        quotas.append(DatasetQuota(
            dataset_id=f"pjm_{data_type}",
            iso_code="PJM",
            max_records_per_hour=400_000,
            max_bytes_per_hour=400_000_000,
            max_concurrent_requests=4,
            max_requests_per_minute=40,
            priority=1
        ))

    # ERCOT quotas
    for data_type in ['spp', 'load', 'generation']:
        quotas.append(DatasetQuota(
            dataset_id=f"ercot_{data_type}",
            iso_code="ERCOT",
            max_records_per_hour=600_000,
            max_bytes_per_hour=600_000_000,
            max_concurrent_requests=6,
            max_requests_per_minute=60,
            priority=1
        ))

    # SPP quotas
    for data_type in ['lmp', 'load', 'generation']:
        quotas.append(DatasetQuota(
            dataset_id=f"spp_{data_type}",
            iso_code="SPP",
            max_records_per_hour=200_000,
            max_bytes_per_hour=200_000_000,
            max_concurrent_requests=3,
            max_requests_per_minute=30,
            priority=3
        ))

    return quotas
