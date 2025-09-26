"""Service Level Objective (SLO) monitoring and alerting."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .metrics import (
    PROMETHEUS_AVAILABLE,
    increment_slo_compliance,
    observe_slo_violation_duration,
    set_slo_availability,
    set_slo_error_rate,
    set_slo_latency_p50,
    set_slo_latency_p95,
    set_slo_latency_p99,
)

logger = logging.getLogger(__name__)


class SLOStatus(str, Enum):
    """SLO compliance status."""
    COMPLIANT = "compliant"
    VIOLATING = "violating"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


class SLOType(str, Enum):
    """Types of SLOs."""
    AVAILABILITY = "availability"
    LATENCY = "latency"
    ERROR_RATE = "error_rate"
    THROUGHPUT = "throughput"
    FRESHNESS = "freshness"


class SLOViolationLevel(str, Enum):
    """SLO violation severity levels."""
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class SLOWindow:
    """Time window for SLO measurement."""

    def __init__(self, duration: timedelta):
        self.duration = duration
        self.start_time = datetime.now() - duration
        self.measurements: List[Dict[str, Any]] = []

    def add_measurement(self, measurement: Dict[str, Any]) -> None:
        """Add a measurement to the window."""
        self.measurements.append({
            **measurement,
            "timestamp": datetime.now()
        })

        # Remove old measurements
        cutoff_time = datetime.now() - self.duration
        self.measurements = [
            m for m in self.measurements
            if m["timestamp"] > cutoff_time
        ]

    def get_compliance_rate(self) -> float:
        """Get the compliance rate for this window."""
        if not self.measurements:
            return 1.0

        compliant = sum(1 for m in self.measurements if m.get("compliant", True))
        return compliant / len(self.measurements)

    def get_average_value(self, field: str) -> float:
        """Get the average value of a field."""
        if not self.measurements:
            return 0.0

        values = [m.get(field, 0) for m in self.measurements if field in m]
        return sum(values) / len(values) if values else 0.0


class SLO:
    """Service Level Objective definition."""

    def __init__(
        self,
        name: str,
        slo_type: SLOType,
        target: float,
        window: timedelta,
        alert_on_violation: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        description: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None
    ):
        self.name = name
        self.slo_type = slo_type
        self.target = target  # Target as percentage (0.0-1.0) or value
        self.window = window
        self.alert_callback = alert_on_violation
        self.description = description or f"SLO for {name}"
        self.labels = labels or {}
        self.measurement_window = SLOWindow(window)
        self.violation_start: Optional[datetime] = None
        self.last_violation_duration: Optional[timedelta] = None

    def add_measurement(self, value: float, compliant: bool = True, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Add a measurement to this SLO."""
        measurement = {
            "value": value,
            "compliant": compliant,
            "metadata": metadata or {}
        }
        self.measurement_window.add_measurement(measurement)

        # Check for violations
        if not compliant:
            if self.violation_start is None:
                self.violation_start = datetime.now()
                logger.warning(
                    "SLO violation started",
                    extra={
                        "slo_name": self.name,
                        "slo_type": self.slo_type.value,
                        "target": self.target,
                        "value": value
                    }
                )
        else:
            if self.violation_start is not None:
                violation_duration = datetime.now() - self.violation_start
                self.last_violation_duration = violation_duration
                self.violation_start = None

                # Record violation duration
                if PROMETHEUS_AVAILABLE:
                    asyncio.create_task(
                        observe_slo_violation_duration(
                            self.name,
                            violation_duration.total_seconds()
                        )
                    )

                logger.info(
                    "SLO violation ended",
                    extra={
                        "slo_name": self.name,
                        "slo_type": self.slo_type.value,
                        "violation_duration_seconds": violation_duration.total_seconds()
                    }
                )

    def is_violating(self) -> bool:
        """Check if SLO is currently violating."""
        compliance_rate = self.measurement_window.get_compliance_rate()
        return compliance_rate < self.target

    def get_current_compliance_rate(self) -> float:
        """Get current compliance rate."""
        return self.measurement_window.get_compliance_rate()

    def get_status(self) -> SLOStatus:
        """Get current SLO status."""
        if self.violation_start is not None:
            return SLOStatus.VIOLATING

        compliance_rate = self.get_current_compliance_rate()
        if compliance_rate >= self.target:
            return SLOStatus.COMPLIANT
        elif compliance_rate >= (self.target * 0.9):  # 90% of target
            return SLOStatus.DEGRADED
        else:
            return SLOStatus.VIOLATING

    async def check_and_alert(self) -> None:
        """Check SLO status and trigger alerts if needed."""
        status = self.get_status()

        if status == SLOStatus.VIOLATING and self.alert_callback:
            await self.alert_callback({
                "slo_name": self.name,
                "slo_type": self.slo_type.value,
                "status": status.value,
                "compliance_rate": self.get_current_compliance_rate(),
                "target": self.target,
                "violation_start": self.violation_start.isoformat() if self.violation_start else None,
                "labels": self.labels
            })

    def get_summary(self) -> Dict[str, Any]:
        """Get SLO summary."""
        return {
            "name": self.name,
            "type": self.slo_type.value,
            "target": self.target,
            "window_duration_seconds": self.window.total_seconds(),
            "current_compliance_rate": self.get_current_compliance_rate(),
            "status": self.get_status().value,
            "measurements_count": len(self.measurement_window.measurements),
            "violation_start": self.violation_start.isoformat() if self.violation_start else None,
            "last_violation_duration_seconds": self.last_violation_duration.total_seconds() if self.last_violation_duration else None,
            "labels": self.labels
        }


class SLOMonitor:
    """Monitor multiple SLOs and manage alerting."""

    def __init__(self):
        self.slos: Dict[str, SLO] = {}
        self.alert_history: List[Dict[str, Any]] = []
        self.last_check: Optional[datetime] = None

    def add_slo(self, slo: SLO) -> None:
        """Add an SLO to monitor."""
        self.slos[slo.name] = slo
        logger.info(
            "Added SLO",
            extra={
                "slo_name": slo.name,
                "slo_type": slo.slo_type.value,
                "target": slo.target
            }
        )

    def add_availability_slo(
        self,
        name: str,
        target: float = 0.99,
        window: timedelta = timedelta(days=30),
        alert_on_violation: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        description: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> SLO:
        """Add an availability SLO."""
        slo = SLO(
            name=name,
            slo_type=SLOType.AVAILABILITY,
            target=target,
            window=window,
            alert_on_violation=alert_on_violation,
            description=description,
            labels=labels
        )
        self.add_slo(slo)
        return slo

    def add_latency_slo(
        self,
        name: str,
        target: float,
        window: timedelta = timedelta(days=30),
        alert_on_violation: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        description: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> SLO:
        """Add a latency SLO."""
        slo = SLO(
            name=name,
            slo_type=SLOType.LATENCY,
            target=target,  # Target as seconds
            window=window,
            alert_on_violation=alert_on_violation,
            description=description,
            labels=labels
        )
        self.add_slo(slo)
        return slo

    def add_error_rate_slo(
        self,
        name: str,
        target: float = 0.01,  # 1% error rate
        window: timedelta = timedelta(days=30),
        alert_on_violation: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        description: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> SLO:
        """Add an error rate SLO."""
        slo = SLO(
            name=name,
            slo_type=SLOType.ERROR_RATE,
            target=target,
            window=window,
            alert_on_violation=alert_on_violation,
            description=description,
            labels=labels
        )
        self.add_slo(slo)
        return slo

    def record_measurement(
        self,
        slo_name: str,
        value: float,
        compliant: bool = True,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record a measurement for an SLO."""
        if slo_name not in self.slos:
            logger.warning(f"Unknown SLO: {slo_name}")
            return

        slo = self.slos[slo_name]
        slo.add_measurement(value, compliant, metadata)

        # Record compliance in Prometheus
        if PROMETHEUS_AVAILABLE:
            status = "success" if compliant else "violation"
            asyncio.create_task(increment_slo_compliance(slo_name, status))

        # Update Prometheus gauges
        if PROMETHEUS_AVAILABLE:
            asyncio.create_task(self._update_prometheus_gauges(slo))

    async def _update_prometheus_gauges(self, slo: SLO) -> None:
        """Update Prometheus gauges for an SLO."""
        try:
            if slo.slo_type == SLOType.AVAILABILITY:
                await set_slo_availability(slo.name, slo.get_current_compliance_rate())
            elif slo.slo_type == SLOType.LATENCY:
                # Get average latency from measurements
                avg_latency = slo.measurement_window.get_average_value("value")
                await set_slo_latency_p50(slo.name, avg_latency)
                await set_slo_latency_p95(slo.name, avg_latency)
                await set_slo_latency_p99(slo.name, avg_latency)
            elif slo.slo_type == SLOType.ERROR_RATE:
                error_rate = 1.0 - slo.get_current_compliance_rate()
                await set_slo_error_rate(slo.name, error_rate)
        except Exception as e:
            logger.error(f"Failed to update Prometheus gauges for {slo.name}: {e}")

    async def check_all_slos(self) -> List[Dict[str, Any]]:
        """Check all SLOs and trigger alerts if needed."""
        violations = []
        self.last_check = datetime.now()

        for slo_name, slo in self.slos.items():
            try:
                await slo.check_and_alert()

                if slo.is_violating():
                    violations.append({
                        "slo_name": slo_name,
                        "slo_type": slo.slo_type.value,
                        "status": SLOStatus.VIOLATING.value,
                        "compliance_rate": slo.get_current_compliance_rate(),
                        "target": slo.target,
                        "timestamp": self.last_check.isoformat()
                    })

            except Exception as e:
                logger.error(f"Error checking SLO {slo_name}: {e}")

        return violations

    def get_slo_summary(self) -> Dict[str, Any]:
        """Get summary of all SLOs."""
        return {
            "total_slos": len(self.slos),
            "compliant_slos": len([s for s in self.slos.values() if s.get_status() == SLOStatus.COMPLIANT]),
            "violating_slos": len([s for s in self.slos.values() if s.get_status() == SLOStatus.VIOLATING]),
            "degraded_slos": len([s for s in self.slos.values() if s.get_status() == SLOStatus.DEGRADED]),
            "slos": {name: slo.get_summary() for name, slo in self.slos.items()},
            "last_check": self.last_check.isoformat() if self.last_check else None
        }

    def get_violations(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent SLO violations."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            v for v in self.alert_history
            if datetime.fromisoformat(v["timestamp"]) > cutoff_time
        ]


# Global SLO monitor instance
_slo_monitor: Optional[SLOMonitor] = None


def get_slo_monitor() -> SLOMonitor:
    """Get global SLO monitor instance."""
    global _slo_monitor
    if _slo_monitor is None:
        _slo_monitor = SLOMonitor()
    return _slo_monitor


async def setup_default_slos() -> SLOMonitor:
    """Set up default SLOs for the application."""
    monitor = get_slo_monitor()

    # API Availability SLO
    monitor.add_availability_slo(
        name="api_availability",
        target=0.99,  # 99% availability
        window=timedelta(days=30),
        description="API endpoints should be available 99% of the time"
    )

    # API Latency SLO
    monitor.add_latency_slo(
        name="api_latency",
        target=1.0,  # 1 second p95 latency
        window=timedelta(days=30),
        description="API requests should complete within 1 second (p95)"
    )

    # Error Rate SLO
    monitor.add_error_rate_slo(
        name="error_rate",
        target=0.01,  # 1% error rate
        window=timedelta(days=30),
        description="API error rate should be less than 1%"
    )

    # Data Freshness SLO
    monitor.add_availability_slo(
        name="data_freshness",
        target=0.95,  # 95% of data fresh
        window=timedelta(days=7),
        description="External data should be no older than 24 hours"
    )

    # Cache Hit Rate SLO
    monitor.add_availability_slo(
        name="cache_hit_rate",
        target=0.90,  # 90% cache hit rate
        window=timedelta(days=1),
        description="Cache hit rate should be above 90%"
    )

    return monitor


async def record_api_measurement(
    slo_name: str,
    duration_seconds: float,
    status_code: int,
    method: str = "GET",
    path: str = "/"
) -> None:
    """Record an API measurement for SLO tracking."""
    monitor = get_slo_monitor()

    # Determine if request was successful
    is_success = 200 <= status_code < 400

    # For latency SLOs, record the duration
    if "latency" in slo_name.lower():
        compliant = duration_seconds <= 1.0  # 1 second threshold
        monitor.record_measurement(slo_name, duration_seconds, compliant)

    # For availability SLOs, record success/failure
    elif "availability" in slo_name.lower() or "error_rate" in slo_name.lower():
        compliant = is_success
        monitor.record_measurement(slo_name, 1.0 if is_success else 0.0, compliant)


async def record_data_freshness_measurement(
    slo_name: str,
    dataset: str,
    hours_old: float
) -> None:
    """Record a data freshness measurement."""
    monitor = get_slo_monitor()

    # Data is considered fresh if < 24 hours old
    is_fresh = hours_old <= 24.0
    compliant = is_fresh

    monitor.record_measurement(slo_name, hours_old, compliant, {
        "dataset": dataset,
        "hours_old": hours_old,
        "is_fresh": is_fresh
    })


async def record_cache_measurement(
    slo_name: str,
    cache_type: str,
    hit_ratio: float
) -> None:
    """Record a cache performance measurement."""
    monitor = get_slo_monitor()

    # Cache hit ratio should be above 90%
    is_good = hit_ratio >= 0.90
    compliant = is_good

    monitor.record_measurement(slo_name, hit_ratio, compliant, {
        "cache_type": cache_type,
        "hit_ratio": hit_ratio
    })
