"""Monitoring and alerting for external data pipelines."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable

from aurum.external.collect.base import CollectorMetrics
from aurum.observability.metrics import (
    EXTERNAL_PIPELINE_HEALTH,
    EXTERNAL_PIPELINE_SLA_VIOLATIONS,
    EXTERNAL_DATA_FRESHNESS,
    EXTERNAL_DATA_QUALITY_SCORE
)

logger = logging.getLogger(__name__)


class PipelineHealth:
    """Health status for external data pipeline components."""

    def __init__(
        self,
        component: str,
        status: str = "unknown",  # unknown, healthy, degraded, failed
        last_check: Optional[datetime] = None,
        error_message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ):
        self.component = component
        self.status = status
        self.last_check = last_check or datetime.now()
        self.error_message = error_message
        self.metrics = metrics or {}

    def is_healthy(self) -> bool:
        """Check if component is healthy."""
        return self.status == "healthy"

    def is_degraded(self) -> bool:
        """Check if component is degraded."""
        return self.status == "degraded"

    def is_failed(self) -> bool:
        """Check if component has failed."""
        return self.status == "failed"

    def to_dict(self) -> Dict[str, Any]:
        """Convert health status to dictionary."""
        return {
            "component": self.component,
            "status": self.status,
            "last_check": self.last_check.isoformat(),
            "error_message": self.error_message,
            "metrics": self.metrics
        }


class SLAMonitor:
    """SLA monitoring for external data pipelines."""

    def __init__(self):
        self._sla_rules = {}
        self._violations = []

    def add_sla_rule(
        self,
        name: str,
        target: str,
        threshold: timedelta,
        alert_on_violation: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ):
        """Add SLA rule for monitoring."""
        self._sla_rules[name] = {
            "target": target,
            "threshold": threshold,
            "alert_callback": alert_on_violation
        }

    async def check_sla_violation(
        self,
        rule_name: str,
        actual_duration: timedelta,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check if SLA rule is violated."""
        if rule_name not in self._sla_rules:
            return False

        rule = self._sla_rules[rule_name]
        threshold = rule["threshold"]

        if actual_duration > threshold:
            violation = {
                "rule_name": rule_name,
                "target": rule["target"],
                "threshold_seconds": threshold.total_seconds(),
                "actual_seconds": actual_duration.total_seconds(),
                "context": context or {},
                "timestamp": datetime.now().isoformat()
            }

            self._violations.append(violation)

            # Record metric
            if EXTERNAL_PIPELINE_SLA_VIOLATIONS:
                EXTERNAL_PIPELINE_SLA_VIOLATIONS.labels(
                    rule=rule_name,
                    target=rule["target"]
                ).inc()

            # Call alert callback if provided
            if rule["alert_callback"]:
                await rule["alert_callback"](rule_name, violation)

            logger.warning(
                "SLA violation detected",
                extra={
                    "rule": rule_name,
                    "target": rule["target"],
                    "threshold": threshold.total_seconds(),
                    "actual": actual_duration.total_seconds()
                }
            )

            return True

        return False

    def get_violations(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get SLA violations in the last N hours."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            v for v in self._violations
            if datetime.fromisoformat(v["timestamp"]) > cutoff_time
        ]


class DataQualityMonitor:
    """Monitor data quality metrics for external data."""

    def __init__(self):
        self._quality_scores = {}
        self._quality_thresholds = {
            "completeness": 0.95,
            "accuracy": 0.90,
            "timeliness": 0.85,
            "consistency": 0.95
        }

    async def record_quality_metric(
        self,
        provider: str,
        dataset: str,
        metric_name: str,
        value: float,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Record a data quality metric."""
        key = f"{provider}:{dataset}:{metric_name}"

        self._quality_scores[key] = {
            "value": value,
            "timestamp": datetime.now(),
            "metadata": metadata or {}
        }

        # Record metric
        if EXTERNAL_DATA_QUALITY_SCORE:
            EXTERNAL_DATA_QUALITY_SCORE.labels(
                provider=provider,
                dataset=dataset,
                metric=metric_name
            ).set(value)

        logger.info(
            "Data quality metric recorded",
            extra={
                "provider": provider,
                "dataset": dataset,
                "metric": metric_name,
                "value": value
            }
        )

    async def get_quality_score(
        self,
        provider: str,
        dataset: str,
        weights: Optional[Dict[str, float]] = None
    ) -> float:
        """Calculate overall quality score for a dataset."""
        weights = weights or {
            "completeness": 0.3,
            "accuracy": 0.4,
            "timeliness": 0.2,
            "consistency": 0.1
        }

        total_score = 0.0
        total_weight = 0.0

        for metric_name, weight in weights.items():
            key = f"{provider}:{dataset}:{metric_name}"
            if key in self._quality_scores:
                score = self._quality_scores[key]
                total_score += score["value"] * weight
                total_weight += weight

        if total_weight == 0:
            return 0.0

        overall_score = total_score / total_weight

        # Record overall quality score
        if EXTERNAL_DATA_QUALITY_SCORE:
            EXTERNAL_DATA_QUALITY_SCORE.labels(
                provider=provider,
                dataset=dataset,
                metric="overall"
            ).set(overall_score)

        return overall_score

    async def is_quality_acceptable(
        self,
        provider: str,
        dataset: str,
        min_score: float = 0.8
    ) -> bool:
        """Check if data quality is acceptable."""
        score = await self.get_quality_score(provider, dataset)
        return score >= min_score


class ExternalDataMonitor:
    """Comprehensive monitor for external data pipelines."""

    def __init__(self):
        self._pipeline_health = {}
        self._sla_monitor = SLAMonitor()
        self._quality_monitor = DataQualityMonitor()

        # Set up SLA rules
        self._setup_sla_rules()

    def _setup_sla_rules(self):
        """Set up SLA rules for monitoring."""
        # Incremental processing SLA - should complete within 2 hours
        self._sla_monitor.add_sla_rule(
            name="incremental_processing",
            target="external_data_incremental",
            threshold=timedelta(hours=2)
        )

        # Backfill processing SLA - should complete within 8 hours
        self._sla_monitor.add_sla_rule(
            name="backfill_processing",
            target="external_data_backfill",
            threshold=timedelta(hours=8)
        )

        # Data freshness SLA - data should be no older than 24 hours
        self._sla_monitor.add_sla_rule(
            name="data_freshness",
            target="external_data_freshness",
            threshold=timedelta(hours=24)
        )

    async def record_pipeline_health(
        self,
        component: str,
        status: str,
        error_message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ):
        """Record pipeline health status."""
        health = PipelineHealth(
            component=component,
            status=status,
            error_message=error_message,
            metrics=metrics
        )

        self._pipeline_health[component] = health

        # Record metric
        if EXTERNAL_PIPELINE_HEALTH:
            health_score = 1.0 if status == "healthy" else 0.5 if status == "degraded" else 0.0
            EXTERNAL_PIPELINE_HEALTH.labels(component=component).set(health_score)

        logger.info(
            "Pipeline health recorded",
            extra={
                "component": component,
                "status": status,
                "error_message": error_message
            }
        )

    async def check_overall_health(self) -> Dict[str, Any]:
        """Check overall pipeline health."""
        healthy_count = 0
        degraded_count = 0
        failed_count = 0

        for component, health in self._pipeline_health.items():
            if health.is_healthy():
                healthy_count += 1
            elif health.is_degraded():
                degraded_count += 1
            elif health.is_failed():
                failed_count += 1

        overall_status = "healthy"
        if failed_count > 0:
            overall_status = "failed"
        elif degraded_count > 0:
            overall_status = "degraded"

        return {
            "overall_status": overall_status,
            "healthy_components": healthy_count,
            "degraded_components": degraded_count,
            "failed_components": failed_count,
            "total_components": len(self._pipeline_health),
            "components": {
                name: health.to_dict()
                for name, health in self._pipeline_health.items()
            }
        }

    async def record_data_freshness(
        self,
        provider: str,
        dataset: str,
        last_updated: datetime,
        expected_frequency_hours: int = 24
    ):
        """Record data freshness for a dataset."""
        now = datetime.now()
        age_hours = (now - last_updated).total_seconds() / 3600

        # Record freshness metric
        if EXTERNAL_DATA_FRESHNESS:
            EXTERNAL_DATA_FRESHNESS.labels(
                provider=provider,
                dataset=dataset
            ).set(age_hours)

        # Check SLA violation
        await self._sla_monitor.check_sla_violation(
            rule_name="data_freshness",
            actual_duration=timedelta(hours=age_hours),
            context={
                "provider": provider,
                "dataset": dataset,
                "last_updated": last_updated.isoformat(),
                "expected_frequency_hours": expected_frequency_hours
            }
        )

        logger.info(
            "Data freshness recorded",
            extra={
                "provider": provider,
                "dataset": dataset,
                "age_hours": age_hours,
                "last_updated": last_updated.isoformat()
            }
        )

    async def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get comprehensive monitoring summary."""
        return {
            "pipeline_health": await self.check_overall_health(),
            "sla_violations": self._sla_monitor.get_violations(),
            "timestamp": datetime.now().isoformat()
        }


# Global monitor instance
_external_monitor: Optional[ExternalDataMonitor] = None


def get_external_monitor() -> ExternalDataMonitor:
    """Get global external data monitor instance."""
    global _external_monitor
    if _external_monitor is None:
        _external_monitor = ExternalDataMonitor()
    return _external_monitor


async def run_health_check() -> Dict[str, Any]:
    """Run health check for external data pipeline."""
    monitor = get_external_monitor()
    return await monitor.check_overall_health()


async def run_sla_check() -> List[Dict[str, Any]]:
    """Run SLA compliance check."""
    monitor = get_external_monitor()
    return monitor._sla_monitor.get_violations()


async def run_quality_check(provider: str, dataset: str) -> Dict[str, Any]:
    """Run data quality check for a dataset."""
    monitor = get_external_monitor()
    score = await monitor._quality_monitor.get_quality_score(provider, dataset)
    acceptable = await monitor._quality_monitor.is_quality_acceptable(provider, dataset)

    return {
        "provider": provider,
        "dataset": dataset,
        "quality_score": score,
        "acceptable": acceptable,
        "timestamp": datetime.now().isoformat()
    }
