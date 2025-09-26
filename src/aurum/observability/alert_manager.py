"""Alert management and routing system."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .metrics import (
    PROMETHEUS_AVAILABLE,
    increment_alerts_fired,
    increment_alerts_suppressed,
    observe_alert_processing_duration,
)

logger = logging.getLogger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class AlertChannel(str, Enum):
    """Alert notification channels."""
    SLACK = "slack"
    PAGERDUTY = "pagerduty"
    EMAIL = "email"
    WEBHOOK = "webhook"
    LOG = "log"


class AlertStatus(str, Enum):
    """Alert processing status."""
    PENDING = "pending"
    FIRING = "firing"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class AlertRule:
    """Alert rule definition."""

    def __init__(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        severity: AlertSeverity,
        channels: List[AlertChannel],
        description: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        cooldown_minutes: int = 15,
        tags: Optional[List[str]] = None
    ):
        self.name = name
        self.condition = condition
        self.severity = severity
        self.channels = channels
        self.description = description or f"Alert for {name}"
        self.labels = labels or {}
        self.cooldown_minutes = cooldown_minutes
        self.tags = tags or []
        self.last_fired: Optional[datetime] = None
        self.is_silenced = False

    def should_fire(self, context: Dict[str, Any]) -> bool:
        """Check if alert should fire."""
        if self.is_silenced:
            return False

        if self.condition(context):
            # Check cooldown
            if self.last_fired:
                cooldown_duration = timedelta(minutes=self.cooldown_minutes)
                if datetime.now() - self.last_fired < cooldown_duration:
                    return False

            return True

        return False

    def mark_fired(self) -> None:
        """Mark alert as fired."""
        self.last_fired = datetime.now()


class Alert:
    """Alert instance."""

    def __init__(
        self,
        rule: AlertRule,
        context: Dict[str, Any],
        fingerprint: Optional[str] = None
    ):
        self.rule = rule
        self.context = context
        self.fingerprint = fingerprint or self._generate_fingerprint()
        self.status = AlertStatus.PENDING
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.acknowledged_at: Optional[datetime] = None
        self.resolved_at: Optional[datetime] = None
        self.notification_history: List[Dict[str, Any]] = []

    def _generate_fingerprint(self) -> str:
        """Generate fingerprint for alert deduplication."""
        context_keys = sorted(self.context.keys())
        fingerprint_data = {
            "rule": self.rule.name,
            "context": {k: self.context[k] for k in context_keys}
        }
        return str(hash(json.dumps(fingerprint_data, sort_keys=True)))

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "rule_name": self.rule.name,
            "severity": self.rule.severity.value,
            "status": self.status.value,
            "context": self.context,
            "fingerprint": self.fingerprint,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "channels": [c.value for c in self.rule.channels],
            "description": self.rule.description,
            "labels": self.rule.labels,
            "tags": self.rule.tags,
            "notification_history": self.notification_history
        }


class AlertManager:
    """Manage alerts and notifications."""

    def __init__(self):
        self.rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.notification_channels: Dict[AlertChannel, Callable[[Alert], Awaitable[None]]] = {}
        self.max_history_size = 10000

    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self.rules[rule.name] = rule
        logger.info(f"Added alert rule: {rule.name}")

    def add_notification_channel(
        self,
        channel: AlertChannel,
        handler: Callable[[Alert], Awaitable[None]]
    ) -> None:
        """Add notification channel handler."""
        self.notification_channels[channel] = handler
        logger.info(f"Added notification channel: {channel.value}")

    async def process_alerts(self, context: Dict[str, Any]) -> List[Alert]:
        """Process alerts based on current context."""
        fired_alerts = []

        for rule_name, rule in self.rules.items():
            try:
                if rule.should_fire(context):
                    alert = self._create_or_update_alert(rule, context)
                    await self._fire_alert(alert)
                    fired_alerts.append(alert)
                    rule.mark_fired()

            except Exception as e:
                logger.error(f"Error processing alert rule {rule_name}: {e}")

        # Check for resolved alerts
        await self._check_resolved_alerts(context)

        return fired_alerts

    def _create_or_update_alert(self, rule: AlertRule, context: Dict[str, Any]) -> Alert:
        """Create or update alert."""
        fingerprint = rule.name  # Simplified fingerprint

        if fingerprint in self.active_alerts:
            alert = self.active_alerts[fingerprint]
            alert.context.update(context)
            alert.updated_at = datetime.now()
            return alert

        alert = Alert(rule, context, fingerprint)
        self.active_alerts[fingerprint] = alert
        return alert

    async def _fire_alert(self, alert: Alert) -> None:
        """Fire alert to configured channels."""
        start_time = datetime.now()
        alert.status = AlertStatus.FIRING

        # Record metric
        if PROMETHEUS_AVAILABLE:
            for channel in alert.rule.channels:
                await increment_alerts_fired(
                    alert.rule.name,
                    alert.rule.severity.value,
                    channel.value
                )

        try:
            # Send notifications
            for channel in alert.rule.channels:
                if channel in self.notification_channels:
                    await self.notification_channels[channel](alert)
                    alert.notification_history.append({
                        "channel": channel.value,
                        "timestamp": datetime.now().isoformat(),
                        "status": "sent"
                    })

            logger.info(
                "Alert fired",
                extra={
                    "alert_name": alert.rule.name,
                    "severity": alert.rule.severity.value,
                    "channels": [c.value for c in alert.rule.channels]
                }
            )

        except Exception as e:
            logger.error(f"Failed to fire alert {alert.rule.name}: {e}")
            alert.notification_history.append({
                "channel": "unknown",
                "timestamp": datetime.now().isoformat(),
                "status": "failed",
                "error": str(e)
            })

        # Record processing duration
        if PROMETHEUS_AVAILABLE:
            duration = (datetime.now() - start_time).total_seconds()
            await observe_alert_processing_duration(alert.rule.name, duration)

    async def _check_resolved_alerts(self, context: Dict[str, Any]) -> None:
        """Check for alerts that should be resolved."""
        resolved_alerts = []

        for fingerprint, alert in list(self.active_alerts.items()):
            if not alert.rule.should_fire(context):
                alert.status = AlertStatus.RESOLVED
                alert.resolved_at = datetime.now()
                resolved_alerts.append(alert)

                # Remove from active alerts
                del self.active_alerts[fingerprint]

        for alert in resolved_alerts:
            self.alert_history.append(alert)
            logger.info(
                "Alert resolved",
                extra={
                    "alert_name": alert.rule.name,
                    "duration_minutes": (alert.resolved_at - alert.created_at).total_seconds() / 60 if alert.resolved_at else 0
                }
            )

        # Trim history
        if len(self.alert_history) > self.max_history_size:
            self.alert_history = self.alert_history[-self.max_history_size:]

    def acknowledge_alert(self, fingerprint: str) -> bool:
        """Acknowledge an alert."""
        if fingerprint in self.active_alerts:
            alert = self.active_alerts[fingerprint]
            alert.status = AlertStatus.ACKNOWLEDGED
            alert.acknowledged_at = datetime.now()
            return True
        return False

    def suppress_alert(self, rule_name: str, reason: str = "manual") -> bool:
        """Suppress an alert rule."""
        if rule_name in self.rules:
            rule = self.rules[rule_name]
            rule.is_silenced = True

            if PROMETHEUS_AVAILABLE:
                asyncio.create_task(increment_alerts_suppressed(rule_name, reason))

            logger.info(f"Suppressed alert rule: {rule_name} (reason: {reason})")
            return True
        return False

    def unsuppress_alert(self, rule_name: str) -> bool:
        """Unsuppress an alert rule."""
        if rule_name in self.rules:
            rule = self.rules[rule_name]
            rule.is_silenced = False
            logger.info(f"Unsuppressed alert rule: {rule_name}")
            return True
        return False

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary."""
        return {
            "active_alerts": len(self.active_alerts),
            "total_rules": len(self.rules),
            "suppressed_rules": len([r for r in self.rules.values() if r.is_silenced]),
            "alerts_by_severity": {
                severity.value: len([a for a in self.active_alerts.values() if a.rule.severity == severity])
                for severity in AlertSeverity
            },
            "recent_alerts": [
                alert.to_dict()
                for alert in list(self.alert_history)[-10:]  # Last 10 alerts
            ]
        }


# Global alert manager instance
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get global alert manager instance."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


def setup_default_alert_rules(manager: AlertManager) -> None:
    """Set up default alert rules."""

    # API availability alert
    manager.add_rule(AlertRule(
        name="api_unavailable",
        condition=lambda ctx: ctx.get("status_code", 200) >= 500,
        severity=AlertSeverity.CRITICAL,
        channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
        description="API endpoints are returning 5xx errors",
        cooldown_minutes=5
    ))

    # High latency alert
    manager.add_rule(AlertRule(
        name="high_latency",
        condition=lambda ctx: ctx.get("duration_seconds", 0) > 5.0,
        severity=AlertSeverity.WARNING,
        channels=[AlertChannel.SLACK],
        description="API requests taking longer than 5 seconds",
        cooldown_minutes=10
    ))

    # Data staleness alert
    manager.add_rule(AlertRule(
        name="data_stale",
        condition=lambda ctx: ctx.get("hours_old", 0) > 24,
        severity=AlertSeverity.WARNING,
        channels=[AlertChannel.SLACK, AlertChannel.EMAIL],
        description="Data is older than 24 hours",
        cooldown_minutes=60
    ))

    # Great Expectations validation failure
    manager.add_rule(AlertRule(
        name="ge_validation_failed",
        condition=lambda ctx: not ctx.get("validation_success", True),
        severity=AlertSeverity.CRITICAL,
        channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
        description="Great Expectations validation failed",
        cooldown_minutes=15
    ))

    # Canary failure alert
    manager.add_rule(AlertRule(
        name="canary_unhealthy",
        condition=lambda ctx: ctx.get("canary_status") != "healthy",
        severity=AlertSeverity.CRITICAL,
        channels=[AlertChannel.SLACK, AlertChannel.PAGERDUTY],
        description="Canary execution failed or unhealthy",
        cooldown_minutes=5
    ))

    # Database connection pool issues
    manager.add_rule(AlertRule(
        name="db_pool_exhausted",
        condition=lambda ctx: ctx.get("pool_utilization", 0) > 0.95,
        severity=AlertSeverity.WARNING,
        channels=[AlertChannel.SLACK],
        description="Database connection pool utilization > 95%",
        cooldown_minutes=10
    ))

    # Cache hit rate degradation
    manager.add_rule(AlertRule(
        name="cache_hit_rate_low",
        condition=lambda ctx: ctx.get("hit_ratio", 1.0) < 0.8,
        severity=AlertSeverity.WARNING,
        channels=[AlertChannel.SLACK],
        description="Cache hit rate below 80%",
        cooldown_minutes=30
    ))


async def process_api_alert(
    method: str,
    path: str,
    status_code: int,
    duration_seconds: float
) -> None:
    """Process API-related alerts."""
    manager = get_alert_manager()

    context = {
        "method": method,
        "path": path,
        "status_code": status_code,
        "duration_seconds": duration_seconds
    }

    await manager.process_alerts(context)


async def process_data_freshness_alert(
    dataset: str,
    source: str,
    hours_old: float
) -> None:
    """Process data freshness alerts."""
    manager = get_alert_manager()

    context = {
        "dataset": dataset,
        "source": source,
        "hours_old": hours_old
    }

    await manager.process_alerts(context)


async def process_ge_validation_alert(
    dataset: str,
    suite: str,
    validation_success: bool,
    quality_score: float
) -> None:
    """Process Great Expectations validation alerts."""
    manager = get_alert_manager()

    context = {
        "dataset": dataset,
        "suite": suite,
        "validation_success": validation_success,
        "quality_score": quality_score
    }

    await manager.process_alerts(context)


async def process_canary_alert(
    canary_name: str,
    status: str,
    execution_time_seconds: float,
    errors: List[str]
) -> None:
    """Process canary execution alerts."""
    manager = get_alert_manager()

    context = {
        "canary_name": canary_name,
        "canary_status": status,
        "execution_time_seconds": execution_time_seconds,
        "errors": errors
    }

    await manager.process_alerts(context)


async def process_db_pool_alert(
    pool_name: str,
    active_connections: int,
    idle_connections: int,
    total_connections: int
) -> None:
    """Process database connection pool alerts."""
    manager = get_alert_manager()

    pool_utilization = active_connections / max(total_connections, 1)

    context = {
        "pool_name": pool_name,
        "active_connections": active_connections,
        "idle_connections": idle_connections,
        "total_connections": total_connections,
        "pool_utilization": pool_utilization
    }

    await manager.process_alerts(context)


async def process_cache_alert(
    cache_type: str,
    hit_ratio: float,
    memory_usage_bytes: int,
    efficiency_score: float
) -> None:
    """Process cache performance alerts."""
    manager = get_alert_manager()

    context = {
        "cache_type": cache_type,
        "hit_ratio": hit_ratio,
        "memory_usage_bytes": memory_usage_bytes,
        "efficiency_score": efficiency_score
    }

    await manager.process_alerts(context)


# Slack notification handler
async def slack_notification_handler(alert: Alert) -> None:
    """Send alert to Slack."""
    # Implementation would integrate with Slack webhook
    logger.info(f"Slack notification for alert: {alert.rule.name}")


# PagerDuty notification handler
async def pagerduty_notification_handler(alert: Alert) -> None:
    """Send alert to PagerDuty."""
    # Implementation would integrate with PagerDuty API
    logger.info(f"PagerDuty notification for alert: {alert.rule.name}")


# Email notification handler
async def email_notification_handler(alert: Alert) -> None:
    """Send alert via email."""
    # Implementation would integrate with email service
    logger.info(f"Email notification for alert: {alert.rule.name}")


# Webhook notification handler
async def webhook_notification_handler(alert: Alert) -> None:
    """Send alert to webhook."""
    # Implementation would send POST request to webhook URL
    logger.info(f"Webhook notification for alert: {alert.rule.name}")


# Log notification handler
async def log_notification_handler(alert: Alert) -> None:
    """Log alert to system logs."""
    logger.warning(
        f"Alert triggered: {alert.rule.name}",
        extra={
            "severity": alert.rule.severity.value,
            "context": alert.context,
            "channels": [c.value for c in alert.rule.channels]
        }
    )
