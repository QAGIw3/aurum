"""Production-ready database health monitoring with alerting.

This module provides enterprise-grade health monitoring for database connection pools
with integration to alerting systems and comprehensive observability.
"""

from __future__ import annotations

import asyncio
import json
import logging
import smtplib
import time
from dataclasses import dataclass, field
from email.mime.text import MimeText
from typing import Dict, List, Optional, Protocol, Any

from aurum.database import get_connection_manager_registry, PoolMetrics, ConnectionManagerRegistry
from aurum.observability import get_application_metrics

logger = logging.getLogger(__name__)


@dataclass
class AlertConfig:
    """Configuration for alerting."""

    enabled: bool = True
    smtp_server: Optional[str] = None
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    from_email: str = "aurum@noreply.com"
    to_emails: List[str] = field(default_factory=lambda: ["admin@aurum.com"])
    slack_webhook: Optional[str] = None
    pagerduty_routing_key: Optional[str] = None


@dataclass
class AlertRule:
    """Alert rule configuration."""

    name: str
    condition: str  # "pool_utilization > 0.8", "response_time_ms > 5000", etc.
    severity: str = "warning"  # warning, error, critical
    cooldown_seconds: int = 300  # Minimum time between alerts


class AlertHandler(Protocol):
    """Protocol for alert handlers."""

    async def send_alert(self, rule: AlertRule, pool_name: str, metrics: PoolMetrics, message: str) -> bool:
        """Send an alert."""
        ...


class EmailAlertHandler:
    """Email alert handler for database monitoring."""

    def __init__(self, config: AlertConfig):
        self.config = config

    async def send_alert(self, rule: AlertRule, pool_name: str, metrics: PoolMetrics, message: str) -> bool:
        """Send alert via email."""
        if not self.config.enabled or not self.config.smtp_server:
            return False

        try:
            msg = MimeText(f"""
Database Alert: {rule.name}

Pool: {pool_name}
Severity: {rule.severity}
Condition: {rule.condition}

Metrics:
- Active Connections: {metrics.active_connections}
- Idle Connections: {metrics.idle_connections}
- Pool Utilization: {metrics.pool_utilization:.2%}
- Response Time: {metrics.acquire_timeout_seconds:.2f}s

Message: {message}

Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}
""")

            msg['Subject'] = f"[Aurum DB Alert] {rule.severity.upper()}: {rule.name} - {pool_name}"
            msg['From'] = self.config.from_email
            msg['To'] = ", ".join(self.config.to_emails)

            with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                if self.config.smtp_username and self.config.smtp_password:
                    server.starttls()
                    server.login(self.config.smtp_username, self.config.smtp_password)
                server.sendmail(self.config.from_email, self.config.to_emails, msg.as_string())

            logger.info(f"Sent email alert for {rule.name} on pool {pool_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False


class SlackAlertHandler:
    """Slack alert handler for database monitoring."""

    def __init__(self, config: AlertConfig):
        self.config = config

    async def send_alert(self, rule: AlertRule, pool_name: str, metrics: PoolMetrics, message: str) -> bool:
        """Send alert via Slack webhook."""
        if not self.config.enabled or not self.config.slack_webhook:
            return False

        try:
            import aiohttp

            payload = {
                "text": f"🚨 *Database Alert: {rule.severity.upper()}*",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"🚨 {rule.name}"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Pool:* {pool_name}"},
                            {"type": "mrkdwn", "text": f"*Severity:* {rule.severity}"},
                            {"type": "mrkdwn", "text": f"*Active Connections:* {metrics.active_connections}"},
                            {"type": "mrkdwn", "text": f"*Pool Utilization:* {metrics.pool_utilization:.1%}"},
                            {"type": "mrkdwn", "text": f"*Response Time:* {metrics.acquire_timeout_seconds:.2f}s"},
                        ]
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*{message}*"
                        }
                    }
                ]
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(self.config.slack_webhook, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"Sent Slack alert for {rule.name} on pool {pool_name}")
                        return True
                    else:
                        logger.error(f"Slack alert failed: {response.status}")
                        return False

        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False


class PagerDutyAlertHandler:
    """PagerDuty alert handler for critical database issues."""

    def __init__(self, config: AlertConfig):
        self.config = config

    async def send_alert(self, rule: AlertRule, pool_name: str, metrics: PoolMetrics, message: str) -> bool:
        """Send alert via PagerDuty."""
        if not self.config.enabled or not self.config.pagerduty_routing_key:
            return False

        try:
            import aiohttp

            payload = {
                "routing_key": self.config.pagerduty_routing_key,
                "event_action": "trigger",
                "payload": {
                    "summary": f"Database Alert: {rule.name} - {pool_name}",
                    "source": pool_name,
                    "severity": rule.severity,
                    "component": "database",
                    "group": "infrastructure",
                    "class": "database_connection_pool",
                    "custom_details": {
                        "pool_name": pool_name,
                        "active_connections": metrics.active_connections,
                        "pool_utilization": metrics.pool_utilization,
                        "response_time_seconds": metrics.acquire_timeout_seconds,
                        "message": message,
                    }
                }
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://events.pagerduty.com/v2/enqueue",
                    json=payload
                ) as response:
                    if response.status == 202:
                        logger.info(f"Sent PagerDuty alert for {rule.name} on pool {pool_name}")
                        return True
                    else:
                        logger.error(f"PagerDuty alert failed: {response.status}")
                        return False

        except Exception as e:
            logger.error(f"Failed to send PagerDuty alert: {e}")
            return False


class ProductionDatabaseMonitor:
    """Production-ready database health monitor with alerting."""

    def __init__(
        self,
        alert_config: Optional[AlertConfig] = None,
        alert_rules: Optional[List[AlertRule]] = None,
        registry: Optional[ConnectionManagerRegistry] = None
    ):
        self.alert_config = alert_config or AlertConfig()
        self.registry = registry or get_connection_manager_registry()

        # Default alert rules
        self.alert_rules = alert_rules or [
            AlertRule(
                name="high_pool_utilization",
                condition="pool_utilization > 0.8",
                severity="warning"
            ),
            AlertRule(
                name="critical_pool_utilization",
                condition="pool_utilization > 0.95",
                severity="critical"
            ),
            AlertRule(
                name="slow_response_time",
                condition="response_time_ms > 5000",
                severity="warning"
            ),
            AlertRule(
                name="connection_exhaustion",
                condition="max_connections == total_connections and active_connections > 0",
                severity="error"
            ),
        ]

        # Alert handlers
        self.alert_handlers: List[AlertHandler] = []
        self._setup_alert_handlers()

        # Alert state tracking
        self._last_alert_times: Dict[str, float] = {}
        self._alert_cooldowns: Dict[str, float] = {}

        # Get application metrics for recording
        self.app_metrics = get_application_metrics()

    def _setup_alert_handlers(self) -> None:
        """Setup alert handlers based on configuration."""
        if self.alert_config.smtp_server:
            self.alert_handlers.append(EmailAlertHandler(self.alert_config))

        if self.alert_config.slack_webhook:
            self.alert_handlers.append(SlackAlertHandler(self.alert_config))

        if self.alert_config.pagerduty_routing_key:
            self.alert_handlers.append(PagerDutyAlertHandler(self.alert_config))

    async def start_monitoring(self, interval_seconds: float = 30.0) -> None:
        """Start continuous health monitoring with alerting."""
        logger.info("Starting production database monitoring")

        while True:
            try:
                await self._monitoring_cycle()
                await asyncio.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Error in monitoring cycle: {e}")
                await asyncio.sleep(interval_seconds)

    async def _monitoring_cycle(self) -> None:
        """Run a single monitoring cycle."""
        try:
            # Get all pool metrics
            pools = await self.registry.get_all_pools()
            all_metrics = {}

            for pool_name, pool in pools.items():
                try:
                    metrics = await pool.get_pool_metrics()
                    all_metrics[pool_name] = metrics
                except Exception as e:
                    logger.error(f"Error getting metrics for pool {pool_name}: {e}")
                    continue

            # Check alert rules
            await self._check_alert_rules(all_metrics)

            # Record metrics for observability
            self._record_monitoring_metrics(all_metrics)

        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")

    async def _check_alert_rules(self, pool_metrics: Dict[str, PoolMetrics]) -> None:
        """Check alert rules against current metrics."""
        current_time = time.time()

        for pool_name, metrics in pool_metrics.items():
            for rule in self.alert_rules:
                # Check if alert is on cooldown
                alert_key = f"{pool_name}:{rule.name}"
                last_alert_time = self._last_alert_times.get(alert_key, 0)
                cooldown_end = last_alert_time + rule.cooldown_seconds

                if current_time < cooldown_end:
                    continue

                # Evaluate condition
                if self._evaluate_condition(rule.condition, metrics):
                    # Send alert
                    message = f"Alert condition '{rule.condition}' triggered for pool {pool_name}"
                    await self._send_alert(rule, pool_name, metrics, message)

                    # Update last alert time
                    self._last_alert_times[alert_key] = current_time

    async def _send_alert(self, rule: AlertRule, pool_name: str, metrics: PoolMetrics, message: str) -> None:
        """Send alert through all configured handlers."""
        for handler in self.alert_handlers:
            try:
                await handler.send_alert(rule, pool_name, metrics, message)
            except Exception as e:
                logger.error(f"Error sending alert via {handler.__class__.__name__}: {e}")

    def _evaluate_condition(self, condition: str, metrics: PoolMetrics) -> bool:
        """Evaluate an alert condition against metrics."""
        try:
            # Simple condition evaluation (can be enhanced with more complex expressions)
            if "pool_utilization" in condition:
                threshold = float(condition.split(">")[1].strip())
                return metrics.pool_utilization > threshold

            if "response_time_ms" in condition:
                threshold = float(condition.split(">")[1].strip())
                return metrics.acquire_timeout_seconds * 1000 > threshold

            if "max_connections" in condition and "total_connections" in condition:
                return metrics.max_connections == metrics.total_connections and metrics.active_connections > 0

            return False

        except Exception as e:
            logger.error(f"Error evaluating condition '{condition}': {e}")
            return False

    def _record_monitoring_metrics(self, pool_metrics: Dict[str, PoolMetrics]) -> None:
        """Record monitoring metrics for observability."""
        for pool_name, metrics in pool_metrics.items():
            # Record pool utilization as gauge
            self.app_metrics.update_db_connection_pool_size(pool_name, metrics.total_connections)

            # Record response time as histogram
            self.app_metrics.record_db_operation("health_check", pool_name, metrics.acquire_timeout_seconds)

    async def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status."""
        pools = await self.registry.get_all_pools()
        pool_metrics = {}

        for pool_name, pool in pools.items():
            try:
                metrics = await pool.get_pool_metrics()
                pool_metrics[pool_name] = metrics.to_dict()
            except Exception as e:
                logger.error(f"Error getting metrics for {pool_name}: {e}")
                pool_metrics[pool_name] = {"error": str(e)}

        return {
            "monitoring_active": True,
            "alert_handlers_count": len(self.alert_handlers),
            "alert_rules_count": len(self.alert_rules),
            "pools_monitored": len(pools),
            "pool_metrics": pool_metrics,
            "last_check": time.time(),
        }


# Global production monitor instance
_production_monitor: Optional[ProductionDatabaseMonitor] = None


def get_production_monitor(
    alert_config: Optional[AlertConfig] = None
) -> ProductionDatabaseMonitor:
    """Get the global production database monitor."""
    global _production_monitor
    if _production_monitor is None:
        _production_monitor = ProductionDatabaseMonitor(alert_config)
    return _production_monitor


async def start_production_monitoring(
    alert_config: Optional[AlertConfig] = None,
    interval_seconds: float = 30.0
) -> None:
    """Start production database monitoring."""
    monitor = get_production_monitor(alert_config)
    await monitor.start_monitoring(interval_seconds)


def configure_alerting(
    smtp_server: Optional[str] = None,
    smtp_username: Optional[str] = None,
    smtp_password: Optional[str] = None,
    slack_webhook: Optional[str] = None,
    pagerduty_routing_key: Optional[str] = None,
    to_emails: Optional[List[str]] = None,
) -> AlertConfig:
    """Configure alerting settings."""
    return AlertConfig(
        enabled=True,
        smtp_server=smtp_server,
        smtp_username=smtp_username,
        smtp_password=smtp_password,
        slack_webhook=slack_webhook,
        pagerduty_routing_key=pagerduty_routing_key,
        to_emails=to_emails or ["admin@aurum.com"],
    )
