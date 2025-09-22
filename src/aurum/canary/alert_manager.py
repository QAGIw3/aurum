"""Canary-specific alert management."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable, Any

from ..logging import StructuredLogger, LogLevel, create_logger


class CanaryAlertSeverity(str, Enum):
    """Canary alert severity levels."""
    LOW = "LOW"           # Minor issues or warnings
    MEDIUM = "MEDIUM"     # Data quality issues or degraded performance
    HIGH = "HIGH"         # API failures or timeouts
    CRITICAL = "CRITICAL" # Multiple failures or critical API breaks


@dataclass
class CanaryAlertRule:
    """Rule for canary alerts."""

    name: str
    condition: str  # Expression to evaluate
    severity: CanaryAlertSeverity
    cooldown_minutes: int = 15
    enabled: bool = True

    # Alert configuration
    channels: List[str] = None  # Email, Slack, PagerDuty, etc.
    recipients: List[str] = None  # Specific recipients for this rule

    # Additional metadata
    description: str = ""
    tags: List[str] = None

    def __post_init__(self):
        if self.channels is None:
            self.channels = ["email"]
        if self.recipients is None:
            self.recipients = []
        if self.tags is None:
            self.tags = ["canary"]


@dataclass
class CanaryAlert:
    """Canary alert notification."""

    id: str
    rule_name: str
    severity: CanaryAlertSeverity
    canary_name: str
    source: str
    title: str
    message: str
    timestamp: str

    # Canary-specific information
    status: str
    consecutive_failures: int
    last_success: Optional[str]
    last_failure: Optional[str]

    # Alert metadata
    channels: List[str]
    recipients: List[str]
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class CanaryAlertManager:
    """Manage alerts for canary dataset failures."""

    def __init__(self):
        """Initialize canary alert manager."""
        self.logger = create_logger(
            source_name="canary_alert_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.canary.alerts",
            dataset="canary_alerting"
        )

        # Alert rules
        self.alert_rules = self._create_default_alert_rules()

        # State tracking
        self.active_alerts: Dict[str, CanaryAlert] = {}
        self.alert_history: List[CanaryAlert] = []
        self.last_alerts: Dict[str, datetime] = {}

    def _create_default_alert_rules(self) -> List[CanaryAlertRule]:
        """Create default canary alert rules."""
        return [
            CanaryAlertRule(
                name="canary_failure_critical",
                condition="consecutive_failures >= 3 and status == 'ERROR'",
                severity=CanaryAlertSeverity.CRITICAL,
                cooldown_minutes=5,
                channels=["email", "pagerduty"],
                description="Critical canary failure with multiple consecutive errors",
                tags=["canary", "critical", "failure"]
            ),
            CanaryAlertRule(
                name="canary_timeout",
                condition="status == 'TIMEOUT'",
                severity=CanaryAlertSeverity.HIGH,
                cooldown_minutes=10,
                channels=["email", "slack"],
                description="Canary execution timed out",
                tags=["canary", "timeout"]
            ),
            CanaryAlertRule(
                name="api_unhealthy",
                condition="status == 'UNHEALTHY'",
                severity=CanaryAlertSeverity.HIGH,
                cooldown_minutes=15,
                channels=["email", "slack"],
                description="API health check failed for canary",
                tags=["canary", "api", "unhealthy"]
            ),
            CanaryAlertRule(
                name="canary_degraded",
                condition="status == 'HEALTHY' and len(warnings) > 0",
                severity=CanaryAlertSeverity.MEDIUM,
                cooldown_minutes=30,
                channels=["email"],
                description="Canary running but with warnings",
                tags=["canary", "degraded"]
            ),
            CanaryAlertRule(
                name="canary_recovery",
                condition="status == 'HEALTHY' and consecutive_failures == 0",
                severity=CanaryAlertSeverity.LOW,
                cooldown_minutes=60,
                channels=["email"],
                description="Canary recovered from previous failures",
                tags=["canary", "recovery"]
            )
        ]

    def process_canary_result(self, canary_name: str, result: Dict[str, Any]) -> None:
        """Process canary result and generate alerts if needed.

        Args:
            canary_name: Name of the canary
            result: Canary execution result
        """
        self.logger.log(
            LogLevel.INFO,
            f"Processing canary result: {canary_name}",
            "canary_result_processing",
            canary_name=canary_name,
            status=result.get("status"),
            execution_time_seconds=result.get("execution_time_seconds")
        )

        # Get canary for context
        canary = None
        if hasattr(self, '_canary_manager'):
            canary = self._canary_manager.get_canary(canary_name)

        # Evaluate alert rules
        triggered_alerts = []

        for rule in self.alert_rules:
            if not rule.enabled:
                continue

            try:
                if self._evaluate_rule_condition(rule, result, canary):
                    alert = self._create_canary_alert(rule, canary_name, result, canary)
                    triggered_alerts.append((rule, alert))

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error evaluating alert rule {rule.name}: {e}",
                    "alert_rule_error",
                    rule_name=rule.name,
                    canary_name=canary_name,
                    error=str(e)
                )

        # Send triggered alerts
        for rule, alert in triggered_alerts:
            if self._should_send_alert(rule, canary_name):
                self._send_canary_alert(rule, alert)
                self._record_alert(alert)

    def _evaluate_rule_condition(self, rule: CanaryAlertRule, result: Dict[str, Any], canary: Any) -> bool:
        """Evaluate an alert rule condition.

        Args:
            rule: Alert rule to evaluate
            result: Canary result
            canary: Canary object (if available)

        Returns:
            True if condition is met
        """
        try:
            # Simple condition evaluator
            status = result.get("status")
            consecutive_failures = result.get("consecutive_failures", 0)
            warnings = result.get("warnings", [])

            # Status-based conditions
            if "status == 'ERROR'" in rule.condition and status == "ERROR":
                return consecutive_failures >= 3
            if "status == 'TIMEOUT'" in rule.condition and status == "TIMEOUT":
                return True
            if "status == 'UNHEALTHY'" in rule.condition and status == "UNHEALTHY":
                return True
            if "status == 'HEALTHY'" in rule.condition and status == "HEALTHY":
                if "len(warnings) > 0" in rule.condition and len(warnings) > 0:
                    return True
                if "consecutive_failures == 0" in rule.condition:
                    return consecutive_failures == 0

            return False

        except Exception:
            return False

    def _create_canary_alert(
        self,
        rule: CanaryAlertRule,
        canary_name: str,
        result: Dict[str, Any],
        canary: Any
    ) -> CanaryAlert:
        """Create a canary alert.

        Args:
            rule: Alert rule that triggered
            canary_name: Canary name
            result: Canary result
            canary: Canary object (if available)

        Returns:
            CanaryAlert instance
        """
        alert_id = f"{rule.name}_{canary_name}_{int(time.time())}"

        # Create alert title and message
        status = result.get("status", "UNKNOWN")
        consecutive_failures = result.get("consecutive_failures", 0)

        title = f"[{rule.severity.value}] Canary Alert: {canary_name}"
        message = self._format_canary_message(rule, canary_name, result, canary)

        # Get source from canary if available
        source = "unknown"
        if canary:
            source = canary.source

        return CanaryAlert(
            id=alert_id,
            rule_name=rule.name,
            severity=rule.severity,
            canary_name=canary_name,
            source=source,
            title=title,
            message=message,
            timestamp=datetime.now().isoformat(),
            status=status,
            consecutive_failures=consecutive_failures,
            last_success=result.get("last_success"),
            last_failure=result.get("last_failure"),
            channels=rule.channels,
            recipients=rule.recipients,
            metadata=result
        )

    def _format_canary_message(
        self,
        rule: CanaryAlertRule,
        canary_name: str,
        result: Dict[str, Any],
        canary: Any
    ) -> str:
        """Format canary alert message.

        Args:
            rule: Alert rule
            canary_name: Canary name
            result: Canary result
            canary: Canary object

        Returns:
            Formatted message
        """
        status = result.get("status", "UNKNOWN")
        execution_time = result.get("execution_time_seconds", 0)
        consecutive_failures = result.get("consecutive_failures", 0)
        errors = result.get("errors", [])
        warnings = result.get("warnings", [])

        message = f"""
Canary: {canary_name}
Status: {status}
Execution Time: {execution_time:.2f} seconds
Consecutive Failures: {consecutive_failures}

"""

        if errors:
            message += f"Errors:\n"
            for error in errors[:3]:  # Limit to first 3 errors
                message += f"  - {error}\n"

        if warnings:
            message += f"Warnings:\n"
            for warning in warnings[:3]:  # Limit to first 3 warnings
                message += f"  - {warning}\n"

        if canary and hasattr(canary, 'config'):
            message += f"\nAPI Endpoint: {canary.config.api_endpoint}"
            message += f"\nSource: {canary.source}"
            message += f"\nDataset: {canary.dataset}"

        message += f"\nAlert Rule: {rule.description}"
        message += f"\nTriggered: {datetime.now().isoformat()}"

        return message.strip()

    def _should_send_alert(self, rule: CanaryAlertRule, canary_name: str) -> bool:
        """Check if alert should be sent based on cooldown.

        Args:
            rule: Alert rule
            canary_name: Canary name

        Returns:
            True if alert should be sent
        """
        alert_key = f"{rule.name}:{canary_name}"

        now = datetime.now()
        last_alert = self.last_alerts.get(alert_key)

        if last_alert is None:
            self.last_alerts[alert_key] = now
            return True

        time_since_last_alert = now - last_alert
        if time_since_last_alert.total_seconds() > (rule.cooldown_minutes * 60):
            self.last_alerts[alert_key] = now
            return True

        return False

    def _send_canary_alert(self, rule: CanaryAlertRule, alert: CanaryAlert) -> None:
        """Send canary alert via configured channels.

        Args:
            rule: Alert rule
            alert: Alert to send
        """
        self.logger.log(
            LogLevel.INFO,
            f"Sending canary alert {alert.id} for {alert.canary_name}",
            "canary_alert_send",
            alert_id=alert.id,
            canary_name=alert.canary_name,
            severity=alert.severity.value,
            channels=alert.channels
        )

        # In a real implementation, this would send to actual alert channels
        # For now, just log the alert
        self.logger.log(
            LogLevel.WARN if alert.severity in ["HIGH", "CRITICAL"] else LogLevel.INFO,
            f"CANARY ALERT: {alert.title}\n{alert.message}",
            "alert_content",
            alert_id=alert.id,
            canary_name=alert.canary_name,
            severity=alert.severity.value
        )

    def _record_alert(self, alert: CanaryAlert) -> None:
        """Record alert in history.

        Args:
            alert: Alert to record
        """
        self.alert_history.append(alert)
        self.active_alerts[alert.id] = alert

        # Keep only recent history (last 1000 alerts)
        if len(self.alert_history) > 1000:
            self.alert_history = self.alert_history[-1000:]

        self.logger.log(
            LogLevel.INFO,
            f"Recorded canary alert {alert.id}",
            "canary_alert_recorded",
            alert_id=alert.id,
            canary_name=alert.canary_name,
            severity=alert.severity.value
        )

    def get_active_canary_alerts(self) -> List[CanaryAlert]:
        """Get currently active canary alerts.

        Returns:
            List of active alerts
        """
        return list(self.active_alerts.values())

    def get_canary_alert_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get canary alert summary.

        Args:
            hours: Time period in hours

        Returns:
            Dictionary with alert summary
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)

        recent_alerts = [
            alert for alert in self.alert_history
            if datetime.fromisoformat(alert.timestamp) > cutoff_time
        ]

        summary = {
            "total_alerts": len(recent_alerts),
            "by_severity": {},
            "by_canary": {},
            "by_rule": {},
            "recent_alerts": []
        }

        # Count by severity
        for alert in recent_alerts:
            severity = alert.severity.value
            summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + 1

        # Count by canary
        for alert in recent_alerts:
            canary = alert.canary_name
            summary["by_canary"][canary] = summary["by_canary"].get(canary, 0) + 1

        # Count by rule
        for alert in recent_alerts:
            rule = alert.rule_name
            summary["by_rule"][rule] = summary["by_rule"].get(rule, 0) + 1

        # Recent alerts (last 10)
        summary["recent_alerts"] = [
            {
                "id": alert.id,
                "canary_name": alert.canary_name,
                "severity": alert.severity.value,
                "title": alert.title,
                "timestamp": alert.timestamp
            }
            for alert in recent_alerts[-10:]
        ]

        return summary
