"""Staleness-specific alert management."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable, Any

from ..logging import StructuredLogger, LogLevel, create_logger


class StalenessAlertSeverity(str, Enum):
    """Staleness alert severity levels."""
    LOW = "LOW"           # Fresh data with minor issues
    MEDIUM = "MEDIUM"     # Stale data within grace period
    HIGH = "HIGH"         # Stale data beyond grace period
    CRITICAL = "CRITICAL" # Critically stale data


@dataclass
class StalenessAlertRule:
    """Rule for staleness alerts."""

    name: str
    condition: str  # Expression to evaluate
    severity: StalenessAlertSeverity
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
            self.tags = []


@dataclass
class StalenessAlert:
    """Staleness alert notification."""

    id: str
    rule_name: str
    severity: StalenessAlertSeverity
    dataset: str
    title: str
    message: str
    timestamp: str

    # Staleness information
    hours_since_update: float
    expected_frequency_hours: int
    grace_period_hours: int

    # Alert metadata
    channels: List[str]
    recipients: List[str]
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class StalenessAlertManager:
    """Manage alerts for dataset staleness."""

    def __init__(self):
        """Initialize staleness alert manager."""
        self.logger = create_logger(
            source_name="staleness_alert_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.staleness.alerts",
            dataset="staleness_alerting"
        )

        # Alert rules
        self.alert_rules = self._create_default_alert_rules()

        # State tracking
        self.active_alerts: Dict[str, StalenessAlert] = {}
        self.alert_history: List[StalenessAlert] = []
        self.last_alerts: Dict[str, datetime] = {}

    def _create_default_alert_rules(self) -> List[StalenessAlertRule]:
        """Create default staleness alert rules."""
        return [
            StalenessAlertRule(
                name="critical_staleness",
                condition="staleness_level == 'CRITICAL'",
                severity=StalenessAlertSeverity.CRITICAL,
                cooldown_minutes=5,
                channels=["email", "pagerduty"],
                description="Critical dataset staleness requiring immediate attention",
                tags=["staleness", "critical"]
            ),
            StalenessAlertRule(
                name="high_staleness",
                condition="staleness_level == 'STALE' and hours_since_update > grace_period_hours",
                severity=StalenessAlertSeverity.HIGH,
                cooldown_minutes=15,
                channels=["email", "slack"],
                description="High severity staleness beyond grace period",
                tags=["staleness", "high"]
            ),
            StalenessAlertRule(
                name="data_freshness_monitoring",
                condition="staleness_level == 'FRESH' and consecutive_checks > 10",
                severity=StalenessAlertSeverity.LOW,
                cooldown_minutes=60,
                channels=["monitoring"],
                description="Data freshness monitoring - dataset is consistently fresh",
                tags=["monitoring", "fresh"]
            ),
            StalenessAlertRule(
                name="watermark_missing",
                condition="last_watermark is None",
                severity=StalenessAlertSeverity.HIGH,
                cooldown_minutes=30,
                channels=["email", "slack"],
                description="Missing watermark - data ingestion may be broken",
                tags=["watermark", "missing"]
            )
        ]

    def process_staleness(
        self,
        dataset: str,
        staleness_info: Dict[str, Any]
    ) -> None:
        """Process staleness information and generate alerts if needed.

        Args:
            dataset: Dataset name
            staleness_info: Staleness information dictionary
        """
        self.logger.log(
            LogLevel.INFO,
            f"Processing staleness for {dataset}",
            "staleness_processing",
            dataset=dataset,
            staleness_level=staleness_info.get("staleness_level"),
            hours_since_update=staleness_info.get("hours_since_update")
        )

        # Evaluate alert rules
        triggered_alerts = []

        for rule in self.alert_rules:
            if not rule.enabled:
                continue

            try:
                if self._evaluate_rule_condition(rule, staleness_info):
                    alert = self._create_staleness_alert(rule, dataset, staleness_info)
                    triggered_alerts.append((rule, alert))

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error evaluating alert rule {rule.name}: {e}",
                    "alert_rule_error",
                    rule_name=rule.name,
                    dataset=dataset,
                    error=str(e)
                )

        # Send triggered alerts
        for rule, alert in triggered_alerts:
            if self._should_send_alert(rule, dataset):
                self._send_staleness_alert(rule, alert)
                self._record_alert(alert)

    def _evaluate_rule_condition(self, rule: StalenessAlertRule, staleness_info: Dict[str, Any]) -> bool:
        """Evaluate an alert rule condition.

        Args:
            rule: Alert rule to evaluate
            staleness_info: Staleness information

        Returns:
            True if condition is met
        """
        try:
            # Simple condition evaluator - could be enhanced with more complex expressions
            if "staleness_level" in rule.condition:
                actual_level = staleness_info.get("staleness_level", "UNKNOWN")
                return actual_level in rule.condition

            if "hours_since_update" in rule.condition:
                hours = staleness_info.get("hours_since_update", 0)
                if ">" in rule.condition:
                    threshold = float(rule.condition.split(">")[1])
                    return hours > threshold
                if "<" in rule.condition:
                    threshold = float(rule.condition.split("<")[1])
                    return hours < threshold

            if "last_watermark" in rule.condition:
                has_watermark = staleness_info.get("last_watermark") is not None
                return "None" if not has_watermark else "not None"

            return False

        except Exception:
            return False

    def _create_staleness_alert(
        self,
        rule: StalenessAlertRule,
        dataset: str,
        staleness_info: Dict[str, Any]
    ) -> StalenessAlert:
        """Create a staleness alert.

        Args:
            rule: Alert rule that triggered
            dataset: Dataset name
            staleness_info: Staleness information

        Returns:
            StalenessAlert instance
        """
        alert_id = f"{rule.name}_{dataset}_{int(time.time())}"

        # Create alert title and message
        staleness_level = staleness_info.get("staleness_level", "UNKNOWN")
        hours_since_update = staleness_info.get("hours_since_update", 0)

        title = f"[{rule.severity.value}] Staleness Alert: {dataset}"
        message = self._format_staleness_message(rule, dataset, staleness_info)

        return StalenessAlert(
            id=alert_id,
            rule_name=rule.name,
            severity=rule.severity,
            dataset=dataset,
            title=title,
            message=message,
            timestamp=datetime.now().isoformat(),
            hours_since_update=hours_since_update,
            expected_frequency_hours=staleness_info.get("expected_frequency_hours", 24),
            grace_period_hours=staleness_info.get("grace_period_hours", 1),
            channels=rule.channels,
            recipients=rule.recipients,
            metadata=staleness_info
        )

    def _format_staleness_message(
        self,
        rule: StalenessAlertRule,
        dataset: str,
        staleness_info: Dict[str, Any]
    ) -> str:
        """Format staleness alert message.

        Args:
            rule: Alert rule
            dataset: Dataset name
            staleness_info: Staleness information

        Returns:
            Formatted message
        """
        hours = staleness_info.get("hours_since_update", 0)
        staleness_level = staleness_info.get("staleness_level", "UNKNOWN")

        message = f"""
Dataset: {dataset}
Staleness Level: {staleness_level}
Hours Since Update: {hours".2f"}

"""

        if "last_watermark" in staleness_info and staleness_info["last_watermark"]:
            message += f"Last Watermark: {staleness_info['last_watermark']}\n"

        if "expected_frequency_hours" in staleness_info:
            message += f"Expected Frequency: {staleness_info['expected_frequency_hours']} hours\n"

        if "grace_period_hours" in staleness_info:
            message += f"Grace Period: {staleness_info['grace_period_hours']} hours\n"

        message += f"\nAlert Rule: {rule.description}"
        message += f"\nTriggered: {datetime.now().isoformat()}"

        return message.strip()

    def _should_send_alert(self, rule: StalenessAlertRule, dataset: str) -> bool:
        """Check if alert should be sent based on cooldown.

        Args:
            rule: Alert rule
            dataset: Dataset name

        Returns:
            True if alert should be sent
        """
        alert_key = f"{rule.name}:{dataset}"

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

    def _send_staleness_alert(self, rule: StalenessAlertRule, alert: StalenessAlert) -> None:
        """Send staleness alert via configured channels.

        Args:
            rule: Alert rule
            alert: Alert to send
        """
        self.logger.log(
            LogLevel.INFO,
            f"Sending staleness alert {alert.id} for {alert.dataset}",
            "staleness_alert_send",
            alert_id=alert.id,
            dataset=alert.dataset,
            severity=alert.severity.value,
            channels=alert.channels
        )

        # In a real implementation, this would send to actual alert channels
        # For now, just log the alert
        self.logger.log(
            LogLevel.WARN if alert.severity in ["HIGH", "CRITICAL"] else LogLevel.INFO,
            f"STALENESS ALERT: {alert.title}\n{alert.message}",
            "alert_content",
            alert_id=alert.id,
            dataset=alert.dataset,
            severity=alert.severity.value
        )

    def _record_alert(self, alert: StalenessAlert) -> None:
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
            f"Recorded staleness alert {alert.id}",
            "staleness_alert_recorded",
            alert_id=alert.id,
            dataset=alert.dataset,
            severity=alert.severity.value
        )

    def get_active_staleness_alerts(self) -> List[StalenessAlert]:
        """Get currently active staleness alerts.

        Returns:
            List of active alerts
        """
        return list(self.active_alerts.values())

    def get_staleness_alert_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get staleness alert summary.

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
            "by_dataset": {},
            "by_rule": {},
            "recent_alerts": []
        }

        # Count by severity
        for alert in recent_alerts:
            severity = alert.severity.value
            summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + 1

        # Count by dataset
        for alert in recent_alerts:
            dataset = alert.dataset
            summary["by_dataset"][dataset] = summary["by_dataset"].get(dataset, 0) + 1

        # Count by rule
        for alert in recent_alerts:
            rule = alert.rule_name
            summary["by_rule"][rule] = summary["by_rule"].get(rule, 0) + 1

        # Recent alerts (last 10)
        summary["recent_alerts"] = [
            {
                "id": alert.id,
                "dataset": alert.dataset,
                "severity": alert.severity.value,
                "title": alert.title,
                "timestamp": alert.timestamp
            }
            for alert in recent_alerts[-10:]
        ]

        return summary
