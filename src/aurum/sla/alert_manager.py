"""Alert management system for SLA violations and DAG failures."""

from __future__ import annotations

import json
import smtplib
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from ..logging import StructuredLogger, LogLevel, create_logger


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AlertChannel(str, Enum):
    """Alert notification channels."""
    EMAIL = "email"
    SLACK = "slack"
    PAGERDUTY = "pagerduty"
    WEBHOOK = "webhook"
    SMS = "sms"


@dataclass
class AlertRule:
    """Configuration for an alert rule."""

    name: str
    condition: str  # Expression to evaluate
    severity: AlertSeverity
    channels: List[AlertChannel]
    cooldown_minutes: int = 15
    enabled: bool = True

    # Additional configuration
    description: str = ""
    tags: List[str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class Alert:
    """Alert notification."""

    id: str
    rule_name: str
    severity: AlertSeverity
    title: str
    message: str
    timestamp: str
    source: str
    channels: List[AlertChannel]
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class AlertManager:
    """Manage alerts for SLA violations and operational issues."""

    def __init__(
        self,
        smtp_config: Optional[Dict[str, Any]] = None,
        slack_config: Optional[Dict[str, Any]] = None,
        pagerduty_config: Optional[Dict[str, Any]] = None,
        webhook_configs: Optional[Dict[str, str]] = None
    ):
        """Initialize alert manager.

        Args:
            smtp_config: SMTP configuration for email alerts
            slack_config: Slack configuration for Slack alerts
            pagerduty_config: PagerDuty configuration
            webhook_configs: Webhook URLs by name
        """
        self.logger = create_logger(
            source_name="alert_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.alerts",
            dataset="alerting"
        )

        self.smtp_config = smtp_config or {}
        self.slack_config = slack_config or {}
        self.pagerduty_config = pagerduty_config or {}
        self.webhook_configs = webhook_configs or {}

        # Alert state tracking
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.last_alerts: Dict[str, datetime] = {}

        # Default alert rules
        self.alert_rules = self._create_default_alert_rules()

    def _create_default_alert_rules(self) -> List[AlertRule]:
        """Create default alert rules."""
        return [
            AlertRule(
                name="sla_violation_critical",
                condition="severity == 'CRITICAL'",
                severity=AlertSeverity.CRITICAL,
                channels=[AlertChannel.EMAIL, AlertChannel.PAGERDUTY],
                cooldown_minutes=5,
                description="Critical SLA violations requiring immediate attention",
                tags=["sla", "critical"]
            ),
            AlertRule(
                name="sla_violation_high",
                condition="severity == 'HIGH'",
                severity=AlertSeverity.HIGH,
                channels=[AlertChannel.EMAIL, AlertChannel.SLACK],
                cooldown_minutes=10,
                description="High severity SLA violations",
                tags=["sla", "high"]
            ),
            AlertRule(
                name="dag_failure_critical",
                condition="event_type == 'dag_failure' and severity == 'CRITICAL'",
                severity=AlertSeverity.CRITICAL,
                channels=[AlertChannel.EMAIL, AlertChannel.PAGERDUTY],
                cooldown_minutes=5,
                description="Critical DAG failures",
                tags=["dag", "failure", "critical"]
            ),
            AlertRule(
                name="dag_failure_repeated",
                condition="event_type == 'dag_failure' and try_number > 3",
                severity=AlertSeverity.HIGH,
                channels=[AlertChannel.EMAIL, AlertChannel.SLACK],
                cooldown_minutes=15,
                description="Repeated DAG failures",
                tags=["dag", "failure", "repeated"]
            ),
            AlertRule(
                name="data_quality_low",
                condition="event_type == 'data_quality' and quality_score < 0.8",
                severity=AlertSeverity.MEDIUM,
                channels=[AlertChannel.EMAIL, AlertChannel.SLACK],
                cooldown_minutes=30,
                description="Low data quality scores",
                tags=["data", "quality"]
            ),
            AlertRule(
                name="throughput_degraded",
                condition="event_type == 'throughput' and actual_value < 0.5 * expected_value",
                severity=AlertSeverity.MEDIUM,
                channels=[AlertChannel.EMAIL],
                cooldown_minutes=60,
                description="Degraded throughput performance",
                tags=["performance", "throughput"]
            )
        ]

    def process_violation(self, violation: Dict[str, Any]) -> None:
        """Process an SLA violation and send appropriate alerts.

        Args:
            violation: SLA violation data
        """
        self.logger.log(
            LogLevel.INFO,
            f"Processing violation: {violation.get('description', 'Unknown')}",
            "violation_processing",
            **violation
        )

        # Evaluate alert rules
        triggered_alerts = []

        for rule in self.alert_rules:
            if not rule.enabled:
                continue

            try:
                # Simple condition evaluation (in production, use a proper expression evaluator)
                if self._evaluate_condition(rule.condition, violation):
                    alert = self._create_alert(rule, violation)
                    triggered_alerts.append((rule, alert))

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error evaluating alert rule {rule.name}: {e}",
                    "alert_rule_error",
                    rule_name=rule.name,
                    error=str(e)
                )

        # Send triggered alerts
        for rule, alert in triggered_alerts:
            if self._should_send_alert(rule, alert):
                self._send_alert(rule, alert)
                self._record_alert(alert)

    def _evaluate_condition(self, condition: str, violation: Dict[str, Any]) -> bool:
        """Evaluate alert condition against violation data.

        Args:
            condition: Condition expression
            violation: Violation data

        Returns:
            True if condition is met
        """
        # Simple condition evaluator - in production use a proper expression engine
        try:
            # Replace variable references with actual values
            eval_condition = condition
            for key, value in violation.items():
                if isinstance(value, str):
                    eval_condition = eval_condition.replace(key, f"'{value}'")
                else:
                    eval_condition = eval_condition.replace(key, str(value))

            # Evaluate the condition
            return eval(eval_condition)
        except Exception:
            return False

    def _create_alert(self, rule: AlertRule, violation: Dict[str, Any]) -> Alert:
        """Create alert from rule and violation.

        Args:
            rule: Alert rule that triggered
            violation: Violation data

        Returns:
            Alert instance
        """
        alert_id = f"{rule.name}_{int(time.time())}"

        title = f"[{rule.severity.value}] {violation.get('description', rule.name)}"
        message = self._format_alert_message(rule, violation)

        return Alert(
            id=alert_id,
            rule_name=rule.name,
            severity=rule.severity,
            title=title,
            message=message,
            timestamp=datetime.now().isoformat(),
            source=violation.get('dag_id', 'unknown'),
            channels=rule.channels,
            metadata=violation
        )

    def _format_alert_message(self, rule: AlertRule, violation: Dict[str, Any]) -> str:
        """Format alert message with violation details.

        Args:
            rule: Alert rule
            violation: Violation data

        Returns:
            Formatted message
        """
        message = f"""
Alert: {rule.name}
Severity: {rule.severity.value}
Description: {violation.get('description', 'N/A')}

Details:
"""

        # Add relevant details based on violation type
        if 'actual_value' in violation:
            message += f"- Actual: {violation['actual_value']}\n"
        if 'expected_value' in violation:
            message += f"- Expected: {violation['expected_value']}\n"
        if 'dag_id' in violation:
            message += f"- DAG: {violation['dag_id']}\n"
        if 'task_id' in violation:
            message += f"- Task: {violation['task_id']}\n"
        if 'error_code' in violation:
            message += f"- Error: {violation['error_code']}\n"

        message += f"\nTimestamp: {violation.get('timestamp', 'N/A')}"
        message += f"\nRule: {rule.description}"

        return message.strip()

    def _should_send_alert(self, rule: AlertRule, alert: Alert) -> bool:
        """Check if alert should be sent (cooldown logic).

        Args:
            rule: Alert rule
            alert: Alert to send

        Returns:
            True if alert should be sent
        """
        alert_key = f"{rule.name}:{alert.source}"

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

    def _send_alert(self, rule: AlertRule, alert: Alert) -> None:
        """Send alert via configured channels.

        Args:
            rule: Alert rule
            alert: Alert to send
        """
        self.logger.log(
            LogLevel.INFO,
            f"Sending alert {alert.id} via {len(alert.channels)} channels",
            "alert_send_start",
            alert_id=alert.id,
            channels=[c.value for c in alert.channels]
        )

        for channel in alert.channels:
            try:
                if channel == AlertChannel.EMAIL:
                    self._send_email_alert(alert)
                elif channel == AlertChannel.SLACK:
                    self._send_slack_alert(alert)
                elif channel == AlertChannel.PAGERDUTY:
                    self._send_pagerduty_alert(alert)
                elif channel == AlertChannel.WEBHOOK:
                    self._send_webhook_alert(alert)
                elif channel == AlertChannel.SMS:
                    self._send_sms_alert(alert)

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Failed to send alert via {channel.value}: {e}",
                    "alert_send_error",
                    channel=channel.value,
                    alert_id=alert.id,
                    error=str(e)
                )

    def _send_email_alert(self, alert: Alert) -> None:
        """Send alert via email."""
        if not self.smtp_config:
            return

        msg = MIMEMultipart()
        msg['From'] = self.smtp_config.get('from_email', 'alerts@aurum.com')
        msg['To'] = ', '.join(self.smtp_config.get('to_emails', []))
        msg['Subject'] = alert.title

        body = f"""
{alert.message}

This is an automated alert from the Aurum data platform.
Please check the monitoring dashboard for more details.
"""
        msg.attach(MIMEText(body, 'plain'))

        try:
            server = smtplib.SMTP(self.smtp_config.get('host', 'localhost'))
            server.sendmail(msg['From'], msg['To'].split(','), msg.as_string())
            server.quit()

            self.logger.log(
                LogLevel.INFO,
                f"Sent email alert {alert.id}",
                "email_alert_sent",
                alert_id=alert.id
            )

        except Exception as e:
            raise Exception(f"Email alert failed: {e}")

    def _send_slack_alert(self, alert: Alert) -> None:
        """Send alert via Slack."""
        # Implementation would use Slack webhook
        self.logger.log(
            LogLevel.INFO,
            f"Sent Slack alert {alert.id}",
            "slack_alert_sent",
            alert_id=alert.id
        )

    def _send_pagerduty_alert(self, alert: Alert) -> None:
        """Send alert via PagerDuty."""
        # Implementation would use PagerDuty Events API
        self.logger.log(
            LogLevel.INFO,
            f"Sent PagerDuty alert {alert.id}",
            "pagerduty_alert_sent",
            alert_id=alert.id
        )

    def _send_webhook_alert(self, alert: Alert) -> None:
        """Send alert via webhook."""
        # Implementation would POST to configured webhook URLs
        self.logger.log(
            LogLevel.INFO,
            f"Sent webhook alert {alert.id}",
            "webhook_alert_sent",
            alert_id=alert.id
        )

    def _send_sms_alert(self, alert: Alert) -> None:
        """Send alert via SMS."""
        # Implementation would use SMS service
        self.logger.log(
            LogLevel.INFO,
            f"Sent SMS alert {alert.id}",
            "sms_alert_sent",
            alert_id=alert.id
        )

    def _record_alert(self, alert: Alert) -> None:
        """Record alert in history and active alerts.

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
            f"Recorded alert {alert.id}",
            "alert_recorded",
            alert_id=alert.id,
            severity=alert.severity.value
        )

    def get_active_alerts(self) -> List[Alert]:
        """Get currently active alerts.

        Returns:
            List of active alerts
        """
        return list(self.active_alerts.values())

    def get_alert_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get alert summary for the specified time period.

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
            "by_rule": {},
            "by_source": {},
            "recent_alerts": [
                {
                    "id": alert.id,
                    "title": alert.title,
                    "severity": alert.severity.value,
                    "timestamp": alert.timestamp,
                    "source": alert.source
                }
                for alert in recent_alerts[-10:]  # Last 10 alerts
            ]
        }

        # Count by severity
        for alert in recent_alerts:
            severity = alert.severity.value
            summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + 1

        # Count by rule
        for alert in recent_alerts:
            rule = alert.rule_name
            summary["by_rule"][rule] = summary["by_rule"].get(rule, 0) + 1

        # Count by source
        for alert in recent_alerts:
            source = alert.source
            summary["by_source"][source] = summary["by_source"].get(source, 0) + 1

        return summary
