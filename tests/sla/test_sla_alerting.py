"""Tests for SLA monitoring and alerting system."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.sla import (
    SLAMonitor,
    SLAConfig,
    SLAViolation,
    FailureCallbackManager,
    FailureCallback,
    MetricsPusher,
    DAGMetrics,
    AlertManager,
    AlertRule,
    AlertSeverity,
    AlertChannel
)


class TestSLAConfig:
    def test_sla_config_defaults(self) -> None:
        """Test SLA configuration defaults."""
        config = SLAConfig()

        assert config.max_execution_time_minutes == 60
        assert config.max_error_rate_percent == 5.0
        assert config.min_records_per_hour == 100
        assert config.max_data_age_hours == 24
        assert config.sla_check_interval_minutes == 5
        assert config.alert_cooldown_minutes == 15

    def test_sla_config_custom_values(self) -> None:
        """Test SLA configuration with custom values."""
        config = SLAConfig(
            max_execution_time_minutes=30,
            max_error_rate_percent=2.0,
            min_records_per_hour=50,
            max_data_age_hours=12,
            sla_check_interval_minutes=10,
            alert_cooldown_minutes=30,
            dataset_overrides={
                "critical_dataset": {
                    "max_execution_time_minutes": 15,
                    "max_error_rate_percent": 1.0
                }
            }
        )

        assert config.max_execution_time_minutes == 30
        assert config.max_error_rate_percent == 2.0
        assert config.min_records_per_hour == 50
        assert config.max_data_age_hours == 12
        assert config.sla_check_interval_minutes == 10
        assert config.alert_cooldown_minutes == 30
        assert "critical_dataset" in config.dataset_overrides


class TestSLAMonitor:
    def test_sla_monitor_init(self) -> None:
        """Test SLA monitor initialization."""
        config = SLAConfig()
        monitor = SLAMonitor(config)

        assert monitor.config == config
        assert monitor.dag_states == {}
        assert monitor.violation_history == {}
        assert monitor.last_alerts == {}

    def test_check_dag_sla_timeout_violation(self) -> None:
        """Test SLA check for timeout violation."""
        config = SLAConfig(max_execution_time_minutes=30)
        monitor = SLAMonitor(config)

        violations = monitor.check_dag_sla(
            dag_id="test_dag",
            task_id="test_task",
            execution_time=45 * 60  # 45 minutes
        )

        assert len(violations) == 1
        assert violations[0].violation_type.value == "timeout"
        assert violations[0].severity == "HIGH"
        assert violations[0].actual_value == 45
        assert violations[0].expected_value == 30

    def test_check_dag_sla_error_rate_violation(self) -> None:
        """Test SLA check for error rate violation."""
        config = SLAConfig(max_error_rate_percent=5.0)
        monitor = SLAMonitor(config)

        violations = monitor.check_dag_sla(
            dag_id="test_dag",
            task_id="test_task",
            execution_time=30 * 60,
            records_processed=100,
            error_count=10  # 10% error rate
        )

        assert len(violations) == 1
        assert violations[0].violation_type.value == "error_rate"
        assert violations[0].severity == "HIGH"
        assert violations[0].actual_value == 10.0
        assert violations[0].expected_value == 5.0

    def test_check_dag_sla_throughput_violation(self) -> None:
        """Test SLA check for throughput violation."""
        config = SLAConfig(min_records_per_hour=100)
        monitor = SLAMonitor(config)

        violations = monitor.check_dag_sla(
            dag_id="test_dag",
            task_id="test_task",
            execution_time=60 * 60,  # 1 hour
            records_processed=50  # Less than minimum
        )

        assert len(violations) == 1
        assert violations[0].violation_type.value == "throughput"
        assert violations[0].severity == "LOW"
        assert violations[0].actual_value == 50
        assert violations[0].expected_value == 100

    def test_check_dag_sla_data_quality_violation(self) -> None:
        """Test SLA check for data quality violation."""
        config = SLAConfig()
        monitor = SLAMonitor(config)

        violations = monitor.check_dag_sla(
            dag_id="test_dag",
            task_id="test_task",
            execution_time=30 * 60,
            records_processed=100,
            error_count=0,
            data_quality_score=0.8  # Below 95% threshold
        )

        assert len(violations) == 1
        assert violations[0].violation_type.value == "data_quality"
        assert violations[0].severity == "MEDIUM"
        assert violations[0].actual_value == 0.8
        assert violations[0].expected_value == 0.95

    def test_check_dag_sla_staleness_violation(self) -> None:
        """Test SLA check for data staleness violation."""
        config = SLAConfig(max_data_age_hours=24)
        monitor = SLAMonitor(config)

        last_success = datetime.now() - timedelta(hours=30)  # 30 hours ago

        violations = monitor.check_dag_sla(
            dag_id="test_dag",
            task_id="test_task",
            execution_time=30 * 60,
            records_processed=100,
            error_count=0,
            data_quality_score=0.95,
            last_success_time=last_success
        )

        assert len(violations) == 1
        assert violations[0].violation_type.value == "staleness"
        assert violations[0].severity == "HIGH"
        assert violations[0].actual_value == 30
        assert violations[0].expected_value == 24

    def test_get_sla_status(self) -> None:
        """Test SLA status retrieval."""
        config = SLAConfig()
        monitor = SLAMonitor(config)

        # No state recorded
        status = monitor.get_sla_status("unknown_dag")
        assert status["status"] == "unknown"

        # Record some state
        monitor.record_dag_state("test_dag", "success", execution_time=30.0, records_processed=100)

        status = monitor.get_sla_status("test_dag")
        assert status["status"] == "healthy"
        assert status["recent_violations"] == 0

    def test_should_alert_cooldown(self) -> None:
        """Test alert cooldown logic."""
        config = SLAConfig(alert_cooldown_minutes=15)
        monitor = SLAMonitor(config)

        violation = SLAViolation(
            timestamp=datetime.now().isoformat(),
            dag_id="test_dag",
            task_id="test_task",
            violation_type=SLAViolationType.TIMEOUT,
            description="Test violation",
            severity="HIGH",
            actual_value=60,
            expected_value=30
        )

        # First alert should be sent
        assert monitor.should_alert(violation) is True

        # Second alert should be blocked by cooldown
        assert monitor.should_alert(violation) is False


class TestFailureCallbackManager:
    def test_callback_manager_init(self) -> None:
        """Test failure callback manager initialization."""
        manager = FailureCallbackManager()

        assert len(manager.callbacks) == 4  # Default callbacks
        assert all(cb.priority >= 10 for cb in manager.callbacks)

    def test_execute_callbacks(self) -> None:
        """Test callback execution."""
        manager = FailureCallbackManager()

        context = {
            "dag": {"dag_id": "test_dag"},
            "task_instance": MagicMock(task_id="test_task", try_number=1)
        }

        with patch.object(manager, 'callbacks') as mock_callbacks:
            mock_callback = MagicMock()
            mock_callbacks.__getitem__ = MagicMock(return_value=[mock_callback])

            manager.execute_callbacks(context)

            mock_callback.assert_called_once_with(context, None)

    def test_add_remove_callback(self) -> None:
        """Test adding and removing callbacks."""
        manager = FailureCallbackManager()

        initial_count = len(manager.callbacks)

        # Add callback
        new_callback = FailureCallback(
            name="test_callback",
            callback_func=MagicMock()
        )
        manager.add_callback(new_callback)

        assert len(manager.callbacks) == initial_count + 1
        assert any(cb.name == "test_callback" for cb in manager.callbacks)

        # Remove callback
        assert manager.remove_callback("test_callback") is True
        assert len(manager.callbacks) == initial_count
        assert not any(cb.name == "test_callback" for cb in manager.callbacks)

        # Try to remove non-existent callback
        assert manager.remove_callback("nonexistent") is False

    def test_check_conditions_severity(self) -> None:
        """Test condition checking for severity."""
        manager = FailureCallbackManager()

        callback = FailureCallback(
            name="severity_test",
            callback_func=MagicMock(),
            conditions={"severity": ["HIGH", "CRITICAL"]}
        )

        context = {"task_instance": MagicMock(try_number=5)}
        exception = ValueError("test error")

        # Should trigger for HIGH severity
        with patch.object(manager, '_determine_severity', return_value="HIGH"):
            assert manager._check_conditions(callback, context, exception) is True

        # Should not trigger for LOW severity
        with patch.object(manager, '_determine_severity', return_value="LOW"):
            assert manager._check_conditions(callback, context, exception) is False

    def test_determine_severity(self) -> None:
        """Test severity determination."""
        manager = FailureCallbackManager()

        # Critical for repeated failures
        context = {"task_instance": MagicMock(try_number=5)}
        assert manager._determine_severity(context) == "CRITICAL"

        # High for timeout errors
        context = {"task_instance": MagicMock(try_number=1)}
        exception = TimeoutError("timeout")
        assert manager._determine_severity(context, exception) == "HIGH"

        # Medium for other cases
        context = {"task_instance": MagicMock(try_number=1)}
        exception = ValueError("validation error")
        assert manager._determine_severity(context, exception) == "MEDIUM"


class TestMetricsPusher:
    def test_metrics_pusher_init(self) -> None:
        """Test metrics pusher initialization."""
        pusher = MetricsPusher(
            prometheus_gateway="http://localhost:9091",
            statsd_host="localhost",
            statsd_port=8125,
            kafka_bootstrap_servers="localhost:9092",
            kafka_topic="aurum.metrics"
        )

        assert pusher.prometheus_gateway == "http://localhost:9091"
        assert pusher.statsd_host == "localhost"
        assert pusher.statsd_port == 8125
        assert len(pusher.metrics_buffer) == 0

    def test_collect_metrics(self) -> None:
        """Test metrics collection."""
        pusher = MetricsPusher()

        metrics = pusher.collect_metrics(
            dag_id="test_dag",
            task_id="test_task",
            execution_date="2024-01-01T12:00:00",
            state="success",
            try_number=1,
            max_tries=3,
            duration_seconds=123.45,
            records_processed=1000,
            bytes_processed=1048576,
            error_count=5,
            data_quality_score=0.95
        )

        assert metrics.dag_id == "test_dag"
        assert metrics.task_id == "test_task"
        assert metrics.state == "success"
        assert metrics.duration_seconds == 123.45
        assert metrics.records_processed == 1000
        assert len(pusher.metrics_buffer) == 1

    def test_push_metrics_buffer_logic(self) -> None:
        """Test metrics push buffer logic."""
        pusher = MetricsPusher()

        # Add some metrics
        for i in range(50):
            pusher.collect_metrics(f"dag_{i}", f"task_{i}")

        # Buffer not full yet
        with patch.object(pusher, '_push_to_kafka') as mock_push:
            pusher.push_metrics()
            mock_push.assert_not_called()

        # Add more metrics to trigger push
        for i in range(50, 100):
            pusher.collect_metrics(f"dag_{i}", f"task_{i}")

        # Buffer should trigger push now
        with patch.object(pusher, '_push_to_kafka') as mock_push:
            pusher.push_metrics()
            mock_push.assert_called_once()

    def test_get_dag_performance_summary(self) -> None:
        """Test DAG performance summary generation."""
        pusher = MetricsPusher()

        # Add some metrics
        base_time = datetime.now() - timedelta(hours=12)

        for i in range(10):
            execution_time = base_time + timedelta(hours=i)
            pusher.collect_metrics(
                "test_dag",
                f"task_{i}",
                execution_date=execution_time.isoformat(),
                state="success" if i < 8 else "failed",
                duration_seconds=60 + i * 10,
                records_processed=100 + i * 50
            )

        summary = pusher.get_dag_performance_summary("test_dag", hours=24)

        assert summary["dag_id"] == "test_dag"
        assert summary["total_runs"] == 10
        assert summary["successful_runs"] == 8
        assert summary["failed_runs"] == 2
        assert summary["success_rate"] == 0.8
        assert "avg_duration_seconds" in summary
        assert "total_records_processed" in summary


class TestAlertManager:
    def test_alert_manager_init(self) -> None:
        """Test alert manager initialization."""
        smtp_config = {"host": "localhost", "from_email": "test@example.com"}
        slack_config = {"webhook": "https://hooks.slack.com/test"}
        pagerduty_config = {"routing_key": "test_key"}
        webhook_configs = {"test": "https://example.com/webhook"}

        manager = AlertManager(
            smtp_config=smtp_config,
            slack_config=slack_config,
            pagerduty_config=pagerduty_config,
            webhook_configs=webhook_configs
        )

        assert len(manager.alert_rules) == 6  # Default rules
        assert manager.smtp_config == smtp_config
        assert manager.slack_config == slack_config
        assert len(manager.active_alerts) == 0
        assert len(manager.alert_history) == 0

    def test_create_default_alert_rules(self) -> None:
        """Test default alert rules creation."""
        manager = AlertManager()

        critical_rules = [r for r in manager.alert_rules if r.severity == AlertSeverity.CRITICAL]
        high_rules = [r for r in manager.alert_rules if r.severity == AlertSeverity.HIGH]
        medium_rules = [r for r in manager.alert_rules if r.severity == AlertSeverity.MEDIUM]

        assert len(critical_rules) >= 1
        assert len(high_rules) >= 2
        assert len(medium_rules) >= 2

    def test_evaluate_condition_simple(self) -> None:
        """Test simple condition evaluation."""
        manager = AlertManager()

        # Test equality
        violation = {"severity": "CRITICAL", "description": "Test"}
        assert manager._evaluate_condition("severity == 'CRITICAL'", violation) is True
        assert manager._evaluate_condition("severity == 'HIGH'", violation) is False

        # Test numeric comparison
        violation = {"actual_value": 10, "expected_value": 5}
        assert manager._evaluate_condition("actual_value > expected_value", violation) is True
        assert manager._evaluate_condition("actual_value < expected_value", violation) is False

    def test_should_send_alert_cooldown(self) -> None:
        """Test alert cooldown logic."""
        manager = AlertManager()

        rule = AlertRule(
            name="test_rule",
            condition="severity == 'HIGH'",
            severity=AlertSeverity.HIGH,
            cooldown_minutes=15
        )

        alert = MagicMock()
        alert.id = "test_alert"
        alert.source = "test_dag"

        # First alert should be sent
        assert manager._should_send_alert(rule, alert) is True

        # Second alert should be blocked by cooldown
        assert manager._should_send_alert(rule, alert) is False

    def test_get_active_alerts(self) -> None:
        """Test getting active alerts."""
        manager = AlertManager()

        # No active alerts initially
        assert len(manager.get_active_alerts()) == 0

        # Add an alert
        alert = MagicMock()
        alert.id = "test_alert"
        manager.active_alerts["test_alert"] = alert

        assert len(manager.get_active_alerts()) == 1
        assert manager.get_active_alerts()[0].id == "test_alert"

    def test_get_alert_summary(self) -> None:
        """Test alert summary generation."""
        manager = AlertManager()

        # Add some test alerts
        for i in range(5):
            alert = MagicMock()
            alert.id = f"alert_{i}"
            alert.title = f"Test Alert {i}"
            alert.severity.value = "HIGH" if i < 3 else "MEDIUM"
            alert.timestamp = (datetime.now() - timedelta(hours=i)).isoformat()
            alert.source = f"dag_{i % 2}"  # Alternate between two sources
            alert.rule_name = "test_rule"
            manager.alert_history.append(alert)

        summary = manager.get_alert_summary(hours=24)

        assert summary["total_alerts"] == 5
        assert "by_severity" in summary
        assert "by_rule" in summary
        assert "by_source" in summary
        assert "recent_alerts" in summary
        assert len(summary["recent_alerts"]) == 5

    def test_process_violation(self) -> None:
        """Test violation processing."""
        manager = AlertManager()

        violation = {
            "dag_id": "test_dag",
            "task_id": "test_task",
            "violation_type": "timeout",
            "description": "Test timeout violation",
            "severity": "HIGH",
            "actual_value": 60,
            "expected_value": 30,
            "timestamp": datetime.now().isoformat()
        }

        with patch.object(manager, '_create_alert') as mock_create, \
             patch.object(manager, '_send_alert') as mock_send, \
             patch.object(manager, '_record_alert') as mock_record:

            manager.process_violation(violation)

            # Should create and send alerts
            assert mock_create.called
            assert mock_send.called
            assert mock_record.called


class TestIntegrationScenarios:
    def test_complete_sla_workflow(self) -> None:
        """Test complete SLA monitoring workflow."""
        config = SLAConfig(
            max_execution_time_minutes=30,
            max_error_rate_percent=5.0,
            min_records_per_hour=100
        )

        monitor = SLAMonitor(config)
        callback_manager = FailureCallbackManager()
        metrics_pusher = MetricsPusher()
        alert_manager = AlertManager()

        # Simulate DAG failure
        context = {
            "dag": {"dag_id": "test_dag"},
            "task_instance": MagicMock(task_id="test_task", try_number=2),
            "execution_date": datetime.now().isoformat()
        }

        exception = ValueError("Data validation failed")

        # Check SLA violations
        violations = monitor.check_dag_sla(
            dag_id="test_dag",
            task_id="test_task",
            execution_time=45 * 60,  # 45 minutes - exceeds limit
            records_processed=50,     # Below minimum
            error_count=10,           # High error rate
            data_quality_score=0.8    # Low quality score
        )

        assert len(violations) >= 3  # Should detect multiple violations

        # Execute failure callbacks
        callback_manager.execute_callbacks(context, exception)

        # Push metrics
        metrics = metrics_pusher.collect_metrics(
            dag_id="test_dag",
            task_id="test_task",
            state="failed",
            try_number=2,
            duration_seconds=45 * 60,
            records_processed=50,
            error_count=10,
            data_quality_score=0.8
        )

        metrics_pusher.push_metrics(force=True)

        # Process violations for alerting
        for violation in violations:
            alert_manager.process_violation(violation.__dict__)

        # Verify all components worked
        assert len(monitor.violation_history.get("test_dag", [])) > 0
        assert len(metrics_pusher.metrics_buffer) == 1
        assert len(alert_manager.alert_history) > 0

    def test_performance_monitoring(self) -> None:
        """Test performance monitoring and metrics collection."""
        pusher = MetricsPusher()

        # Collect metrics for multiple DAGs
        dags = ["eia_processor", "fred_ingestion", "cpi_pipeline"]
        states = ["success", "success", "failed"]

        for i, (dag_id, state) in enumerate(zip(dags, states)):
            pusher.collect_metrics(
                dag_id=dag_id,
                task_id=f"task_{i}",
                execution_date=datetime.now().isoformat(),
                state=state,
                duration_seconds=60 + i * 30,
                records_processed=1000 + i * 500,
                error_count=5 if state == "failed" else 0,
                data_quality_score=0.95 - (0.1 if state == "failed" else 0)
            )

        # Get performance summaries
        for dag_id in dags:
            summary = pusher.get_dag_performance_summary(dag_id, hours=24)

            assert summary["dag_id"] == dag_id
            assert "total_runs" in summary
            assert "success_rate" in summary
            assert "avg_duration_seconds" in summary

        # Push all metrics
        pusher.push_metrics(force=True)

        assert len(pusher.metrics_buffer) == 0  # Should be cleared after push

    def test_alert_escalation(self) -> None:
        """Test alert escalation based on severity and frequency."""
        manager = AlertManager()

        # Simulate escalating violations
        violations = [
            {
                "dag_id": "critical_dag",
                "task_id": "critical_task",
                "violation_type": "timeout",
                "description": "Critical timeout violation",
                "severity": "CRITICAL",
                "actual_value": 120,
                "expected_value": 30,
                "timestamp": datetime.now().isoformat()
            },
            {
                "dag_id": "high_dag",
                "task_id": "high_task",
                "violation_type": "error_rate",
                "description": "High error rate violation",
                "severity": "HIGH",
                "actual_value": 15.0,
                "expected_value": 5.0,
                "timestamp": datetime.now().isoformat()
            },
            {
                "dag_id": "medium_dag",
                "task_id": "medium_task",
                "violation_type": "throughput",
                "description": "Medium throughput violation",
                "severity": "MEDIUM",
                "actual_value": 50,
                "expected_value": 100,
                "timestamp": datetime.now().isoformat()
            }
        ]

        # Process violations
        for violation in violations:
            manager.process_violation(violation)

        # Check that alerts were created with appropriate channels
        active_alerts = manager.get_active_alerts()

        # Should have alerts for all violations
        assert len(active_alerts) == 3

        # Check alert channels based on severity
        critical_alert = next((a for a in active_alerts if a.severity == AlertSeverity.CRITICAL), None)
        high_alert = next((a for a in active_alerts if a.severity == AlertSeverity.HIGH), None)
        medium_alert = next((a for a in active_alerts if a.severity == AlertSeverity.MEDIUM), None)

        assert critical_alert is not None
        assert high_alert is not None
        assert medium_alert is not None

        # Critical should have PagerDuty
        assert AlertChannel.PAGERDUTY in critical_alert.channels
        # High should have Slack and Email
        assert AlertChannel.SLACK in high_alert.channels
        assert AlertChannel.EMAIL in high_alert.channels
        # Medium should have Email only
        assert AlertChannel.EMAIL in medium_alert.channels

    def test_multi_channel_alerting(self) -> None:
        """Test alerting via multiple channels."""
        smtp_config = {
            "host": "localhost",
            "from_email": "alerts@aurum.com",
            "to_emails": ["ops@aurum.com"]
        }

        slack_config = {"webhook": "https://hooks.slack.com/test"}
        pagerduty_config = {"routing_key": "test_key"}
        webhook_configs = {"monitoring": "https://monitoring.aurum.com/alerts"}

        manager = AlertManager(
            smtp_config=smtp_config,
            slack_config=slack_config,
            pagerduty_config=pagerduty_config,
            webhook_configs=webhook_configs
        )

        violation = {
            "dag_id": "multi_channel_dag",
            "task_id": "multi_channel_task",
            "violation_type": "timeout",
            "description": "Multi-channel test violation",
            "severity": "CRITICAL",
            "actual_value": 120,
            "expected_value": 30,
            "timestamp": datetime.now().isoformat()
        }

        with patch.object(manager, '_send_email_alert') as mock_email, \
             patch.object(manager, '_send_slack_alert') as mock_slack, \
             patch.object(manager, '_send_pagerduty_alert') as mock_pagerduty, \
             patch.object(manager, '_send_webhook_alert') as mock_webhook:

            manager.process_violation(violation)

            # All channels should be called
            mock_email.assert_called_once()
            mock_slack.assert_called_once()
            mock_pagerduty.assert_called_once()
            mock_webhook.assert_called_once()

        # Verify alert was recorded
        assert len(manager.alert_history) == 1
        assert len(manager.active_alerts) == 1
