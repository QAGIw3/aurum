"""Tests for staleness monitoring system."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from pathlib import Path

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.staleness import (
    StalenessMonitor,
    StalenessConfig,
    DatasetStaleness,
    StalenessLevel,
    StalenessDetector,
    StalenessCheck,
    WatermarkTracker,
    WatermarkStatus,
    StalenessAlertManager,
    StalenessAlertRule,
    StalenessAlertSeverity
)


class TestStalenessConfig:
    def test_config_defaults(self) -> None:
        """Test staleness configuration defaults."""
        config = StalenessConfig()

        assert config.warning_threshold_hours == 2
        assert config.critical_threshold_hours == 6
        assert config.grace_period_hours == 1
        assert config.check_interval_minutes == 5
        assert config.alert_cooldown_minutes == 15

    def test_config_custom_values(self) -> None:
        """Test staleness configuration with custom values."""
        config = StalenessConfig(
            warning_threshold_hours=1,
            critical_threshold_hours=3,
            grace_period_hours=0.5,
            check_interval_minutes=10,
            alert_cooldown_minutes=30,
            dataset_thresholds={
                "test_dataset": {"warning": 2, "critical": 4}
            }
        )

        assert config.warning_threshold_hours == 1
        assert config.critical_threshold_hours == 3
        assert config.grace_period_hours == 0.5
        assert config.check_interval_minutes == 10
        assert config.alert_cooldown_minutes == 30
        assert "test_dataset" in config.dataset_thresholds

    def test_get_thresholds(self) -> None:
        """Test getting thresholds for datasets."""
        config = StalenessConfig(
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            grace_period_hours=1,
            dataset_thresholds={
                "special_dataset": {"warning": 1, "critical": 3}
            }
        )

        # Default thresholds
        default_thresholds = config.get_thresholds("normal_dataset")
        assert default_thresholds["warning"] == 2
        assert default_thresholds["critical"] == 6

        # Special thresholds
        special_thresholds = config.get_thresholds("special_dataset")
        assert special_thresholds["warning"] == 1
        assert special_thresholds["critical"] == 3


class TestDatasetStaleness:
    def test_staleness_creation(self) -> None:
        """Test DatasetStaleness creation and properties."""
        now = datetime.now()
        last_update = now - timedelta(hours=3)

        staleness = DatasetStaleness(
            dataset="test_dataset",
            last_watermark=last_update,
            current_time=now,
            staleness_level=StalenessLevel.STALE,
            hours_since_update=3.0,
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            grace_period_hours=1
        )

        assert staleness.dataset == "test_dataset"
        assert staleness.last_watermark == last_update
        assert staleness.staleness_level == StalenessLevel.STALE
        assert staleness.hours_since_update == 3.0
        assert staleness.is_stale() is True
        assert staleness.is_fresh() is False
        assert staleness.is_critical() is False

    def test_staleness_to_dict(self) -> None:
        """Test converting staleness to dictionary."""
        now = datetime.now()
        last_update = now - timedelta(hours=3)

        staleness = DatasetStaleness(
            dataset="test_dataset",
            last_watermark=last_update,
            current_time=now,
            staleness_level=StalenessLevel.STALE,
            hours_since_update=3.0,
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            grace_period_hours=1
        )

        result = staleness.to_dict()

        assert result["dataset"] == "test_dataset"
        assert result["staleness_level"] == "stale"
        assert result["hours_since_update"] == 3.0
        assert result["warning_threshold_hours"] == 2
        assert result["critical_threshold_hours"] == 6
        assert "last_watermark" in result
        assert "current_time" in result


class TestStalenessMonitor:
    def test_monitor_init(self) -> None:
        """Test staleness monitor initialization."""
        config = StalenessConfig()
        monitor = StalenessMonitor(config)

        assert monitor.config == config
        assert monitor.expected_datasets == {}
        assert monitor.dataset_states == {}
        assert monitor.violation_history == {}
        assert monitor.last_alerts == {}

    def test_register_dataset(self) -> None:
        """Test dataset registration."""
        config = StalenessConfig()
        monitor = StalenessMonitor(config)

        monitor.register_dataset("test_dataset", expected_frequency_hours=24)

        assert "test_dataset" in monitor.expected_datasets
        assert monitor.expected_datasets["test_dataset"]["expected_frequency_hours"] == 24

    def test_calculate_staleness_fresh(self) -> None:
        """Test staleness calculation for fresh data."""
        config = StalenessConfig(grace_period_hours=1)
        monitor = StalenessMonitor(config)

        now = datetime.now()
        last_update = now - timedelta(minutes=30)  # Within grace period

        thresholds = {"warning": 2, "critical": 6}

        staleness = monitor._calculate_staleness(
            "test_dataset", last_update, now, thresholds
        )

        assert staleness.staleness_level == StalenessLevel.FRESH
        assert staleness.hours_since_update == 0.5
        assert staleness.sla_breach_duration_minutes is None

    def test_calculate_staleness_stale(self) -> None:
        """Test staleness calculation for stale data."""
        config = StalenessConfig(warning_threshold_hours=2, grace_period_hours=1)
        monitor = StalenessMonitor(config)

        now = datetime.now()
        last_update = now - timedelta(hours=3)  # Beyond warning threshold

        thresholds = {"warning": 2, "critical": 6}

        staleness = monitor._calculate_staleness(
            "test_dataset", last_update, now, thresholds
        )

        assert staleness.staleness_level == StalenessLevel.STALE
        assert staleness.hours_since_update == 3.0
        assert staleness.sla_breach_duration_minutes == 120  # 2 hours * 60

    def test_calculate_staleness_critical(self) -> None:
        """Test staleness calculation for critical data."""
        config = StalenessConfig(critical_threshold_hours=6, grace_period_hours=1)
        monitor = StalenessMonitor(config)

        now = datetime.now()
        last_update = now - timedelta(hours=8)  # Beyond critical threshold

        thresholds = {"warning": 2, "critical": 6}

        staleness = monitor._calculate_staleness(
            "test_dataset", last_update, now, thresholds
        )

        assert staleness.staleness_level == StalenessLevel.CRITICAL
        assert staleness.hours_since_update == 8.0
        assert staleness.sla_breach_duration_minutes == 420  # 7 hours * 60

    def test_calculate_staleness_missing(self) -> None:
        """Test staleness calculation for missing watermark."""
        config = StalenessConfig()
        monitor = StalenessMonitor(config)

        now = datetime.now()

        thresholds = {"warning": 2, "critical": 6}

        staleness = monitor._calculate_staleness(
            "test_dataset", None, now, thresholds
        )

        assert staleness.staleness_level == StalenessLevel.CRITICAL
        assert staleness.hours_since_update == float('inf')
        assert staleness.sla_breach_duration_minutes is None

    def test_get_stale_datasets(self) -> None:
        """Test getting stale datasets."""
        config = StalenessConfig()
        monitor = StalenessMonitor(config)

        # Add some test datasets
        now = datetime.now()

        # Fresh dataset
        fresh_staleness = DatasetStaleness(
            dataset="fresh_dataset",
            last_watermark=now - timedelta(minutes=30),
            current_time=now,
            staleness_level=StalenessLevel.FRESH,
            hours_since_update=0.5,
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            grace_period_hours=1
        )

        # Stale dataset
        stale_staleness = DatasetStaleness(
            dataset="stale_dataset",
            last_watermark=now - timedelta(hours=3),
            current_time=now,
            staleness_level=StalenessLevel.STALE,
            hours_since_update=3.0,
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            grace_period_hours=1
        )

        # Critical dataset
        critical_staleness = DatasetStaleness(
            dataset="critical_dataset",
            last_watermark=now - timedelta(hours=8),
            current_time=now,
            staleness_level=StalenessLevel.CRITICAL,
            hours_since_update=8.0,
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            grace_period_hours=1
        )

        monitor.dataset_states = {
            "fresh_dataset": fresh_staleness,
            "stale_dataset": stale_staleness,
            "critical_dataset": critical_staleness
        }

        # Get stale datasets
        stale_datasets = monitor.get_stale_datasets()
        assert len(stale_datasets) == 2
        assert "stale_dataset" in [s.dataset for s in stale_datasets]
        assert "critical_dataset" in [s.dataset for s in stale_datasets]
        assert "fresh_dataset" not in [s.dataset for s in stale_datasets]

        # Get critical datasets only
        critical_datasets = monitor.get_critical_datasets()
        assert len(critical_datasets) == 1
        assert critical_datasets[0].dataset == "critical_dataset"

    def test_should_alert_cooldown(self) -> None:
        """Test alert cooldown logic."""
        config = StalenessConfig(alert_cooldown_minutes=15)
        monitor = StalenessMonitor(config)

        # First alert should be sent
        assert monitor.should_alert("test_dataset", StalenessLevel.STALE) is True

        # Second alert should be blocked by cooldown
        assert monitor.should_alert("test_dataset", StalenessLevel.STALE) is False


class TestStalenessDetector:
    def test_detector_init(self) -> None:
        """Test staleness detector initialization."""
        config = StalenessConfig()
        detector = StalenessDetector(config)

        assert detector.config == config
        assert len(detector.checks) == 0
        assert detector.last_check_times == {}
        assert detector.consecutive_failures == {}

    def test_add_staleness_check(self) -> None:
        """Test adding staleness checks."""
        config = StalenessConfig()
        detector = StalenessDetector(config)

        check = StalenessCheck(
            dataset="test_dataset",
            check_interval_minutes=10,
            enabled=True,
            alert_on_stale=True,
            alert_on_critical=True
        )

        detector.add_staleness_check(check)

        assert "test_dataset" in detector.checks
        assert detector.checks["test_dataset"].check_interval_minutes == 10

    def test_run_staleness_checks_no_checks(self) -> None:
        """Test running checks with no checks configured."""
        config = StalenessConfig()
        detector = StalenessDetector(config)

        results = detector.run_staleness_checks()

        assert results["total_checks"] == 0
        assert results["datasets_checked"] == 0
        assert results["stale_datasets"] == 0
        assert results["critical_datasets"] == 0

    def test_should_run_check_logic(self) -> None:
        """Test check execution timing logic."""
        config = StalenessConfig()
        detector = StalenessDetector(config)

        check = StalenessCheck(dataset="test_dataset", check_interval_minutes=5)
        detector.add_staleness_check(check)

        now = datetime.now()

        # First check should run
        assert detector._should_run_check("test_dataset", now) is True

        # Record last check time
        detector.last_check_times["test_dataset"] = now - timedelta(minutes=3)

        # Should not run yet (only 3 minutes since last check)
        assert detector._should_run_check("test_dataset", now) is False

        # After 5 minutes, should run
        detector.last_check_times["test_dataset"] = now - timedelta(minutes=6)
        assert detector._should_run_check("test_dataset", now) is True

    def test_get_detector_status(self) -> None:
        """Test detector status retrieval."""
        config = StalenessConfig()
        detector = StalenessDetector(config)

        check = StalenessCheck(dataset="test_dataset")
        detector.add_staleness_check(check)

        detector.consecutive_failures["test_dataset"] = 2
        detector.last_check_times["test_dataset"] = datetime.now() - timedelta(minutes=10)

        status = detector.get_detector_status()

        assert status["total_checks"] == 1
        assert status["enabled_checks"] == 1
        assert status["consecutive_failures"]["test_dataset"] == 2
        assert "last_check_times" in status
        assert "monitor_summary" in status

    def test_set_check_enabled(self) -> None:
        """Test enabling/disabling checks."""
        config = StalenessConfig()
        detector = StalenessDetector(config)

        check = StalenessCheck(dataset="test_dataset", enabled=False)
        detector.add_staleness_check(check)

        # Initially disabled
        assert detector.checks["test_dataset"].enabled is False

        # Enable
        detector.set_check_enabled("test_dataset", True)
        assert detector.checks["test_dataset"].enabled is True

        # Disable
        detector.set_check_enabled("test_dataset", False)
        assert detector.checks["test_dataset"].enabled is False


class TestWatermarkTracker:
    def test_tracker_init(self) -> None:
        """Test watermark tracker initialization."""
        tracker = WatermarkTracker()

        assert tracker.watermarks == {}
        assert tracker.default_frequency_hours == 24
        assert tracker.default_grace_period_hours == 1

    def test_register_dataset(self) -> None:
        """Test dataset registration."""
        tracker = WatermarkTracker()

        tracker.register_dataset(
            "test_dataset",
            expected_frequency_hours=12,
            grace_period_hours=2
        )

        assert "test_dataset" in tracker.watermarks
        assert tracker.watermarks["test_dataset"].expected_frequency_hours == 12
        assert tracker.watermarks["test_dataset"].grace_period_hours == 2

    def test_update_watermark(self) -> None:
        """Test watermark updates."""
        tracker = WatermarkTracker()

        # Mock database update
        with patch('aurum.staleness.watermark_tracker.update_ingest_watermark') as mock_update:
            mock_update.return_value = True

            now = datetime.now()
            success = tracker.update_watermark("test_dataset", now, policy="exact")

            assert success is True
            assert "test_dataset" in tracker.watermarks
            assert tracker.watermarks["test_dataset"].last_watermark == now
            assert tracker.watermarks["test_dataset"].status == WatermarkStatus.CURRENT

    def test_check_watermark_status(self) -> None:
        """Test watermark status checking."""
        tracker = WatermarkTracker()

        # Mock database retrieval
        with patch('aurum.staleness.watermark_tracker.get_ingest_watermark') as mock_get:
            mock_get.return_value = datetime.now().isoformat()

            status = tracker.check_watermark_status("test_dataset")

            assert status in [WatermarkStatus.CURRENT, WatermarkStatus.STALE]

    def test_get_stale_watermarks(self) -> None:
        """Test getting stale watermarks."""
        tracker = WatermarkTracker()

        # Add test datasets
        tracker.register_dataset("fresh_dataset")
        tracker.register_dataset("stale_dataset")
        tracker.register_dataset("missing_dataset")

        # Mock database responses
        with patch('aurum.staleness.watermark_tracker.get_ingest_watermark') as mock_get:
            # Fresh dataset - recent watermark
            mock_get.return_value = datetime.now().isoformat()
            tracker.check_watermark_status("fresh_dataset")

            # Stale dataset - old watermark
            mock_get.return_value = (datetime.now() - timedelta(hours=5)).isoformat()
            tracker.check_watermark_status("stale_dataset")

            # Missing dataset - no watermark
            mock_get.return_value = None
            tracker.check_watermark_status("missing_dataset")

        stale_watermarks = tracker.get_stale_watermarks()

        assert len(stale_watermarks) >= 2  # stale and missing datasets

    def test_get_tracking_summary(self) -> None:
        """Test tracking summary generation."""
        tracker = WatermarkTracker()

        tracker.register_dataset("test_dataset")
        tracker.watermarks["test_dataset"].status = WatermarkStatus.CURRENT

        summary = tracker.get_tracking_summary()

        assert "total_tracked_datasets" in summary
        assert "current_watermarks" in summary
        assert "stale_watermarks" in summary
        assert "missing_watermarks" in summary
        assert "error_watermarks" in summary
        assert "datasets" in summary


class TestStalenessAlertManager:
    def test_alert_manager_init(self) -> None:
        """Test alert manager initialization."""
        manager = StalenessAlertManager()

        assert len(manager.alert_rules) == 4  # Default rules
        assert len(manager.active_alerts) == 0
        assert len(manager.alert_history) == 0
        assert len(manager.last_alerts) == 0

    def test_create_default_alert_rules(self) -> None:
        """Test default alert rules creation."""
        manager = StalenessAlertManager()

        critical_rules = [r for r in manager.alert_rules if r.severity == StalenessAlertSeverity.CRITICAL]
        high_rules = [r for r in manager.alert_rules if r.severity == StalenessAlertSeverity.HIGH]
        low_rules = [r for r in manager.alert_rules if r.severity == StalenessAlertSeverity.LOW]

        assert len(critical_rules) == 1
        assert len(high_rules) == 2
        assert len(low_rules) == 1

    def test_evaluate_rule_condition(self) -> None:
        """Test alert rule condition evaluation."""
        manager = StalenessAlertManager()

        # Test staleness level condition
        staleness_info = {"staleness_level": "CRITICAL"}
        rule = StalenessAlertRule(
            name="test_critical",
            condition="staleness_level == 'CRITICAL'",
            severity=StalenessAlertSeverity.CRITICAL
        )

        assert manager._evaluate_rule_condition(rule, staleness_info) is True

        # Test hours condition
        staleness_info = {"hours_since_update": 5}
        rule = StalenessAlertRule(
            name="test_hours",
            condition="hours_since_update > 3",
            severity=StalenessAlertSeverity.HIGH
        )

        assert manager._evaluate_rule_condition(rule, staleness_info) is True

    def test_should_send_alert_cooldown(self) -> None:
        """Test alert cooldown logic."""
        manager = StalenessAlertManager()

        rule = StalenessAlertRule(
            name="test_rule",
            condition="severity == 'HIGH'",
            severity=StalenessAlertSeverity.HIGH,
            cooldown_minutes=15
        )

        # First alert should be sent
        assert manager._should_send_alert(rule, "test_dataset") is True

        # Second alert should be blocked by cooldown
        assert manager._should_send_alert(rule, "test_dataset") is False

    def test_get_active_staleness_alerts(self) -> None:
        """Test getting active staleness alerts."""
        manager = StalenessAlertManager()

        # No active alerts initially
        assert len(manager.get_active_staleness_alerts()) == 0

        # Add an alert
        from aurum.staleness.alert_manager import StalenessAlert
        alert = StalenessAlert(
            id="test_alert",
            rule_name="test_rule",
            severity=StalenessAlertSeverity.HIGH,
            dataset="test_dataset",
            title="Test Alert",
            message="Test message",
            timestamp=datetime.now().isoformat(),
            hours_since_update=5.0,
            expected_frequency_hours=24,
            grace_period_hours=1,
            channels=["email"],
            recipients=[]
        )

        manager.active_alerts["test_alert"] = alert

        active_alerts = manager.get_active_staleness_alerts()
        assert len(active_alerts) == 1
        assert active_alerts[0].id == "test_alert"

    def test_get_staleness_alert_summary(self) -> None:
        """Test staleness alert summary generation."""
        manager = StalenessAlertManager()

        # Add test alerts
        from aurum.staleness.alert_manager import StalenessAlert

        for i in range(5):
            alert = StalenessAlert(
                id=f"alert_{i}",
                rule_name="test_rule",
                severity=StalenessAlertSeverity.HIGH if i < 3 else StalenessAlertSeverity.MEDIUM,
                dataset=f"dataset_{i % 2}",
                title=f"Test Alert {i}",
                message=f"Test message {i}",
                timestamp=(datetime.now() - timedelta(hours=i)).isoformat(),
                hours_since_update=5.0 + i,
                expected_frequency_hours=24,
                grace_period_hours=1,
                channels=["email"],
                recipients=[]
            )
            manager.alert_history.append(alert)

        summary = manager.get_staleness_alert_summary(hours=24)

        assert summary["total_alerts"] == 5
        assert "by_severity" in summary
        assert "by_dataset" in summary
        assert "by_rule" in summary
        assert "recent_alerts" in summary
        assert len(summary["recent_alerts"]) == 5

    def test_process_staleness(self) -> None:
        """Test staleness processing."""
        manager = StalenessAlertManager()

        staleness_info = {
            "dataset": "test_dataset",
            "staleness_level": "CRITICAL",
            "hours_since_update": 8.0,
            "expected_frequency_hours": 24,
            "grace_period_hours": 1,
            "last_watermark": datetime.now().isoformat()
        }

        with patch.object(manager, '_create_staleness_alert') as mock_create, \
             patch.object(manager, '_send_staleness_alert') as mock_send, \
             patch.object(manager, '_record_alert') as mock_record:

            manager.process_staleness("test_dataset", staleness_info)

            # Should create and send alert
            assert mock_create.called
            assert mock_send.called
            assert mock_record.called


class TestIntegrationScenarios:
    def test_complete_staleness_workflow(self) -> None:
        """Test complete staleness monitoring workflow."""
        config = StalenessConfig(
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            grace_period_hours=1
        )

        monitor = StalenessMonitor(config)
        detector = StalenessDetector(config)
        tracker = WatermarkTracker()
        alert_manager = StalenessAlertManager()

        # Register datasets
        datasets = ["eia_prices", "fred_gdp", "cpi_inflation"]

        for dataset in datasets:
            monitor.register_dataset(dataset, expected_frequency_hours=24)
            tracker.register_dataset(dataset)

            detector.add_staleness_check(StalenessCheck(
                dataset=dataset,
                check_interval_minutes=5,
                enabled=True,
                alert_on_stale=True,
                alert_on_critical=True
            ))

        # Mock watermark retrieval
        with patch('aurum.staleness.watermark_tracker.get_ingest_watermark') as mock_get:
            # Set up different staleness scenarios
            mock_get.side_effect = [
                (datetime.now() - timedelta(hours=1)).isoformat(),  # Fresh
                (datetime.now() - timedelta(hours=4)).isoformat(),  # Stale
                None  # Missing
            ]

            # Run staleness checks
            results = detector.run_staleness_checks()

            # Check results
            assert results["total_checks"] == 3
            assert results["datasets_checked"] == 3
            assert results["stale_datasets"] >= 1  # At least the stale one
            assert results["critical_datasets"] >= 1  # At least the missing one

            # Check staleness information
            assert "eia_prices" in results["results"]
            assert "fred_gdp" in results["results"]
            assert "cpi_inflation" in results["results"]

            # Verify monitor state
            stale_datasets = monitor.get_stale_datasets()
            assert len(stale_datasets) >= 2  # stale and missing

            # Verify tracker state
            stale_watermarks = tracker.get_stale_watermarks()
            assert len(stale_watermarks) >= 2

    def test_staleness_levels_progression(self) -> None:
        """Test progression through staleness levels over time."""
        config = StalenessConfig(
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            grace_period_hours=1
        )

        monitor = StalenessMonitor(config)

        dataset = "test_progression"
        monitor.register_dataset(dataset, expected_frequency_hours=24)

        base_time = datetime.now()

        # Test fresh (within grace period)
        fresh_time = base_time - timedelta(minutes=30)
        fresh_staleness = monitor._calculate_staleness(
            dataset, fresh_time, base_time,
            {"warning": 2, "critical": 6}
        )
        assert fresh_staleness.staleness_level == StalenessLevel.FRESH

        # Test stale (beyond warning threshold)
        stale_time = base_time - timedelta(hours=3)
        stale_staleness = monitor._calculate_staleness(
            dataset, stale_time, base_time,
            {"warning": 2, "critical": 6}
        )
        assert stale_staleness.staleness_level == StalenessLevel.STALE

        # Test critical (beyond critical threshold)
        critical_time = base_time - timedelta(hours=8)
        critical_staleness = monitor._calculate_staleness(
            dataset, critical_time, base_time,
            {"warning": 2, "critical": 6}
        )
        assert critical_staleness.staleness_level == StalenessLevel.CRITICAL

    def test_alert_escalation(self) -> None:
        """Test alert escalation based on staleness severity."""
        alert_manager = StalenessAlertManager()

        # Test different staleness scenarios
        scenarios = [
            {
                "dataset": "critical_dataset",
                "staleness_info": {
                    "staleness_level": "CRITICAL",
                    "hours_since_update": 8.0,
                    "expected_frequency_hours": 24,
                    "grace_period_hours": 1
                }
            },
            {
                "dataset": "high_dataset",
                "staleness_info": {
                    "staleness_level": "STALE",
                    "hours_since_update": 3.0,
                    "expected_frequency_hours": 24,
                    "grace_period_hours": 1
                }
            },
            {
                "dataset": "fresh_dataset",
                "staleness_info": {
                    "staleness_level": "FRESH",
                    "hours_since_update": 0.5,
                    "expected_frequency_hours": 24,
                    "grace_period_hours": 1
                }
            }
        ]

        # Process each scenario
        for scenario in scenarios:
            alert_manager.process_staleness(
                scenario["dataset"],
                scenario["staleness_info"]
            )

        # Check that alerts were generated appropriately
        active_alerts = alert_manager.get_active_staleness_alerts()

        # Should have alerts for critical and high severity
        assert len(active_alerts) >= 2

        # Check alert properties
        critical_alert = next(
            (a for a in active_alerts if a.dataset == "critical_dataset"),
            None
        )
        high_alert = next(
            (a for a in active_alerts if a.dataset == "high_dataset"),
            None
        )

        assert critical_alert is not None
        assert high_alert is not None
        assert critical_alert.severity == StalenessAlertSeverity.CRITICAL
        assert high_alert.severity == StalenessAlertSeverity.HIGH

    def test_dataset_specific_thresholds(self) -> None:
        """Test dataset-specific staleness thresholds."""
        config = StalenessConfig(
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            dataset_thresholds={
                "critical_eia": {"warning": 1, "critical": 2},
                "relaxed_noaa": {"warning": 12, "critical": 48}
            }
        )

        monitor = StalenessMonitor(config)

        # Test default thresholds
        default_thresholds = config.get_thresholds("normal_dataset")
        assert default_thresholds["warning"] == 2
        assert default_thresholds["critical"] == 6

        # Test critical EIA thresholds
        eia_thresholds = config.get_thresholds("critical_eia")
        assert eia_thresholds["warning"] == 1
        assert eia_thresholds["critical"] == 2

        # Test relaxed NOAA thresholds
        noaa_thresholds = config.get_thresholds("relaxed_noaa")
        assert noaa_thresholds["warning"] == 12
        assert noaa_thresholds["critical"] == 48

    def test_multi_dataset_monitoring(self) -> None:
        """Test monitoring multiple datasets with different characteristics."""
        config = StalenessConfig()
        detector = StalenessDetector(config)

        # Add datasets with different characteristics
        datasets = [
            ("eia_high_freq", 1),      # High frequency
            ("fred_weekly", 24 * 7),   # Weekly
            ("cpi_monthly", 24 * 30),  # Monthly
            ("noaa_relaxed", 24 * 2), # Relaxed
        ]

        for dataset, frequency in datasets:
            detector.add_staleness_check(StalenessCheck(
                dataset=dataset,
                check_interval_minutes=5,
                enabled=True,
                alert_on_stale=True,
                alert_on_critical=True
            ))

        # Run checks
        results = detector.run_staleness_checks()

        # Verify all datasets were checked
        assert results["total_checks"] == 4
        assert results["datasets_checked"] == 4
        assert "results" in results
        assert len(results["results"]) == 4

        # Check detector status
        status = detector.get_detector_status()
        assert status["total_checks"] == 4
        assert status["enabled_checks"] == 4
