"""Tests for canary dataset monitoring system."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.canary import (
    CanaryManager,
    CanaryConfig,
    CanaryDataset,
    CanaryStatus,
    APIHealthChecker,
    APIHealthResult,
    APIHealthStatus,
    CanaryRunner,
    CanaryResult,
    CanaryAlertManager,
    CanaryAlertRule,
    CanaryAlertSeverity
)


class TestCanaryConfig:
    def test_config_creation(self) -> None:
        """Test canary configuration creation."""
        config = CanaryConfig(
            name="test_canary",
            source="test_source",
            dataset="test_dataset",
            api_endpoint="https://api.example.com/test",
            expected_fields=["data", "timestamp"],
            timeout_seconds=30
        )

        assert config.name == "test_canary"
        assert config.source == "test_source"
        assert config.dataset == "test_dataset"
        assert config.api_endpoint == "https://api.example.com/test"
        assert config.expected_fields == ["data", "timestamp"]
        assert config.timeout_seconds == 30
        assert config.enabled is True
        assert config.schedule == "0 */6 * * *"

    def test_config_with_custom_values(self) -> None:
        """Test canary configuration with custom values."""
        config = CanaryConfig(
            name="custom_canary",
            source="custom_source",
            dataset="custom_dataset",
            api_endpoint="https://custom.api.com/data",
            expected_response_format="xml",
            expected_fields=["items", "metadata"],
            timeout_seconds=60,
            max_retries=5,
            retry_delay_seconds=10,
            alert_on_failure=False,
            alert_channels=["slack"],
            description="Custom test canary"
        )

        assert config.expected_response_format == "xml"
        assert config.max_retries == 5
        assert config.retry_delay_seconds == 10
        assert config.alert_on_failure is False
        assert config.alert_channels == ["slack"]
        assert config.description == "Custom test canary"


class TestCanaryDataset:
    def test_canary_creation(self) -> None:
        """Test canary dataset creation."""
        config = CanaryConfig(
            name="test_canary",
            source="test_source",
            dataset="test_dataset",
            api_endpoint="https://api.example.com/test"
        )

        canary = CanaryDataset(
            name="test_canary",
            source="test_source",
            dataset="test_dataset",
            config=config
        )

        assert canary.name == "test_canary"
        assert canary.source == "test_source"
        assert canary.dataset == "test_dataset"
        assert canary.config == config
        assert canary.status == CanaryStatus.DISABLED
        assert canary.consecutive_failures == 0
        assert canary.total_runs == 0

    def test_canary_status_methods(self) -> None:
        """Test canary status checking methods."""
        canary = CanaryDataset(
            name="test_canary",
            source="test_source",
            dataset="test_dataset",
            config=MagicMock()
        )

        # Test healthy status
        canary.status = CanaryStatus.HEALTHY
        assert canary.is_healthy() is True
        assert canary.is_unhealthy() is False

        # Test unhealthy status
        canary.status = CanaryStatus.ERROR
        assert canary.is_healthy() is False
        assert canary.is_unhealthy() is True

        # Test should_alert logic
        assert canary.should_alert() is True  # No last run, should alert
        canary.last_run = datetime.now() - timedelta(minutes=30)
        assert canary.should_alert() is False  # Recent run, don't alert

    def test_canary_update_status(self) -> None:
        """Test canary status updates."""
        canary = CanaryDataset(
            name="test_canary",
            source="test_source",
            dataset="test_dataset",
            config=MagicMock()
        )

        # Update to healthy
        canary.update_status(CanaryStatus.HEALTHY, 1.5, 100)

        assert canary.status == CanaryStatus.HEALTHY
        assert canary.execution_time_seconds == 1.5
        assert canary.records_processed == 100
        assert canary.total_runs == 1
        assert canary.successful_runs == 1
        assert canary.consecutive_failures == 0

        # Update to error
        canary.update_status(CanaryStatus.ERROR, 2.0, 0, ["Test error"])

        assert canary.status == CanaryStatus.ERROR
        assert canary.total_runs == 2
        assert canary.successful_runs == 1
        assert canary.consecutive_failures == 1
        assert canary.failure_rate == 0.5  # 1 failure out of 2 runs

    def test_canary_to_dict(self) -> None:
        """Test canary to dictionary conversion."""
        canary = CanaryDataset(
            name="test_canary",
            source="test_source",
            dataset="test_dataset",
            config=MagicMock()
        )

        canary.update_status(CanaryStatus.HEALTHY, 1.5, 100)
        result = canary.to_dict()

        assert result["name"] == "test_canary"
        assert result["source"] == "test_source"
        assert result["status"] == "healthy"
        assert result["total_runs"] == 1
        assert result["is_healthy"] is True
        assert result["is_unhealthy"] is False


class TestCanaryManager:
    def test_manager_init(self) -> None:
        """Test canary manager initialization."""
        manager = CanaryManager()

        assert len(manager.canaries) == 0
        assert len(manager.config_registry) == 0

    def test_register_canary(self) -> None:
        """Test canary registration."""
        manager = CanaryManager()

        config = CanaryConfig(
            name="test_canary",
            source="test_source",
            dataset="test_dataset",
            api_endpoint="https://api.example.com/test"
        )

        canary = manager.register_canary(config)

        assert canary.name == "test_canary"
        assert "test_canary" in manager.canaries
        assert "test_canary" in manager.config_registry

    def test_get_canary(self) -> None:
        """Test canary retrieval."""
        manager = CanaryManager()

        config = CanaryConfig(
            name="test_canary",
            source="test_source",
            dataset="test_dataset",
            api_endpoint="https://api.example.com/test"
        )

        manager.register_canary(config)

        # Get existing canary
        canary = manager.get_canary("test_canary")
        assert canary is not None
        assert canary.name == "test_canary"

        # Get non-existent canary
        assert manager.get_canary("nonexistent") is None

    def test_get_canaries_by_source(self) -> None:
        """Test getting canaries by source."""
        manager = CanaryManager()

        # Register canaries for different sources
        for i in range(3):
            config = CanaryConfig(
                name=f"test_canary_{i}",
                source="source_a" if i < 2 else "source_b",
                dataset=f"dataset_{i}",
                api_endpoint=f"https://api{i}.example.com/test"
            )
            manager.register_canary(config)

        source_a_canaries = manager.get_canaries_by_source("source_a")
        source_b_canaries = manager.get_canaries_by_source("source_b")

        assert len(source_a_canaries) == 2
        assert len(source_b_canaries) == 1

    def test_get_unhealthy_canaries(self) -> None:
        """Test getting unhealthy canaries."""
        manager = CanaryManager()

        # Create healthy canary
        healthy_config = CanaryConfig(name="healthy", source="test", dataset="test", api_endpoint="https://test.com")
        healthy_canary = manager.register_canary(healthy_config)
        healthy_canary.status = CanaryStatus.HEALTHY

        # Create unhealthy canary
        unhealthy_config = CanaryConfig(name="unhealthy", source="test", dataset="test", api_endpoint="https://test.com")
        unhealthy_canary = manager.register_canary(unhealthy_config)
        unhealthy_canary.status = CanaryStatus.ERROR

        unhealthy_canaries = manager.get_unhealthy_canaries()

        assert len(unhealthy_canaries) == 1
        assert unhealthy_canaries[0].name == "unhealthy"

    def test_get_critical_canaries(self) -> None:
        """Test getting critical canaries."""
        manager = CanaryManager()

        # Create normal unhealthy canary
        normal_config = CanaryConfig(name="normal", source="test", dataset="test", api_endpoint="https://test.com")
        normal_canary = manager.register_canary(normal_config)
        normal_canary.status = CanaryStatus.ERROR
        normal_canary.consecutive_failures = 1

        # Create critical canary (high failure rate)
        critical_config = CanaryConfig(name="critical", source="test", dataset="test", api_endpoint="https://test.com")
        critical_canary = manager.register_canary(critical_config)
        critical_canary.status = CanaryStatus.ERROR
        critical_canary.consecutive_failures = 5
        critical_canary.total_runs = 10
        critical_canary.successful_runs = 2  # 80% failure rate

        critical_canaries = manager.get_critical_canaries()

        assert len(critical_canaries) == 1
        assert critical_canaries[0].name == "critical"

    def test_update_canary_status(self) -> None:
        """Test canary status updates."""
        manager = CanaryManager()

        config = CanaryConfig(name="test", source="test", dataset="test", api_endpoint="https://test.com")
        manager.register_canary(config)

        # Update status
        success = manager.update_canary_status(
            "test",
            CanaryStatus.HEALTHY,
            execution_time=1.5,
            records_processed=100,
            errors=["Test error"],
            warnings=["Test warning"]
        )

        assert success is True
        canary = manager.get_canary("test")
        assert canary.status == CanaryStatus.HEALTHY
        assert canary.execution_time_seconds == 1.5
        assert canary.records_processed == 100
        assert "Test error" in canary.errors

    def test_get_canary_health_summary(self) -> None:
        """Test canary health summary generation."""
        manager = CanaryManager()

        # Add canaries with different health states
        for i in range(5):
            config = CanaryConfig(name=f"canary_{i}", source="test", dataset="test", api_endpoint="https://test.com")
            canary = manager.register_canary(config)

            if i < 2:  # Healthy
                canary.status = CanaryStatus.HEALTHY
            elif i < 4:  # Unhealthy
                canary.status = CanaryStatus.ERROR
            else:  # Disabled
                canary.status = CanaryStatus.DISABLED

        summary = manager.get_canary_health_summary()

        assert summary["total_canaries"] == 5
        assert summary["healthy_canaries"] == 2
        assert summary["unhealthy_canaries"] == 2
        assert summary["disabled_canaries"] == 1
        assert summary["overall_health_rate"] == 0.4  # 2/5
        assert "health_by_source" in summary
        assert "canaries" in summary


class TestAPIHealthChecker:
    def test_checker_init(self) -> None:
        """Test API health checker initialization."""
        checker = APIHealthChecker()

        assert len(checker.health_checks) == 0

    def test_register_health_check(self) -> None:
        """Test health check registration."""
        checker = APIHealthChecker()

        from aurum.canary.api_health_checker import APIHealthCheck

        check = APIHealthCheck(
            name="test_check",
            url="https://api.example.com/test",
            method="GET",
            expected_status_codes=[200, 201]
        )

        checker.register_health_check(check)

        assert "test_check" in checker.health_checks
        assert checker.health_checks["test_check"].name == "test_check"

    def test_check_api_health_success(self) -> None:
        """Test successful API health check."""
        checker = APIHealthChecker()

        from aurum.canary.api_health_checker import APIHealthCheck

        check = APIHealthCheck(
            name="test_check",
            url="https://httpbin.org/get",
            method="GET",
            expected_response_format="json"
        )

        checker.register_health_check(check)

        # Mock successful response
        with patch.object(checker.session, 'request') as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = b'{"test": "data"}'
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            result = checker.check_api_health("test_check")

            assert result.status == APIHealthStatus.HEALTHY
            assert result.response_time_ms > 0
            assert result.status_code == 200

    def test_check_api_health_failure(self) -> None:
        """Test failed API health check."""
        checker = APIHealthChecker()

        from aurum.canary.api_health_checker import APIHealthCheck

        check = APIHealthCheck(
            name="test_check",
            url="https://httpbin.org/status/500",
            method="GET"
        )

        checker.register_health_check(check)

        # Mock failed response
        with patch.object(checker.session, 'request') as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.raise_for_status.side_effect = Exception("500 Internal Server Error")
            mock_request.return_value = mock_response

            result = checker.check_api_health("test_check")

            assert result.status == APIHealthStatus.UNHEALTHY
            assert result.status_code == 500
            assert result.error_message == "500 Internal Server Error"

    def test_check_all_apis(self) -> None:
        """Test checking all APIs."""
        checker = APIHealthChecker()

        # Register multiple checks
        for i in range(3):
            from aurum.canary.api_health_checker import APIHealthCheck
            check = APIHealthCheck(
                name=f"test_check_{i}",
                url=f"https://httpbin.org/get",
                method="GET"
            )
            checker.register_health_check(check)

        # Mock responses
        with patch.object(checker.session, 'request') as mock_request:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = b'{"test": "data"}'
            mock_response.raise_for_status.return_value = None
            mock_request.return_value = mock_response

            results = checker.check_all_apis()

            assert len(results) == 3
            for result in results.values():
                assert result.status == APIHealthStatus.HEALTHY

    def test_get_unhealthy_apis(self) -> None:
        """Test getting unhealthy APIs."""
        checker = APIHealthChecker()

        # Register mixed health checks
        for i in range(3):
            from aurum.canary.api_health_checker import APIHealthCheck
            check = APIHealthCheck(
                name=f"test_check_{i}",
                url=f"https://httpbin.org/get",
                method="GET"
            )
            checker.register_health_check(check)

        # Mock mixed responses
        def mock_request_side_effect(**kwargs):
            mock_response = MagicMock()
            if kwargs.get('url', '').endswith('test_check_1'):
                # Make this one fail
                mock_response.status_code = 500
                mock_response.raise_for_status.side_effect = Exception("500 Error")
            else:
                mock_response.status_code = 200
                mock_response.content = b'{"test": "data"}'
                mock_response.raise_for_status.return_value = None
            return mock_response

        with patch.object(checker.session, 'request', side_effect=mock_request_side_effect):
            unhealthy_apis = checker.get_unhealthy_apis()

            assert len(unhealthy_apis) == 1
            assert unhealthy_apis[0].check_name == "test_check_1"


class TestCanaryRunner:
    def test_runner_init(self) -> None:
        """Test canary runner initialization."""
        manager = CanaryManager()
        checker = APIHealthChecker()

        runner = CanaryRunner(manager, checker)

        assert runner.canary_manager == manager
        assert runner.api_checker == checker

    def test_run_canary_success(self) -> None:
        """Test successful canary execution."""
        manager = CanaryManager()
        checker = APIHealthChecker()

        # Register a canary
        config = CanaryConfig(name="test", source="test", dataset="test", api_endpoint="https://test.com")
        canary = manager.register_canary(config)

        runner = CanaryRunner(manager, checker)

        # Mock successful API check
        with patch.object(runner, '_run_api_checks') as mock_api_checks:
            mock_result = APIHealthResult(
                check_name="test_api_check",
                status=APIHealthStatus.HEALTHY,
                response_time_ms=100.0,
                timestamp=datetime.now().isoformat()
            )
            mock_api_checks.return_value = [mock_result]

            with patch.object(runner, '_validate_canary_data') as mock_validate:
                mock_validate.return_value = {
                    "records_processed": 1,
                    "quality_score": 1.0,
                    "errors": [],
                    "warnings": []
                }

                result = runner.run_canary("test")

                assert result.is_success() is True
                assert result.status == CanaryStatus.HEALTHY
                assert result.records_processed == 1

    def test_run_canary_failure(self) -> None:
        """Test failed canary execution."""
        manager = CanaryManager()
        checker = APIHealthChecker()

        # Register a canary
        config = CanaryConfig(name="test", source="test", dataset="test", api_endpoint="https://test.com")
        canary = manager.register_canary(config)

        runner = CanaryRunner(manager, checker)

        # Mock failed API check
        with patch.object(runner, '_run_api_checks') as mock_api_checks:
            mock_result = APIHealthResult(
                check_name="test_api_check",
                status=APIHealthStatus.ERROR,
                response_time_ms=0.0,
                timestamp=datetime.now().isoformat(),
                error_message="API error"
            )
            mock_api_checks.return_value = [mock_result]

            result = runner.run_canary("test")

            assert result.is_failure() is True
            assert result.status == CanaryStatus.ERROR
            assert len(result.errors) > 0

    def test_run_all_canaries(self) -> None:
        """Test running all canaries."""
        manager = CanaryManager()
        checker = APIHealthChecker()

        # Register multiple canaries
        for i in range(3):
            config = CanaryConfig(name=f"test_{i}", source="test", dataset="test", api_endpoint="https://test.com")
            manager.register_canary(config)

        runner = CanaryRunner(manager, checker)

        # Mock canary execution
        with patch.object(runner, 'run_canary') as mock_run_canary:
            mock_run_canary.return_value = CanaryResult(
                canary_name="test",
                execution_time_seconds=1.0,
                status=CanaryStatus.HEALTHY
            )

            results = runner.run_all_canaries()

            assert len(results) == 3
            mock_run_canary.assert_called()  # Should be called 3 times

    def test_get_runner_status(self) -> None:
        """Test runner status retrieval."""
        manager = CanaryManager()
        checker = APIHealthChecker()

        runner = CanaryRunner(manager, checker)

        # Add some canaries
        for i in range(2):
            config = CanaryConfig(name=f"test_{i}", source="test", dataset="test", api_endpoint="https://test.com")
            canary = manager.register_canary(config)
            canary.status = CanaryStatus.HEALTHY if i == 0 else CanaryStatus.ERROR

        status = runner.get_runner_status()

        assert "total_canaries" in status
        assert "healthy_canaries" in status
        assert "canary_health_summary" in status
        assert "api_checker_status" in status


class TestCanaryAlertManager:
    def test_alert_manager_init(self) -> None:
        """Test alert manager initialization."""
        manager = CanaryAlertManager()

        assert len(manager.alert_rules) == 5  # Default rules
        assert len(manager.active_alerts) == 0
        assert len(manager.alert_history) == 0

    def test_evaluate_rule_condition(self) -> None:
        """Test alert rule condition evaluation."""
        manager = CanaryAlertManager()

        rule = CanaryAlertRule(
            name="test_rule",
            condition="consecutive_failures >= 3 and status == 'ERROR'",
            severity=CanaryAlertSeverity.CRITICAL
        )

        # Test condition that should pass
        result = {
            "status": "ERROR",
            "consecutive_failures": 3
        }
        assert manager._evaluate_rule_condition(rule, result, None) is True

        # Test condition that should fail
        result = {
            "status": "ERROR",
            "consecutive_failures": 1
        }
        assert manager._evaluate_rule_condition(rule, result, None) is False

    def test_process_canary_result(self) -> None:
        """Test canary result processing."""
        manager = CanaryAlertManager()

        # Mock canary for context
        mock_canary = MagicMock()
        mock_canary.source = "test_source"

        with patch.object(manager, '_canary_manager') as mock_manager:
            mock_manager.get_canary.return_value = mock_canary

            with patch.object(manager, '_create_canary_alert') as mock_create, \
                 patch.object(manager, '_send_canary_alert') as mock_send, \
                 patch.object(manager, '_record_alert') as mock_record:

                result = {
                    "status": "ERROR",
                    "consecutive_failures": 3,
                    "errors": ["Test error"]
                }

                manager.process_canary_result("test_canary", result)

                mock_create.assert_called_once()
                mock_send.assert_called_once()
                mock_record.assert_called_once()

    def test_get_active_canary_alerts(self) -> None:
        """Test getting active canary alerts."""
        manager = CanaryAlertManager()

        # No active alerts initially
        assert len(manager.get_active_canary_alerts()) == 0

        # Add an alert
        from aurum.canary.alert_manager import CanaryAlert
        alert = CanaryAlert(
            id="test_alert",
            rule_name="test_rule",
            severity=CanaryAlertSeverity.HIGH,
            canary_name="test_canary",
            source="test_source",
            title="Test Alert",
            message="Test message",
            timestamp=datetime.now().isoformat(),
            status="ERROR",
            consecutive_failures=3,
            last_success=None,
            last_failure=datetime.now().isoformat(),
            channels=["email"],
            recipients=[]
        )

        manager.active_alerts["test_alert"] = alert

        active_alerts = manager.get_active_canary_alerts()
        assert len(active_alerts) == 1
        assert active_alerts[0].id == "test_alert"

    def test_get_canary_alert_summary(self) -> None:
        """Test canary alert summary generation."""
        manager = CanaryAlertManager()

        # Add test alerts
        from aurum.canary.alert_manager import CanaryAlert

        for i in range(5):
            alert = CanaryAlert(
                id=f"alert_{i}",
                rule_name="test_rule",
                severity=CanaryAlertSeverity.HIGH if i < 3 else CanaryAlertSeverity.MEDIUM,
                canary_name=f"canary_{i % 2}",
                source="test_source",
                title=f"Test Alert {i}",
                message=f"Test message {i}",
                timestamp=(datetime.now() - timedelta(hours=i)).isoformat(),
                status="ERROR",
                consecutive_failures=3,
                last_success=None,
                last_failure=datetime.now().isoformat(),
                channels=["email"],
                recipients=[]
            )
            manager.alert_history.append(alert)

        summary = manager.get_canary_alert_summary(hours=24)

        assert summary["total_alerts"] == 5
        assert "by_severity" in summary
        assert "by_canary" in summary
        assert "by_rule" in summary
        assert "recent_alerts" in summary
        assert len(summary["recent_alerts"]) == 5


class TestIntegrationScenarios:
    def test_complete_canary_workflow(self) -> None:
        """Test complete canary monitoring workflow."""
        # Initialize components
        canary_manager = CanaryManager()
        api_checker = APIHealthChecker()
        runner = CanaryRunner(canary_manager, api_checker)
        alert_manager = CanaryAlertManager()

        # Register canary
        config = CanaryConfig(
            name="test_canary",
            source="test_source",
            dataset="test_dataset",
            api_endpoint="https://api.example.com/test",
            expected_fields=["data"],
            timeout_seconds=30
        )

        canary = canary_manager.register_canary(config)
        alert_manager._canary_manager = canary_manager  # For accessing canary data

        # Mock successful API check
        with patch.object(runner, '_run_api_checks') as mock_api_checks:
            mock_result = APIHealthResult(
                check_name="test_api_check",
                status=APIHealthStatus.HEALTHY,
                response_time_ms=100.0,
                timestamp=datetime.now().isoformat()
            )
            mock_api_checks.return_value = [mock_result]

            with patch.object(runner, '_validate_canary_data') as mock_validate:
                mock_validate.return_value = {
                    "records_processed": 1,
                    "quality_score": 1.0,
                    "errors": [],
                    "warnings": []
                }

                # Run canary
                result = runner.run_canary("test_canary")

                # Verify success
                assert result.is_success() is True
                assert result.status == CanaryStatus.HEALTHY
                assert result.records_processed == 1

                # Verify canary updated
                updated_canary = canary_manager.get_canary("test_canary")
                assert updated_canary.status == CanaryStatus.HEALTHY
                assert updated_canary.consecutive_failures == 0

                # Process result (should not generate alerts for healthy canary)
                alert_manager.process_canary_result("test_canary", result.to_dict())

                # Verify no alerts generated
                active_alerts = alert_manager.get_active_canary_alerts()
                assert len(active_alerts) == 0

    def test_canary_failure_workflow(self) -> None:
        """Test canary failure detection and alerting."""
        # Initialize components
        canary_manager = CanaryManager()
        api_checker = APIHealthChecker()
        runner = CanaryRunner(canary_manager, api_checker)
        alert_manager = CanaryAlertManager()

        # Register canary
        config = CanaryConfig(
            name="failing_canary",
            source="test_source",
            dataset="test_dataset",
            api_endpoint="https://api.example.com/fail",
            expected_fields=["data"],
            timeout_seconds=30
        )

        canary = canary_manager.register_canary(config)
        alert_manager._canary_manager = canary_manager  # For accessing canary data

        # Mock failed API check
        with patch.object(runner, '_run_api_checks') as mock_api_checks:
            mock_result = APIHealthResult(
                check_name="failing_api_check",
                status=APIHealthStatus.ERROR,
                response_time_ms=0.0,
                timestamp=datetime.now().isoformat(),
                error_message="API connection failed"
            )
            mock_api_checks.return_value = [mock_result]

            # Run canary (should fail)
            result = runner.run_canary("failing_canary")

            # Verify failure
            assert result.is_failure() is True
            assert result.status == CanaryStatus.ERROR
            assert len(result.errors) > 0

            # Verify canary updated with failure
            updated_canary = canary_manager.get_canary("failing_canary")
            assert updated_canary.status == CanaryStatus.ERROR
            assert updated_canary.consecutive_failures == 1

            # Process result (should generate alerts)
            alert_manager.process_canary_result("failing_canary", result.to_dict())

            # Verify alerts were generated
            active_alerts = alert_manager.get_active_canary_alerts()
            assert len(active_alerts) > 0

            # Find the alert for our canary
            canary_alerts = [a for a in active_alerts if a.canary_name == "failing_canary"]
            assert len(canary_alerts) > 0

    def test_canary_health_monitoring(self) -> None:
        """Test comprehensive canary health monitoring."""
        canary_manager = CanaryManager()

        # Register canaries with different health states
        canary_configs = [
            ("healthy_canary", "test_source", CanaryStatus.HEALTHY),
            ("unhealthy_canary", "test_source", CanaryStatus.ERROR),
            ("disabled_canary", "test_source", CanaryStatus.DISABLED),
            ("critical_canary", "test_source", CanaryStatus.ERROR),  # Will be marked critical
        ]

        for name, source, status in canary_configs:
            config = CanaryConfig(name=name, source=source, dataset="test", api_endpoint="https://test.com")
            canary = canary_manager.register_canary(config)
            canary.status = status

            # Make critical canary have high failure rate
            if name == "critical_canary":
                canary.consecutive_failures = 5
                canary.total_runs = 10
                canary.successful_runs = 2  # 80% failure rate

        # Test health monitoring
        healthy_canaries = canary_manager.get_canaries_by_source("test_source")
        unhealthy_canaries = canary_manager.get_unhealthy_canaries()
        critical_canaries = canary_manager.get_critical_canaries()

        assert len(healthy_canaries) == 4  # All canaries from test_source
        assert len(unhealthy_canaries) == 1  # Only unhealthy_canary
        assert len(critical_canaries) == 1   # critical_canary due to high failure rate

        # Test health summary
        summary = canary_manager.get_canary_health_summary()

        assert summary["total_canaries"] == 4
        assert summary["healthy_canaries"] == 1
        assert summary["unhealthy_canaries"] == 1
        assert summary["disabled_canaries"] == 1
        assert summary["overall_health_rate"] == 0.25  # 1/4

        # Test canary management
        assert canary_manager.enable_canary("disabled_canary") is True
        assert canary_manager.disable_canary("healthy_canary") is True

        # Verify state changes
        disabled_canary = canary_manager.get_canary("disabled_canary")
        healthy_canary = canary_manager.get_canary("healthy_canary")

        assert disabled_canary.status == CanaryStatus.HEALTHY  # Now enabled
        assert healthy_canary.status == CanaryStatus.DISABLED  # Now disabled

    def test_multi_source_canary_monitoring(self) -> None:
        """Test canary monitoring across multiple data sources."""
        canary_manager = CanaryManager()

        # Register canaries for different sources
        sources = ["eia", "fred", "cpi", "iso", "noaa"]
        for source in sources:
            for i in range(2):  # 2 canaries per source
                config = CanaryConfig(
                    name=f"{source}_canary_{i}",
                    source=source,
                    dataset=f"{source}_dataset_{i}",
                    api_endpoint=f"https://{source}.api.com/test"
                )
                canary_manager.register_canary(config)

        # Test source-specific queries
        for source in sources:
            source_canaries = canary_manager.get_canaries_by_source(source)
            assert len(source_canaries) == 2
            assert all(c.source == source for c in source_canaries)

        # Test health summary by source
        summary = canary_manager.get_canary_health_summary()

        assert summary["total_canaries"] == 10
        assert "health_by_source" in summary

        for source in sources:
            assert source in summary["health_by_source"]
            assert summary["health_by_source"][source]["healthy"] == 0  # All start as disabled
            assert summary["health_by_source"][source]["disabled"] == 2

    def test_alert_escalation_scenarios(self) -> None:
        """Test alert escalation for different canary failure scenarios."""
        alert_manager = CanaryAlertManager()

        # Test different failure scenarios
        scenarios = [
            {
                "canary_name": "single_failure",
                "result": {
                    "status": "ERROR",
                    "consecutive_failures": 1,
                    "errors": ["API timeout"]
                }
            },
            {
                "canary_name": "multiple_failures",
                "result": {
                    "status": "ERROR",
                    "consecutive_failures": 3,
                    "errors": ["API error", "Connection failed"]
                }
            },
            {
                "canary_name": "timeout_canary",
                "result": {
                    "status": "TIMEOUT",
                    "consecutive_failures": 1,
                    "errors": ["Request timeout"]
                }
            },
            {
                "canary_name": "healthy_canary",
                "result": {
                    "status": "HEALTHY",
                    "consecutive_failures": 0,
                    "warnings": ["Slow response"]
                }
            }
        ]

        # Process each scenario
        for scenario in scenarios:
            alert_manager.process_canary_result(
                scenario["canary_name"],
                scenario["result"]
            )

        # Check that appropriate alerts were generated
        active_alerts = alert_manager.get_active_canary_alerts()

        # Should have alerts for failures and timeouts
        failure_alerts = [a for a in active_alerts if "failure" in a.canary_name.lower()]
        timeout_alerts = [a for a in active_alerts if "timeout" in a.canary_name.lower()]

        assert len(failure_alerts) >= 1
        assert len(timeout_alerts) >= 1

        # Check alert severities
        critical_alerts = [a for a in active_alerts if a.severity == CanaryAlertSeverity.CRITICAL]
        high_alerts = [a for a in active_alerts if a.severity == CanaryAlertSeverity.HIGH]

        # Multiple failures should be critical
        assert len(critical_alerts) >= 1

        # Single failures should be high
        assert len(high_alerts) >= 1
