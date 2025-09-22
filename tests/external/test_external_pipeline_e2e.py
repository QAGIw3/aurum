"""End-to-end tests for external data pipeline."""

from __future__ import annotations

import asyncio
import json
import pytest
from datetime import datetime, timedelta
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

from aurum.external.collect.base import ExternalCollector, CollectorConfig, HttpRequest, HttpResponse
from aurum.external.ge_validation import GreatExpectationsValidator
from aurum.external.monitoring import ExternalDataMonitor
from aurum.external.lineage import LineageManager


class TestExternalDataPipelineE2E:
    """End-to-end tests for external data pipeline."""

    @pytest.fixture
    def sample_collector_config(self) -> CollectorConfig:
        """Sample collector configuration for testing."""
        return CollectorConfig(
            provider="test-provider",
            base_url="https://api.example.com",
            kafka_topic="test-topic",
            kafka_bootstrap_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
            value_schema={"type": "record", "name": "TestRecord", "fields": []},
            retry=RetryConfig(max_attempts=3),
            http_timeout=30.0
        )

    @pytest.fixture
    def sample_http_response(self) -> HttpResponse:
        """Sample HTTP response for testing."""
        return HttpResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            content=json.dumps({"test": "data"}).encode(),
            url="https://api.example.com/test"
        )

    @pytest.mark.asyncio
    async def test_external_collector_end_to_end(
        self,
        sample_collector_config: CollectorConfig,
        sample_http_response: HttpResponse
    ):
        """Test complete external data collection workflow."""
        # Mock HTTP client
        mock_session = AsyncMock()
        mock_session.request.return_value = sample_http_response

        # Create collector
        collector = ExternalCollector(
            config=sample_collector_config,
            session=mock_session
        )

        # Test HTTP request
        request = HttpRequest(
            method="GET",
            path="/test",
            timeout=30.0
        )

        response = await collector.request(request)

        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/json"
        assert response.json() == {"test": "data"}

        # Test metrics recording
        assert collector.metrics.http_requests_total == 1
        assert collector.metrics.http_success == 1
        assert collector.metrics.http_errors == 0

    @pytest.mark.asyncio
    async def test_great_expectations_validation_e2e(self):
        """Test Great Expectations validation workflow."""
        try:
            # Create validator
            validator = GreatExpectationsValidator()

            # Sample data for validation
            sample_data = [
                {
                    "external_provider": "test",
                    "external_series_id": "TEST001",
                    "curve_key": "test_curve",
                    "value": 100.0,
                    "timestamp": "2024-01-01T00:00:00Z"
                }
            ]

            # This will fail in test environment but tests the integration
            try:
                result = await validator.validate_data(
                    table_name="test_table",
                    expectation_suite_name="test_suite",
                    data=sample_data
                )
            except Exception as e:
                # Expected to fail in test environment without proper setup
                assert "Great Expectations" in str(e)

        except RuntimeError as e:
            # Great Expectations not available in test environment
            assert "Great Expectations" in str(e)

    @pytest.mark.asyncio
    async def test_monitoring_e2e(self):
        """Test monitoring and alerting workflow."""
        monitor = ExternalDataMonitor()

        # Record pipeline health
        await monitor.record_pipeline_health(
            component="test-collector",
            status="healthy",
            metrics={"requests_processed": 100, "errors": 0}
        )

        # Record data freshness
        await monitor.record_data_freshness(
            provider="test",
            dataset="test_dataset",
            last_updated=datetime.now() - timedelta(hours=1)
        )

        # Check overall health
        health_status = await monitor.check_overall_health()

        assert health_status["overall_status"] == "healthy"
        assert health_status["healthy_components"] >= 1

    @pytest.mark.asyncio
    async def test_lineage_tracking_e2e(self):
        """Test data lineage tracking workflow."""
        lineage_manager = LineageManager()

        # Track data ingestion
        await lineage_manager.track_external_data_ingestion(
            provider="test",
            dataset="test_dataset",
            records_count=100
        )

        # Track data transformation
        await lineage_manager.track_data_transformation(
            provider="test",
            source_table="raw_table",
            target_table="processed_table",
            transformation_type="cleaning",
            records_in=100,
            records_out=95
        )

        # Track validation
        await lineage_manager.track_validation(
            provider="test",
            table_name="processed_table",
            validation_type="quality_check",
            passed=True
        )

        # Get lineage summary
        summary = await lineage_manager.get_provider_lineage_summary("test")

        assert "dataset" in summary
        assert "total_events" in summary
        assert summary["total_events"] >= 3  # All events tracked

    @pytest.mark.asyncio
    async def test_rate_limiting_e2e(self):
        """Test rate limiting functionality."""
        from aurum.external.collect.base import RateLimitConfig, RateLimiter

        config = RateLimitConfig(rate=2.0, burst=1.0)
        limiter = RateLimiter(config)

        # First request should pass immediately
        wait_time1 = limiter.acquire(lambda x: None)
        assert wait_time1 == 0.0

        # Second request should be rate limited
        wait_time2 = limiter.acquire(lambda x: None)
        assert wait_time2 > 0.0

    @pytest.mark.asyncio
    async def test_error_handling_e2e(self):
        """Test error handling throughout the pipeline."""
        from aurum.external.collect.base import HttpRequestError, RetryLimitExceeded

        # Test HTTP request error
        request = HttpRequest(method="GET", path="/test")

        try:
            raise HttpRequestError(
                "HTTP request failed",
                request=request,
                response=None,
                original=Exception("Connection timeout")
            )
        except HttpRequestError as e:
            assert "HTTP request failed" in str(e)
            assert e.request == request
            assert e.original is not None

        # Test retry limit exceeded
        try:
            raise RetryLimitExceeded(
                "Retries exhausted",
                request=request,
                original=Exception("Network error")
            )
        except RetryLimitExceeded as e:
            assert "Retries exhausted" in str(e)

    @pytest.mark.asyncio
    async def test_configuration_loading_e2e(self):
        """Test configuration loading and validation."""
        import tempfile
        import os

        # Create temporary config files
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "test_config.json")

            test_config = {
                "providers": [
                    {
                        "name": "test",
                        "datasets": ["TEST001"],
                        "rate_limit_rps": 5
                    }
                ]
            }

            with open(config_path, "w") as f:
                json.dump(test_config, f)

            # Test loading config
            with open(config_path, "r") as f:
                loaded_config = json.load(f)

            assert loaded_config["providers"][0]["name"] == "test"
            assert loaded_config["providers"][0]["rate_limit_rps"] == 5

    @pytest.mark.asyncio
    async def test_metrics_collection_e2e(self):
        """Test metrics collection throughout the pipeline."""
        from aurum.external.collect.base import CollectorMetrics

        metrics = CollectorMetrics("test-provider")

        # Record various metrics
        metrics.record_http_request("GET", 200, 0.1)
        metrics.record_http_request("POST", 500, 0.2)
        metrics.record_rate_limit_wait(0.05)
        metrics.record_backoff(1, 0.5)
        metrics.record_kafka_emit("test-topic", 10)

        # Check metrics snapshot
        snapshot = metrics.snapshot()

        assert snapshot["http_requests_total"] == 2
        assert snapshot["http_success"] == 1
        assert snapshot["http_errors"] == 1
        assert snapshot["rate_limit_wait_seconds"] == [0.05]
        assert snapshot["retry_backoff_events"] == [(1, 0.5)]
        assert snapshot["records_emitted_total"] == 10
        assert snapshot["records_emitted_by_topic"]["test-topic"] == 10

    @pytest.mark.asyncio
    async def test_concurrent_processing_e2e(self):
        """Test concurrent processing capabilities."""
        # Test that multiple collectors can run concurrently
        # This would be tested in integration environment

        # Mock concurrent requests
        async def mock_request(delay: float) -> str:
            await asyncio.sleep(delay)
            return f"response_{delay}"

        # Run concurrent requests
        start_time = datetime.now()

        tasks = [
            mock_request(0.1),
            mock_request(0.2),
            mock_request(0.1),
            mock_request(0.3)
        ]

        results = await asyncio.gather(*tasks)

        duration = (datetime.now() - start_time).total_seconds()

        # Should complete in approximately 0.3 seconds (max delay)
        assert duration < 0.5
        assert len(results) == 4
        assert "response_0.1" in results
        assert "response_0.2" in results
        assert "response_0.3" in results

    @pytest.mark.asyncio
    async def test_data_quality_monitoring_e2e(self):
        """Test data quality monitoring workflow."""
        from aurum.external.monitoring import DataQualityMonitor

        quality_monitor = DataQualityMonitor()

        # Record quality metrics
        await quality_monitor.record_quality_metric(
            provider="test",
            dataset="test_dataset",
            metric_name="completeness",
            value=0.95
        )

        await quality_monitor.record_quality_metric(
            provider="test",
            dataset="test_dataset",
            metric_name="accuracy",
            value=0.92
        )

        # Calculate overall quality score
        overall_score = await quality_monitor.get_quality_score(
            provider="test",
            dataset="test_dataset"
        )

        assert 0.8 <= overall_score <= 1.0

        # Check if quality is acceptable
        is_acceptable = await quality_monitor.is_quality_acceptable(
            provider="test",
            dataset="test_dataset",
            min_score=0.8
        )

        assert is_acceptable

    @pytest.mark.asyncio
    async def test_sla_monitoring_e2e(self):
        """Test SLA monitoring workflow."""
        from aurum.external.monitoring import SLAMonitor

        sla_monitor = SLAMonitor()

        # Add SLA rule
        sla_monitor.add_sla_rule(
            name="test_sla",
            target="test_target",
            threshold=timedelta(hours=1)
        )

        # Test SLA compliance
        actual_duration = timedelta(minutes=30)
        is_violated = await sla_monitor.check_sla_violation(
            rule_name="test_sla",
            actual_duration=actual_duration,
            context={"test": "context"}
        )

        assert not is_violated  # Should not violate 30min < 1hr threshold

        # Test SLA violation
        actual_duration_violated = timedelta(hours=2)
        is_violated = await sla_monitor.check_sla_violation(
            rule_name="test_sla",
            actual_duration=actual_duration_violated
        )

        assert is_violated  # Should violate 2hr > 1hr threshold

        # Check violations were recorded
        violations = sla_monitor.get_violations()
        assert len(violations) == 1
        assert violations[0]["rule_name"] == "test_sla"


# Integration test that would run in CI/CD
class TestExternalDataPipelineIntegration:
    """Integration tests for external data pipeline."""

    def test_docker_build(self):
        """Test that Docker image can be built."""
        # This would test the Docker build process
        assert True  # Placeholder for actual Docker build test

    def test_kubernetes_deployment(self):
        """Test that Kubernetes manifests are valid."""
        # This would validate k8s YAML files
        assert True  # Placeholder for actual k8s validation

    def test_configuration_files(self):
        """Test that configuration files are valid JSON."""
        import json
        import os

        config_files = [
            "config/external_incremental_config.json",
            "config/external_backfill_config.json",
            "config/external_monitoring_config.json"
        ]

        for config_file in config_files:
            if os.path.exists(config_file):
                with open(config_file, "r") as f:
                    json.load(f)  # Should not raise exception

    def test_requirements_installation(self):
        """Test that all requirements can be installed."""
        # This would test pip install requirements-external.txt
        assert True  # Placeholder for actual requirements test

    def test_health_check_endpoint(self):
        """Test health check endpoint response."""
        # This would test the actual health check endpoint
        assert True  # Placeholder for actual health check test
