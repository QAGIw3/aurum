"""Comprehensive tests for MISO provider implementation."""

import asyncio
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, List

from aurum.external.providers.miso import (
    CircuitBreaker,
    RateLimiter,
    DataQualityMetrics,
    DataValidator,
    MisoHealthMonitor,
    MisoApiConfig,
    MisoDatasetConfig,
    MisoApiClient,
    MisoCollector
)


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in closed state."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

        # Initially closed
        assert not cb.is_open()
        assert cb.get_state() == "CLOSED"

        # Record some successes
        cb.record_success()
        assert cb.get_state() == "CLOSED"

    def test_circuit_breaker_opens_after_failures(self):
        """Test circuit breaker opens after threshold failures."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=60)

        # Record failures
        cb.record_failure()
        assert cb.get_state() == "CLOSED"

        cb.record_failure()
        assert cb.get_state() == "OPEN"
        assert cb.is_open()

    def test_circuit_breaker_half_open_recovery(self):
        """Test circuit breaker transitions to half-open after timeout."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=1)

        cb.record_failure()
        assert cb.get_state() == "OPEN"

        # Wait for recovery timeout
        import time
        time.sleep(1.1)

        assert not cb.is_open()  # Should transition to HALF_OPEN
        assert cb.get_state() == "HALF_OPEN"

        # Success should close it
        cb.record_success()
        assert cb.get_state() == "CLOSED"


class TestRateLimiter:
    """Test adaptive rate limiter."""

    def test_rate_limiter_basic_functionality(self):
        """Test basic rate limiting."""
        rl = RateLimiter(requests_per_minute=10, requests_per_hour=100)

        # Should allow requests initially
        assert rl.can_make_request()

        # Record requests
        for _ in range(5):
            rl.record_request()

        assert rl.can_make_request()

    def test_rate_limiter_adaptive_behavior(self):
        """Test adaptive rate limiting."""
        rl = RateLimiter(requests_per_minute=10, requests_per_hour=100)

        # Record many requests
        for _ in range(12):
            rl.record_request()

        # Should now be more conservative
        assert rl._adaptive_factor < 1.0

        # Record success - should increase rate
        rl.adapt_rate(True)
        assert rl._adaptive_factor > 0.3

        # Record failure - should decrease rate
        rl.adapt_rate(False)
        assert rl._adaptive_factor < 0.8


class TestDataValidator:
    """Test data validation functionality."""

    def test_validate_valid_lmp_record(self):
        """Test validation of valid LMP record."""
        validator = DataValidator()

        record = {
            "location_id": "NODE_001",
            "price_total": 45.67,
            "market": "RTM"
        }

        is_valid, message = validator.validate_record(record, "lmp")
        assert is_valid
        assert message == "Valid record"

    def test_validate_invalid_record_missing_fields(self):
        """Test validation of record with missing fields."""
        validator = DataValidator()

        record = {
            "location_id": "NODE_001"
            # Missing price_total and market
        }

        is_valid, message = validator.validate_record(record, "lmp")
        assert not is_valid
        assert "Missing required fields" in message

    def test_validate_outlier_detection(self):
        """Test outlier detection."""
        validator = DataValidator()

        # Test extreme price
        record = {
            "location_id": "NODE_001",
            "price_total": 10000,  # Should be flagged as outlier
            "market": "RTM"
        }

        is_valid, message = validator.validate_record(record, "lmp")
        assert is_valid  # Valid but flagged as outlier
        assert validator.metrics.outlier_records == 1

    def test_data_quality_metrics(self):
        """Test data quality metrics calculation."""
        validator = DataValidator()

        # Add some test records
        validator.validate_record({"location_id": "NODE_001", "price_total": 45.67, "market": "RTM"}, "lmp")
        validator.validate_record({"location_id": "NODE_002"}, "lmp")  # Invalid

        metrics = validator.get_metrics()
        assert metrics["total_records"] == 2
        assert metrics["valid_records"] == 1
        assert metrics["invalid_records"] == 1
        assert metrics["quality_score"] == 0.5


class TestMisoHealthMonitor:
    """Test health monitoring functionality."""

    @pytest.mark.asyncio
    async def test_health_monitor_success_tracking(self):
        """Test successful operation tracking."""
        monitor = MisoHealthMonitor()

        await monitor.record_success()
        status = monitor.get_health_status()

        assert status["status"] == "healthy"
        assert status["consecutive_failures"] == 0

    @pytest.mark.asyncio
    async def test_health_monitor_failure_tracking(self):
        """Test failure tracking and alerting."""
        monitor = MisoHealthMonitor()

        # Record failures up to threshold
        for i in range(3):
            await monitor.record_failure(ValueError("Test error"))

        status = monitor.get_health_status()
        assert status["status"] == "degraded"
        assert status["consecutive_failures"] == 3

        # One more failure should trigger critical status
        await monitor.record_failure(ValueError("Critical error"))
        status = monitor.get_health_status()
        assert status["status"] == "critical"
        assert status["alerts_sent"] > 0


class TestMisoApiClient:
    """Test MISO API client."""

    @pytest.mark.asyncio
    async def test_api_client_initialization(self):
        """Test API client initialization."""
        config = MisoApiConfig(
            base_url="https://api.misoenergy.org",
            circuit_breaker_enabled=False
        )

        client = MisoApiClient(config)
        assert client.config == config
        assert client.circuit_breaker is None  # Disabled
        assert client.rate_limiter is not None

    @pytest.mark.asyncio
    async def test_api_client_with_circuit_breaker(self):
        """Test API client with circuit breaker enabled."""
        config = MisoApiConfig(
            base_url="https://api.misoenergy.org",
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=2
        )

        client = MisoApiClient(config)
        assert client.circuit_breaker is not None
        assert client.circuit_breaker.failure_threshold == 2

    @patch('requests.Session.get')
    @pytest.mark.asyncio
    async def test_api_client_successful_request(self, mock_get):
        """Test successful API request."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"data": [{"test": "data"}]}
        mock_response.headers.get.return_value = "application/json"
        mock_get.return_value = mock_response

        config = MisoApiConfig(base_url="https://api.misoenergy.org")
        client = MisoApiClient(config)

        result = await client.fetch_data("https://api.misoenergy.org/test", {})

        assert result == {"data": [{"test": "data"}]}
        assert client.circuit_breaker is None or client.circuit_breaker.get_state() == "CLOSED"

    @patch('requests.Session.get')
    @pytest.mark.asyncio
    async def test_api_client_with_fallback(self, mock_get):
        """Test API client with fallback URLs."""
        # Mock first URL failing, second succeeding
        mock_response_fail = MagicMock()
        mock_response_fail.raise_for_status.side_effect = Exception("Connection failed")

        mock_response_success = MagicMock()
        mock_response_success.raise_for_status.return_value = None
        mock_response_success.json.return_value = {"data": [{"test": "data"}]}
        mock_response_success.headers.get.return_value = "application/json"

        mock_get.side_effect = [mock_response_fail, mock_response_success]

        config = MisoApiConfig(base_url="https://api.misoenergy.org")
        client = MisoApiClient(config)

        result = await client.fetch_data(
            "https://api.misoenergy.org/test",
            {},
            fallback_urls=["https://backup.api.misoenergy.org/test"]
        )

        assert result == {"data": [{"test": "data"}]}


class TestMisoCollector:
    """Test MISO collector integration."""

    @pytest.mark.asyncio
    async def test_collector_initialization(self):
        """Test collector initialization."""
        config = MisoDatasetConfig(
            dataset_id="test_lmp",
            description="Test LMP data",
            data_type="lmp",
            market="RTM",
            url_template="https://api.misoenergy.org/lmp",
            interval_minutes=5,
            columns={"price": "price_total"},
            frequency="REALTIME",
            units="USD/MWh"
        )

        api_config = MisoApiConfig(base_url="https://api.misoenergy.org")

        collector = MisoCollector(config, api_config)

        assert collector.dataset_config == config
        assert collector.data_validator is not None
        assert collector.health_monitor is not None
        assert collector.collection_metrics["records_processed"] == 0

    def test_collector_metrics(self):
        """Test collector metrics collection."""
        config = MisoDatasetConfig(
            dataset_id="test_lmp",
            description="Test LMP data",
            data_type="lmp",
            market="RTM",
            url_template="https://api.misoenergy.org/lmp",
            interval_minutes=5,
            columns={"price": "price_total"},
            frequency="REALTIME",
            units="USD/MWh"
        )

        api_config = MisoApiConfig(base_url="https://api.misoenergy.org")

        collector = MisoCollector(config, api_config)

        # Test metrics collection
        collector.collection_metrics["records_processed"] = 100
        collector.collection_metrics["api_calls_made"] = 5
        collector.collection_metrics["validation_errors"] = 2

        metrics = collector.get_collection_metrics()

        assert metrics["records_processed"] == 100
        assert metrics["api_calls_made"] == 5
        assert metrics["validation_errors"] == 2
        assert "data_quality" in metrics
        assert "health_status" in metrics
        assert metrics["dataset_id"] == "test_lmp"

    def test_health_status(self):
        """Test health status reporting."""
        config = MisoDatasetConfig(
            dataset_id="test_lmp",
            description="Test LMP data",
            data_type="lmp",
            market="RTM",
            url_template="https://api.misoenergy.org/lmp",
            interval_minutes=5,
            columns={"price": "price_total"},
            frequency="REALTIME",
            units="USD/MWh"
        )

        api_config = MisoApiConfig(base_url="https://api.misoenergy.org")

        collector = MisoCollector(config, api_config)

        health_status = collector.get_health_status()

        assert "status" in health_status
        assert "consecutive_failures" in health_status
        assert "uptime_percentage" in health_status


# Integration tests
class TestMisoIntegration:
    """Integration tests for MISO components."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_with_rate_limiter(self):
        """Test circuit breaker and rate limiter working together."""
        config = MisoApiConfig(
            base_url="https://api.misoenergy.org",
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=2,
            requests_per_minute=30
        )

        client = MisoApiClient(config)

        # Verify both components are initialized
        assert client.circuit_breaker is not None
        assert client.rate_limiter is not None

        # Test initial state
        assert not client.circuit_breaker.is_open()
        assert client.rate_limiter.can_make_request()

    def test_data_validator_quality_score(self):
        """Test data quality score calculation."""
        validator = DataValidator()

        # Add mix of valid and invalid records
        for i in range(1, 11):
            if i % 3 == 0:
                # Every 3rd record is invalid
                is_valid, _ = validator.validate_record({"invalid": "data"}, "lmp")
                assert not is_valid
            else:
                is_valid, _ = validator.validate_record(
                    {"location_id": f"NODE_{i}", "price_total": 50.0, "market": "RTM"}, "lmp"
                )
                assert is_valid

        metrics = validator.get_metrics()
        assert metrics["total_records"] == 10
        assert metrics["valid_records"] == 7
        assert metrics["invalid_records"] == 3
        assert metrics["quality_score"] == 0.7


# Performance tests
class TestMisoPerformance:
    """Performance tests for MISO components."""

    def test_circuit_breaker_performance(self):
        """Test circuit breaker performance under load."""
        cb = CircuitBreaker(failure_threshold=10, recovery_timeout=1)

        # Record many failures quickly
        for _ in range(10):
            cb.record_failure()

        assert cb.get_state() == "OPEN"

        # Quick recovery
        cb.record_success()
        assert cb.get_state() == "CLOSED"

    def test_rate_limiter_performance(self):
        """Test rate limiter performance with many requests."""
        rl = RateLimiter(requests_per_minute=100, requests_per_hour=1000)

        # Should handle many requests efficiently
        for _ in range(100):
            assert rl.can_make_request()
            rl.record_request()

        # Should still be under limit
        assert rl.can_make_request()


# Test utilities
class TestMisoTestUtils:
    """Test utilities and helpers."""

    def test_create_test_config(self):
        """Test configuration creation for testing."""
        config = MisoDatasetConfig(
            dataset_id="test_dataset",
            description="Test dataset",
            data_type="lmp",
            market="RTM",
            url_template="https://test.api.misoenergy.org/data",
            interval_minutes=5,
            columns={"price": "price_total", "node": "location_id"},
            frequency="REALTIME",
            units="USD/MWh"
        )

        assert config.dataset_id == "test_dataset"
        assert config.data_type == "lmp"
        assert config.url_template == "https://test.api.misoenergy.org/data"

    def test_api_config_validation(self):
        """Test API configuration validation."""
        config = MisoApiConfig(
            base_url="https://api.misoenergy.org",
            requests_per_minute=50,
            requests_per_hour=3000,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout=60
        )

        assert config.base_url == "https://api.misoenergy.org"
        assert config.circuit_breaker_enabled
        assert config.circuit_breaker_threshold == 5
