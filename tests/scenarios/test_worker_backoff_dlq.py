"""Tests for scenario worker exponential backoff, DLQ, and poison-pill detection."""
from __future__ import annotations

import time
from unittest.mock import patch, MagicMock

import pytest

from aurum.scenarios.worker import (
    _backoff_delay,
    _is_poison_pill,
    _record_failure_for_circuit_breaker,
    _is_circuit_open,
    POISON_PILL_DETECTED,
    CIRCUIT_BREAKER_OPENED,
    CIRCUIT_BREAKER_CLOSED,
    CIRCUIT_BREAKER_REJECTED,
    BACKOFF_DELAY,
    _POISON_PILL_CACHE,
    _CIRCUIT_BREAKER_STATE,
)


class TestExponentialBackoff:
    """Test exponential backoff functionality."""

    def test_backoff_basic_calculation(self):
        """Test basic exponential backoff calculation."""
        assert _backoff_delay(1, 1.0) == 1.0
        assert _backoff_delay(2, 1.0) == 2.0
        assert _backoff_delay(3, 1.0) == 4.0
        assert _backoff_delay(4, 1.0) == 8.0

    def test_backoff_with_jitter(self):
        """Test backoff with jitter enabled."""
        # Test multiple calls to verify jitter is applied
        delays = [_backoff_delay(2, 1.0, jitter=True) for _ in range(10)]
        # All delays should be between 1.5 and 2.5 (75%-125% of 2.0)
        assert all(1.5 <= delay <= 2.5 for delay in delays)

    def test_backoff_without_jitter(self):
        """Test backoff without jitter."""
        delays = [_backoff_delay(2, 1.0, jitter=False) for _ in range(5)]
        # All delays should be exactly 2.0
        assert all(delay == 2.0 for delay in delays)

    def test_backoff_max_delay_cap(self):
        """Test that backoff is capped at maximum delay."""
        # Should be capped at 300 seconds (5 minutes)
        assert _backoff_delay(20, 1.0) == 300.0
        assert _backoff_delay(10, 60.0) == 300.0  # Base * 2^(10-1) = 30720, should be capped

    def test_backoff_invalid_attempts(self):
        """Test backoff with invalid attempt numbers."""
        assert _backoff_delay(0, 1.0) == 1.0  # Should use 1 as minimum
        assert _backoff_delay(-1, 1.0) == 1.0


class TestPoisonPillDetection:
    """Test poison-pill detection functionality."""

    def setup_method(self):
        """Clear poison pill cache before each test."""
        _POISON_PILL_CACHE.clear()

    def test_poison_pill_not_detected_initially(self):
        """Test that messages are not considered poison pills initially."""
        assert not _is_poison_pill("test-message", "TestError")

    def test_poison_pill_detection_after_many_failures(self):
        """Test poison pill detection after many failures."""
        message_key = "test-scenario:test-tenant"
        error_type = "TestError"

        # Record 11 failures (exceeds threshold of 10)
        for i in range(11):
            assert not _is_poison_pill(message_key, error_type)

        # 11th failure should trigger poison pill detection
        assert _is_poison_pill(message_key, error_type)

    def test_poison_pill_detection_recent_failures(self):
        """Test poison pill detection based on recent failures."""
        message_key = "test-scenario:test-tenant"
        error_type = "TestError"

        # Record failures with recent timing
        for i in range(6):
            assert not _is_poison_pill(message_key, error_type)
            # Simulate recent failures by calling again quickly
            assert _is_poison_pill(message_key, error_type)

    def test_poison_pill_different_error_types(self):
        """Test that different error types have separate poison pill tracking."""
        message_key = "test-scenario:test-tenant"

        # Should not be poison pill for different error types
        assert not _is_poison_pill(message_key, "ErrorType1")
        assert not _is_poison_pill(message_key, "ErrorType2")

        # Make ErrorType1 a poison pill
        for i in range(11):
            _is_poison_pill(message_key, "ErrorType1")

        assert _is_poison_pill(message_key, "ErrorType1")
        assert not _is_poison_pill(message_key, "ErrorType2")

    def test_poison_pill_cache_structure(self):
        """Test that poison pill cache stores correct information."""
        message_key = "test-scenario:test-tenant"
        error_type = "TestError"

        _is_poison_pill(message_key, error_type)

        cache_key = f"{message_key}:{error_type}"
        assert cache_key in _POISON_PILL_CACHE
        entry = _POISON_PILL_CACHE[cache_key]
        assert "failure_count" in entry
        assert "first_failure" in entry
        assert "last_failure" in entry
        assert entry["failure_count"] == 1


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    def setup_method(self):
        """Clear circuit breaker state before each test."""
        _CIRCUIT_BREAKER_STATE.clear()

    def test_circuit_breaker_initially_closed(self):
        """Test that circuit breaker is initially closed."""
        assert not _is_circuit_open("TestError", "test-tenant")

    def test_circuit_breaker_opens_after_failures(self):
        """Test circuit breaker opens after multiple failures."""
        error_type = "TestError"
        tenant_id = "test-tenant"

        # Record failures rapidly
        for i in range(3):
            result = _record_failure_for_circuit_breaker(error_type, tenant_id)
            if i < 2:  # First two failures shouldn't open circuit
                assert not result
            else:  # Third failure should open circuit
                assert result

        assert _is_circuit_open(error_type, tenant_id)

    def test_circuit_breaker_closes_after_timeout(self):
        """Test circuit breaker closes after timeout period."""
        error_type = "TestError"
        tenant_id = "test-tenant"

        # Open circuit breaker
        _record_failure_for_circuit_breaker(error_type, tenant_id)
        _record_failure_for_circuit_breaker(error_type, tenant_id)
        _record_failure_for_circuit_breaker(error_type, tenant_id)

        assert _is_circuit_open(error_type, tenant_id)

        # Simulate time passing (more than 10 minutes)
        with patch('aurum.scenarios.worker.time.time') as mock_time:
            mock_time.return_value = time.time() + 601  # 10 minutes + 1 second
            assert not _is_circuit_open(error_type, tenant_id)

    def test_circuit_breaker_different_tenants(self):
        """Test that circuit breakers are isolated by tenant."""
        error_type = "TestError"

        # Open circuit for tenant1
        for i in range(3):
            _record_failure_for_circuit_breaker(error_type, "tenant1")

        assert _is_circuit_open(error_type, "tenant1")
        assert not _is_circuit_open(error_type, "tenant2")

    def test_circuit_breaker_different_error_types(self):
        """Test that circuit breakers are isolated by error type."""
        tenant_id = "test-tenant"

        # Open circuit for ErrorType1
        for i in range(3):
            _record_failure_for_circuit_breaker("ErrorType1", tenant_id)

        assert _is_circuit_open("ErrorType1", tenant_id)
        assert not _is_circuit_open("ErrorType2", tenant_id)

    def test_circuit_breaker_state_structure(self):
        """Test that circuit breaker state stores correct information."""
        error_type = "TestError"
        tenant_id = "test-tenant"

        _record_failure_for_circuit_breaker(error_type, tenant_id)

        cb_key = f"{error_type}:{tenant_id}"
        assert cb_key in _CIRCUIT_BREAKER_STATE
        state = _CIRCUIT_BREAKER_STATE[cb_key]
        assert "failure_count" in state
        assert "last_failure" in state
        assert "circuit_open" in state
        assert "circuit_opened_at" in state
        assert state["failure_count"] == 1


class TestBackoffDLQMetrics:
    """Test metrics for backoff, DLQ, and poison-pill detection."""

    def test_poison_pill_metrics_increment(self):
        """Test that poison pill metrics are properly incremented."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import POISON_PILL_DETECTED

        initial_value = POISON_PILL_DETECTED._value.get() if hasattr(POISON_PILL_DETECTED, '_value') else 0

        POISON_PILL_DETECTED.inc()

        new_value = POISON_PILL_DETECTED._value.get() if hasattr(POISON_PILL_DETECTED, '_value') else 0
        assert new_value > initial_value

    def test_circuit_breaker_metrics_increment(self):
        """Test that circuit breaker metrics are properly incremented."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import CIRCUIT_BREAKER_OPENED, CIRCUIT_BREAKER_CLOSED

        initial_opened = CIRCUIT_BREAKER_OPENED._value.get() if hasattr(CIRCUIT_BREAKER_OPENED, '_value') else 0
        initial_closed = CIRCUIT_BREAKER_CLOSED._value.get() if hasattr(CIRCUIT_BREAKER_CLOSED, '_value') else 0

        CIRCUIT_BREAKER_OPENED.inc()
        CIRCUIT_BREAKER_CLOSED.inc()

        new_opened = CIRCUIT_BREAKER_OPENED._value.get() if hasattr(CIRCUIT_BREAKER_OPENED, '_value') else 0
        new_closed = CIRCUIT_BREAKER_CLOSED._value.get() if hasattr(CIRCUIT_BREAKER_CLOSED, '_value') else 0

        assert new_opened > initial_opened
        assert new_closed > initial_closed

    def test_backoff_delay_metrics(self):
        """Test that backoff delay metrics are properly recorded."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import BACKOFF_DELAY

        # Test that backoff delay can be observed
        BACKOFF_DELAY.labels(attempt="3").observe(4.0)

        # Should not raise exception

    def test_metrics_graceful_degradation(self):
        """Test that metrics gracefully degrade when prometheus_client is not available."""
        with patch.dict('sys.modules', {'prometheus_client': None}):
            # Re-import the worker module to get the None metrics
            import importlib
            import aurum.scenarios.worker as worker_module
            importlib.reload(worker_module)

            # All metrics should be None
            assert worker_module.POISON_PILL_DETECTED is None
            assert worker_module.CIRCUIT_BREAKER_OPENED is None
            assert worker_module.CIRCUIT_BREAKER_CLOSED is None
            assert worker_module.CIRCUIT_BREAKER_REJECTED is None
            assert worker_module.BACKOFF_DELAY is None

    def test_metrics_error_handling(self):
        """Test that metrics handle errors gracefully."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import POISON_PILL_DETECTED, BACKOFF_DELAY

        # Test with None metric
        try:
            # This should not crash even if metric is None
            result = POISON_PILL_DETECTED is not None and POISON_PILL_DETECTED.inc()
        except Exception:
            pass

        # Test backoff delay with invalid labels
        try:
            BACKOFF_DELAY.labels(attempt="invalid!@#$").observe(1.0)
        except Exception:
            pass

    def test_circuit_breaker_rejected_metrics(self):
        """Test that circuit breaker rejected metrics work."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import CIRCUIT_BREAKER_REJECTED

        initial_value = CIRCUIT_BREAKER_REJECTED._value.get() if hasattr(CIRCUIT_BREAKER_REJECTED, '_value') else 0

        CIRCUIT_BREAKER_REJECTED.inc()

        new_value = CIRCUIT_BREAKER_REJECTED._value.get() if hasattr(CIRCUIT_BREAKER_REJECTED, '_value') else 0
        assert new_value > initial_value


class TestIntegrationScenarios:
    """Test integration scenarios for backoff, DLQ, and poison-pill detection."""

    def setup_method(self):
        """Clear caches before each test."""
        _POISON_PILL_CACHE.clear()
        _CIRCUIT_BREAKER_STATE.clear()

    def test_normal_retry_flow(self):
        """Test normal retry flow without poison pill detection."""
        message_key = "normal-scenario:test-tenant"
        error_type = "ConnectionError"

        # Simulate normal failures that should be retried
        for attempt in range(3):
            is_poison = _is_poison_pill(message_key, error_type)
            assert not is_poison, f"Should not be poison pill on attempt {attempt + 1}"

    def test_poison_pill_flow(self):
        """Test poison pill detection and DLQ flow."""
        message_key = "poison-scenario:test-tenant"
        error_type = "DatabaseError"

        # Simulate many failures to trigger poison pill
        for attempt in range(15):
            is_poison = _is_poison_pill(message_key, error_type)
            if attempt >= 10:  # Should be poison pill after 10 failures
                assert is_poison, f"Should be poison pill on attempt {attempt + 1}"
            else:
                assert not is_poison, f"Should not be poison pill on attempt {attempt + 1}"

    def test_circuit_breaker_flow(self):
        """Test circuit breaker opening and closing flow."""
        error_type = "APIError"
        tenant_id = "test-tenant"

        # Record rapid failures to open circuit
        for i in range(4):
            should_open = _record_failure_for_circuit_breaker(error_type, tenant_id)
            if i >= 2:  # Should open on 3rd failure
                assert should_open, f"Circuit should open on attempt {i + 1}"

        # Circuit should be open
        assert _is_circuit_open(error_type, tenant_id)

        # Test that circuit stays open
        assert _is_circuit_open(error_type, tenant_id)

    def test_circuit_breaker_timeout_flow(self):
        """Test circuit breaker closing after timeout."""
        error_type = "APIError"
        tenant_id = "test-tenant"

        # Open circuit
        for i in range(3):
            _record_failure_for_circuit_breaker(error_type, tenant_id)

        assert _is_circuit_open(error_type, tenant_id)

        # Simulate time passing and circuit closing
        with patch('aurum.scenarios.worker.time.time') as mock_time:
            mock_time.return_value = time.time() + 601  # 10 minutes + 1 second
            assert not _is_circuit_open(error_type, tenant_id)

    def test_mixed_scenario_types(self):
        """Test different scenarios with different error types and tenants."""
        scenarios = [
            ("scenario1", "tenant1", "ErrorType1"),
            ("scenario2", "tenant1", "ErrorType2"),
            ("scenario1", "tenant2", "ErrorType1"),
        ]

        for scenario_id, tenant_id, error_type in scenarios:
            message_key = f"{scenario_id}:{tenant_id}"
            is_poison = _is_poison_pill(message_key, error_type)
            assert not is_poison, f"Should not be poison pill initially for {message_key}:{error_type}"

        # Make one scenario a poison pill
        for i in range(15):
            _is_poison_pill("scenario1:tenant1", "ErrorType1")

        assert _is_poison_pill("scenario1:tenant1", "ErrorType1")
        assert not _is_poison_pill("scenario2:tenant1", "ErrorType2")
        assert not _is_poison_pill("scenario1:tenant2", "ErrorType1")
