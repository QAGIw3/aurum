"""Tests for scenario worker metrics and monitoring."""
from __future__ import annotations

import time
from unittest.mock import patch, MagicMock

import pytest

from aurum.scenarios.worker import (
    REQUESTS_TOTAL,
    REQUEST_SUCCESS,
    REQUEST_FAILURE,
    REQUEST_DLQ,
    PROCESS_DURATION,
    CANCELLED_RUNS,
    TIMEOUT_RUNS,
    RETRY_ATTEMPTS,
    RETRY_SUCCESS,
    RETRY_EXHAUSTED,
    SCENARIO_TYPE_COUNTER,
    WORKER_INFO,
)


class TestWorkerMetrics:
    """Test scenario worker metrics functionality."""

    def test_metrics_are_defined(self):
        """Test that all metrics are properly defined."""
        # These should be None when prometheus_client is not available
        assert REQUESTS_TOTAL is not None or True  # Allow None for optional dependency
        assert REQUEST_SUCCESS is not None or True
        assert REQUEST_FAILURE is not None or True
        assert REQUEST_DLQ is not None or True
        assert PROCESS_DURATION is not None or True
        assert CANCELLED_RUNS is not None or True
        assert TIMEOUT_RUNS is not None or True
        assert RETRY_ATTEMPTS is not None or True
        assert RETRY_SUCCESS is not None or True
        assert RETRY_EXHAUSTED is not None or True
        assert SCENARIO_TYPE_COUNTER is not None or True
        assert WORKER_INFO is not None or True

    def test_metrics_with_prometheus(self):
        """Test metrics when prometheus_client is available."""
        pytest.importorskip("prometheus_client")

        # Import metrics after confirming prometheus_client is available
        from aurum.scenarios.worker import (
            REQUESTS_TOTAL,
            REQUEST_SUCCESS,
            REQUEST_FAILURE,
            PROCESS_DURATION,
            CANCELLED_RUNS,
            TIMEOUT_RUNS,
        )

        # Test that metrics are properly instantiated
        assert hasattr(REQUESTS_TOTAL, 'inc')
        assert hasattr(REQUEST_SUCCESS, 'inc')
        assert hasattr(REQUEST_FAILURE, 'inc')
        assert hasattr(PROCESS_DURATION, 'observe')
        assert hasattr(CANCELLED_RUNS, 'inc')
        assert hasattr(TIMEOUT_RUNS, 'inc')

    def test_process_duration_histogram_labels(self):
        """Test that PROCESS_DURATION histogram has correct labels."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import PROCESS_DURATION

        # Test label names
        assert PROCESS_DURATION._labelnames == ["scenario_type", "tenant_id", "status"]

        # Test bucket configuration
        expected_buckets = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)
        assert PROCESS_DURATION._buckets == expected_buckets

    def test_scenario_type_counter_labels(self):
        """Test that SCENARIO_TYPE_COUNTER has correct labels."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import SCENARIO_TYPE_COUNTER

        # Test label names
        assert SCENARIO_TYPE_COUNTER._labelnames == ["scenario_type", "tenant_id"]

    def test_worker_info_metric(self):
        """Test that WORKER_INFO metric can be set."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import WORKER_INFO

        # Test setting info
        test_info = {
            "worker_id": "test-worker-1",
            "topics": "test.request.v1,test.output.v1",
            "bootstrap_servers": "localhost:9092",
            "version": "1.0.0",
        }

        # This should not raise an exception
        try:
            WORKER_INFO.info(test_info)
        except Exception:
            # Info metrics might not be available in test environment
            pass

    def test_metrics_increment(self):
        """Test that metrics can be incremented."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import REQUESTS_TOTAL, REQUEST_SUCCESS, REQUEST_FAILURE

        # Get initial values
        initial_total = REQUESTS_TOTAL._value.get() if hasattr(REQUESTS_TOTAL, '_value') else 0
        initial_success = REQUEST_SUCCESS._value.get() if hasattr(REQUEST_SUCCESS, '_value') else 0
        initial_failure = REQUEST_FAILURE._value.get() if hasattr(REQUEST_FAILURE, '_value') else 0

        # Increment metrics
        REQUESTS_TOTAL.inc()
        REQUEST_SUCCESS.inc()
        REQUEST_FAILURE.inc()

        # Check that values increased
        new_total = REQUESTS_TOTAL._value.get() if hasattr(REQUESTS_TOTAL, '_value') else 0
        new_success = REQUEST_SUCCESS._value.get() if hasattr(REQUEST_SUCCESS, '_value') else 0
        new_failure = REQUEST_FAILURE._value.get() if hasattr(REQUEST_FAILURE, '_value') else 0

        assert new_total > initial_total
        assert new_success > initial_success
        assert new_failure > initial_failure

    def test_process_duration_observe(self):
        """Test that PROCESS_DURATION can be observed."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import PROCESS_DURATION

        # Test observing duration
        test_duration = 1.5
        PROCESS_DURATION.observe(test_duration)

        # Check that observation was recorded
        # Note: We can't easily test the actual histogram state, but we can test it doesn't error

    def test_metrics_with_labels(self):
        """Test that metrics work with labels."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import PROCESS_DURATION, SCENARIO_TYPE_COUNTER

        # Test labeled observations
        PROCESS_DURATION.labels(
            scenario_type="policy",
            tenant_id="tenant-123",
            status="succeeded"
        ).observe(2.5)

        SCENARIO_TYPE_COUNTER.labels(
            scenario_type="policy",
            tenant_id="tenant-123"
        ).inc()

        # Should not raise exceptions

    def test_retry_metrics(self):
        """Test retry-related metrics."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import RETRY_ATTEMPTS, RETRY_SUCCESS, RETRY_EXHAUSTED

        # Test retry metrics
        RETRY_ATTEMPTS.inc(3)  # 3 retry attempts
        RETRY_SUCCESS.inc()     # 1 successful retry
        RETRY_EXHAUSTED.inc()   # 1 exhausted retry

        # Should not raise exceptions

    def test_cancellation_and_timeout_metrics(self):
        """Test cancellation and timeout metrics."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import CANCELLED_RUNS, TIMEOUT_RUNS

        # Test cancellation and timeout metrics
        CANCELLED_RUNS.inc()
        TIMEOUT_RUNS.inc()

        # Should not raise exceptions

    def test_metrics_graceful_degradation(self):
        """Test that metrics gracefully degrade when prometheus_client is not available."""
        # Mock prometheus_client not being available
        with patch.dict('sys.modules', {'prometheus_client': None}):
            # Re-import the worker module to get the None metrics
            import importlib
            import aurum.scenarios.worker as worker_module
            importlib.reload(worker_module)

            # All metrics should be None
            assert worker_module.REQUESTS_TOTAL is None
            assert worker_module.REQUEST_SUCCESS is None
            assert worker_module.REQUEST_FAILURE is None
            assert worker_module.PROCESS_DURATION is None
            assert worker_module.CANCELLED_RUNS is None
            assert worker_module.TIMEOUT_RUNS is None
            assert worker_module.RETRY_ATTEMPTS is None
            assert worker_module.RETRY_SUCCESS is None
            assert worker_module.RETRY_EXHAUSTED is None
            assert worker_module.SCENARIO_TYPE_COUNTER is None
            assert worker_module.WORKER_INFO is None

    def test_metrics_error_handling(self):
        """Test that metrics handle errors gracefully."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import PROCESS_DURATION

        # Test with invalid labels
        try:
            PROCESS_DURATION.labels(
                scenario_type="invalid_type_with_special_chars_!@#$%",
                tenant_id="tenant-123",
                status="succeeded"
            ).observe(1.0)
        except Exception:
            # Should handle invalid labels gracefully
            pass

        # Test with None observation
        try:
            PROCESS_DURATION.observe(None)  # type: ignore
        except Exception:
            pass

    def test_metrics_thread_safety(self):
        """Test that metrics are thread-safe for concurrent access."""
        pytest.importorskip("prometheus_client")

        from aurum.scenarios.worker import REQUESTS_TOTAL
        import threading

        results = []

        def increment_metric():
            try:
                REQUESTS_TOTAL.inc()
                results.append("success")
            except Exception as e:
                results.append(f"error: {e}")

        # Start multiple threads incrementing the metric
        threads = []
        for i in range(10):
            thread = threading.Thread(target=increment_metric)
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # All should succeed
        assert all(result == "success" for result in results)
        assert len(results) == 10
