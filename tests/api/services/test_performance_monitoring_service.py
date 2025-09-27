import asyncio
from datetime import datetime

import pytest

from src.aurum.api.services.performance_monitoring_service import (
    LoadTestScenario,
    PerformanceBudget,
    PerformanceMonitoringService,
    PerformanceTestResult,
)


class StubMetricsClient:
    def __init__(self):
        self.counters = []

    def counter(self, name: str, labels=None, value=1):
        self.counters.append((name, labels or {}, value))

    def export(self):
        return {"content_type": "text/plain", "payload": b""}


class StubTelemetry:
    def __init__(self):
        self.metrics_client = StubMetricsClient()
        self.logs = []

    def info(self, message: str, **metadata):  # noqa: D401 - simple log stub
        self.logs.append(("info", message, metadata))

    def warning(self, message: str, **metadata):
        self.logs.append(("warning", message, metadata))

    def error(self, message: str, **metadata):
        self.logs.append(("error", message, metadata))


@pytest.fixture()
def telemetry():
    return StubTelemetry()


@pytest.fixture()
def service(monkeypatch, telemetry):
    monkeypatch.setattr(
        "src.aurum.api.services.performance_monitoring_service.get_unified_cache_manager",
        lambda: object(),
    )
    return PerformanceMonitoringService(telemetry=telemetry)


@pytest.mark.asyncio
async def test_check_performance_budgets_pass(monkeypatch, service):
    budget = PerformanceBudget(
        endpoint="/v1/demo",
        method="GET",
        p95_latency_ms=200,
        p99_latency_ms=400,
        max_error_rate=0.05,
        min_throughput_rps=30,
        max_memory_mb=256,
        max_cpu_percent=50,
        description="Demo endpoint",
    )
    service.register_performance_budget(budget)

    service.record_live_metrics(
        "GET",
        "/v1/demo",
        {
            "p95_latency": 150.0,
            "p99_latency": 250.0,
            "error_rate": 0.01,
            "throughput_rps": 45.0,
            "memory_mb": 128.0,
            "cpu_percent": 25.0,
        },
    )

    results = await service.check_performance_budgets()
    assert results[budget.key] is True


@pytest.mark.asyncio
async def test_run_k6_scenario_simulated(monkeypatch, service):
    monkeypatch.setattr(
        "src.aurum.api.services.performance_monitoring_service.shutil.which",
        lambda _: None,
    )

    test_id = await service.run_k6_scenario("api_smoke_test")
    result = service._test_results[test_id]

    assert result.status == "completed"
    assert result.metrics["throughput_rps"] > 0
    assert result.percentile_metrics


@pytest.mark.asyncio
async def test_compare_performance_reports_regression(service):
    baseline = PerformanceTestResult(
        test_id="baseline",
        scenario_id="api_smoke_test",
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow(),
        status="completed",
        metrics={
            "p95_latency": 300.0,
            "p99_latency": 600.0,
            "error_rate": 0.02,
            "throughput_rps": 80.0,
        },
        percentile_metrics={"p95_latency": 300.0},
    )
    current = PerformanceTestResult(
        test_id="current",
        scenario_id="api_smoke_test",
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow(),
        status="completed",
        metrics={
            "p95_latency": 420.0,
            "p99_latency": 780.0,
            "error_rate": 0.04,
            "throughput_rps": 60.0,
        },
        percentile_metrics={"p95_latency": 420.0},
    )

    service._test_results[baseline.test_id] = baseline
    service._test_results[current.test_id] = current

    comparison_id = await service.compare_performance(baseline.test_id, current.test_id)
    report = service._reports[comparison_id]

    assert report.regression_detected is True
    assert report.metric_deltas["p95_latency"] > 0
    assert report.metric_deltas["throughput_rps"] < 0
    assert report.recommendations
