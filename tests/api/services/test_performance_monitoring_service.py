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
async def test_check_performance_budgets_pass(monkeypatch, service, telemetry):
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

    evaluation_entries = [
        (name, labels, value)
        for name, labels, value in telemetry.metrics_client.counters
        if name == "aurum_performance_budget_evaluations_total"
    ]
    assert any(labels.get("status") == "compliant" for _, labels, _ in evaluation_entries)

    metrics_snapshot = await service.collect_prometheus_metrics()
    counters = metrics_snapshot.get("counters", {})
    assert "aurum_performance_budget_evaluations_total" in counters


@pytest.mark.asyncio
async def test_check_performance_budgets_violation(service, telemetry):
    budget = PerformanceBudget(
        endpoint="/v1/demo",
        method="GET",
        p95_latency_ms=120,
        p99_latency_ms=200,
        max_error_rate=0.02,
        min_throughput_rps=50,
        max_memory_mb=128,
        max_cpu_percent=25,
        description="Strict budget",
    )
    service.register_performance_budget(budget)

    service.record_live_metrics(
        "GET",
        "/v1/demo",
        {
            "p95_latency": 240.0,
            "p99_latency": 320.0,
            "error_rate": 0.05,
            "throughput_rps": 20.0,
            "memory_mb": 200.0,
            "cpu_percent": 45.0,
        },
    )

    results = await service.check_performance_budgets()
    assert results[budget.key] is False

    violation_entries = [
        (name, labels, value)
        for name, labels, value in telemetry.metrics_client.counters
        if name == "aurum_performance_budget_violations_total"
    ]
    assert any(labels.get("endpoint") == budget.endpoint for _, labels, _ in violation_entries)
    evaluations = [
        (name, labels, value)
        for name, labels, value in telemetry.metrics_client.counters
        if name == "aurum_performance_budget_evaluations_total"
    ]
    assert any(labels.get("status") == "violated" for _, labels, _ in evaluations)


@pytest.mark.asyncio
async def test_run_k6_scenario_simulated(monkeypatch, service, telemetry):
    monkeypatch.setattr(
        "src.aurum.api.services.performance_monitoring_service.shutil.which",
        lambda _: None,
    )

    test_id = await service.run_k6_scenario("api_smoke_test")
    result = service.get_test_result(test_id)

    assert result is not None
    assert result.status == "completed"
    assert result.metrics["throughput_rps"] > 0
    assert result.percentile_metrics

    test_entries = [
        (name, labels, value)
        for name, labels, value in telemetry.metrics_client.counters
        if name == "aurum_performance_tests_total"
    ]
    assert any(labels.get("scenario_id") == "api_smoke_test" for _, labels, _ in test_entries)


@pytest.mark.asyncio
async def test_compare_performance_reports_regression(service, telemetry):
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
    report = service.get_comparison(comparison_id)

    assert report is not None
    assert report.regression_detected is True
    assert report.metric_deltas["p95_latency"] > 0
    assert report.metric_deltas["throughput_rps"] < 0
    assert report.recommendations

    regression_checks = [
        (name, labels, value)
        for name, labels, value in telemetry.metrics_client.counters
        if name == "aurum_performance_regression_checks_total"
    ]
    assert any(labels.get("regression_detected") == "true" for _, labels, _ in regression_checks)

    regressions = [
        (name, labels, value)
        for name, labels, value in telemetry.metrics_client.counters
        if name == "aurum_performance_regressions_total"
    ]
    assert regressions
