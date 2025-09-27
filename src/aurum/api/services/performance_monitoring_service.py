"""Performance Monitoring and Regression Harness Service.

This service provides:
- API performance budgets (p95/p99) with automated monitoring
- k6 load testing scenario automation and execution
- Prometheus metrics collection and comparison
- CI/CD integration with performance gating
- Regression detection and alerting
- Performance trend analysis and reporting
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4

import psutil
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from ..observability.telemetry_facade import get_telemetry_facade, MetricCategory
from ..cache.consolidated_manager import get_unified_cache_manager


class PerformanceBudget(BaseModel):
    """Performance budget definition."""

    endpoint: str
    method: str
    p95_latency_ms: float
    p99_latency_ms: float
    max_error_rate: float  # 0.0 to 1.0
    min_throughput_rps: float
    max_memory_mb: float
    max_cpu_percent: float
    description: str
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)


class LoadTestScenario(BaseModel):
    """k6 load test scenario configuration."""

    scenario_id: str
    name: str
    description: str
    script_path: str
    duration_seconds: int = 300
    virtual_users: int = 50
    ramp_up_seconds: int = 30
    ramp_down_seconds: int = 30
    environment: str = "staging"
    tags: Dict[str, str] = field(default_factory=dict)
    thresholds: Dict[str, str] = field(default_factory=dict)
    enabled: bool = True


class PerformanceTestResult(BaseModel):
    """Performance test execution result."""

    test_id: str
    scenario_id: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str = "running"  # "running", "completed", "failed", "cancelled"
    metrics: Dict[str, float] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    k6_output: Optional[str] = None
    prometheus_data: Optional[Dict[str, Any]] = None


class PerformanceComparison(BaseModel):
    """Performance comparison between test runs."""

    comparison_id: str
    baseline_test_id: str
    current_test_id: str
    comparison_metrics: Dict[str, float]
    regression_detected: bool
    improvement_areas: List[str]
    degradation_areas: List[str]
    recommendations: List[str]
    created_at: datetime = field(default_factory=datetime.utcnow)


class PerformanceMonitoringService:
    """Performance Monitoring and Regression Harness Service."""

    def __init__(self):
        """Initialize performance monitoring service."""
        self.cache_manager = get_unified_cache_manager()
        self.telemetry = get_telemetry_facade()

        # Performance state
        self._budgets: Dict[str, PerformanceBudget] = {}
        self._scenarios: Dict[str, LoadTestScenario] = {}
        self._test_results: Dict[str, PerformanceTestResult] = {}
        self._comparisons: Dict[str, PerformanceComparison] = {}

        # Real-time metrics
        self._current_metrics: Dict[str, Any] = {}
        self._metric_history: Dict[str, List[Tuple[datetime, float]]] = defaultdict(list)

        # CI/CD integration
        self._ci_thresholds: Dict[str, float] = {}

        # Initialize default budgets and scenarios
        self._initialize_default_budgets()
        self._initialize_default_scenarios()

    def _initialize_default_budgets(self) -> None:
        """Initialize default performance budgets."""
        default_budgets = [
            PerformanceBudget(
                endpoint="/health",
                method="GET",
                p95_latency_ms=100,
                p99_latency_ms=200,
                max_error_rate=0.01,
                min_throughput_rps=100,
                max_memory_mb=50,
                max_cpu_percent=10,
                description="Health check endpoint performance"
            ),
            PerformanceBudget(
                endpoint="/v2/curves",
                method="GET",
                p95_latency_ms=500,
                p99_latency_ms=1000,
                max_error_rate=0.05,
                min_throughput_rps=50,
                max_memory_mb=200,
                max_cpu_percent=20,
                description="Curve data retrieval performance"
            ),
            PerformanceBudget(
                endpoint="/v2/forecasting",
                method="POST",
                p95_latency_ms=2000,
                p99_latency_ms=5000,
                max_error_rate=0.1,
                min_throughput_rps=10,
                max_memory_mb=500,
                max_cpu_percent=30,
                description="Forecasting endpoint performance"
            )
        ]

        for budget in default_budgets:
            self._budgets[f"{budget.method}:{budget.endpoint}"] = budget

    def _initialize_default_scenarios(self) -> None:
        """Initialize default k6 load test scenarios."""
        default_scenarios = [
            LoadTestScenario(
                scenario_id="api_smoke_test",
                name="API Smoke Test",
                description="Basic functionality test for all endpoints",
                script_path="scripts/k6/smoke_test.js",
                duration_seconds=60,
                virtual_users=10,
                environment="staging",
                thresholds={
                    "http_req_duration": "p95<500",
                    "http_req_failed": "rate<0.1"
                }
            ),
            LoadTestScenario(
                scenario_id="load_test_curves",
                name="Curve Data Load Test",
                description="High load test for curve data endpoints",
                script_path="scripts/k6/curves_load_test.js",
                duration_seconds=300,
                virtual_users=100,
                environment="staging",
                thresholds={
                    "http_req_duration": "p95<1000",
                    "http_req_failed": "rate<0.05"
                }
            ),
            LoadTestScenario(
                scenario_id="stress_test_forecasting",
                name="Forecasting Stress Test",
                description="Stress test for forecasting endpoints",
                script_path="scripts/k6/forecasting_stress_test.js",
                duration_seconds=600,
                virtual_users=200,
                environment="staging",
                thresholds={
                    "http_req_duration": "p99<5000",
                    "http_req_failed": "rate<0.1"
                }
            )
        ]

        for scenario in default_scenarios:
            self._scenarios[scenario.scenario_id] = scenario

    async def check_performance_budgets(self) -> Dict[str, bool]:
        """Check if current performance meets budget requirements."""
        results = {}

        for budget_key, budget in self._budgets.items():
            if not budget.enabled:
                continue

            # Get current metrics for this endpoint
            current_metrics = self._current_metrics.get(budget_key, {})

            # Check each budget constraint
            budget_met = True
            violations = []

            # Latency checks
            if 'p95_latency' in current_metrics:
                if current_metrics['p95_latency'] > budget.p95_latency_ms:
                    budget_met = False
                    violations.append(f"p95 latency {current_metrics['p95_latency']}ms > {budget.p95_latency_ms}ms")

            if 'p99_latency' in current_metrics:
                if current_metrics['p99_latency'] > budget.p99_latency_ms:
                    budget_met = False
                    violations.append(f"p99 latency {current_metrics['p99_latency']}ms > {budget.p99_latency_ms}ms")

            # Error rate check
            if 'error_rate' in current_metrics:
                if current_metrics['error_rate'] > budget.max_error_rate:
                    budget_met = False
                    violations.append(f"error rate {current_metrics['error_rate']".2%"} > {budget.max_error_rate".2%"}")

            results[budget_key] = budget_met

            if not budget_met:
                self.telemetry.warning("Performance budget violation", budget_key=budget_key, violations=violations)

        return results

    async def run_k6_scenario(self, scenario_id: str) -> str:
        """Execute k6 load test scenario."""
        scenario = self._scenarios.get(scenario_id)
        if not scenario:
            raise ValueError(f"Scenario {scenario_id} not found")

        test_id = str(uuid4())
        result = PerformanceTestResult(
            test_id=test_id,
            scenario_id=scenario_id,
            start_time=datetime.utcnow()
        )

        self._test_results[test_id] = result

        try:
            # Execute k6 test (simplified implementation)
            k6_command = [
                "k6", "run",
                f"--duration={scenario.duration_seconds}s",
                f"--vus={scenario.virtual_users}",
                f"--ramp-up-duration={scenario.ramp_up_seconds}s",
                f"--ramp-down-duration={scenario.ramp_down_seconds}s",
                scenario.script_path
            ]

            # Add thresholds as command line arguments
            for threshold_name, threshold_value in scenario.thresholds.items():
                k6_command.extend([f"--tag", f"threshold_{threshold_name}={threshold_value}"])

            # Execute k6 (mock implementation)
            await self._execute_k6_test(k6_command, result)

            result.status = "completed"
            result.end_time = datetime.utcnow()

            # Extract metrics from k6 output
            result.metrics = self._parse_k6_metrics(result.k6_output or "")

            self.telemetry.info("k6 test completed", test_id=test_id, scenario_id=scenario_id)

        except Exception as e:
            result.status = "failed"
            result.end_time = datetime.utcnow()
            result.errors.append(str(e))
            self.telemetry.error("k6 test failed", test_id=test_id, error=str(e))

        return test_id

    async def _execute_k6_test(self, command: List[str], result: PerformanceTestResult) -> None:
        """Execute k6 test command."""
        try:
            # Mock k6 execution - in reality would run the actual command
            await asyncio.sleep(2)  # Simulate execution time

            # Mock k6 output
            result.k6_output = """
            execution: local
               script: scripts/k6/curves_load_test.js
               output: -

             scenarios: (100.00%) 1 scenario, 100 max VUs, 6m30s max duration (incl. graceful stop):
                      * default: 6m0s duration, 100 max VUs, 100000 max iters (100000 total)

             data_received..................: 1.2 GB  200 kB/s
             data_sent......................: 50 MB   8.3 kB/s
             http_req_blocked...............: avg=2.34ms   min=0s      med=0s      max=1.23s    p(90)=1ms     p(95)=5ms
             http_req_connecting............: avg=1.23ms   min=0s      med=0s      max=500ms    p(90)=0s      p(95)=0s
             http_req_duration..............: avg=234.56ms min=12.34ms med=198.76ms max=5.67s   p(90)=456.78ms p(95)=678.90ms
             http_req_failed................: 0.02%   20 out of 100000
             http_req_receiving.............: avg=1.23ms   min=0s      med=0s      max=234ms    p(90)=2ms     p(95)=5ms
             http_req_sending...............: avg=0.56ms   min=0s      med=0s      max=123ms    p(90)=1ms     p(95)=2ms
             http_req_tls_handshaking.......: avg=1.23ms   min=0s      med=0s      max=500ms    p(90)=0s      p(95)=0s
             http_req_waiting...............: avg=230.12ms min=10.23ms med=195.67ms max=5.45s   p(90)=445.67ms p(95)=667.89ms
             http_reqs......................: 100000  278.89/s
             iteration_duration.............: avg=245.67ms min=15.67ms med=210.34ms max=6.78s   p(90)=467.89ms p(95)=689.01ms
             iterations.....................: 100000  278.89/s
             vus............................: 100     min=100 max=100
             vus_max........................: 100     min=100 max=100
            """

        except Exception as e:
            result.errors.append(f"k6 execution failed: {str(e)}")

    def _parse_k6_metrics(self, k6_output: str) -> Dict[str, float]:
        """Parse metrics from k6 output."""
        metrics = {}

        try:
            lines = k6_output.split('\n')
            for line in lines:
                if 'http_req_duration' in line and 'p(95)' in line:
                    parts = line.split()
                    if len(parts) >= 5:
                        metrics['p95_latency'] = float(parts[4].replace('ms', ''))

                if 'http_req_failed' in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        metrics['error_rate'] = float(parts[2].replace('%', '')) / 100

                if 'http_reqs' in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        metrics['throughput_rps'] = float(parts[2])

        except Exception as e:
            self.telemetry.error("k6 metrics parsing failed", error=str(e))

        return metrics

    async def collect_prometheus_metrics(self) -> Dict[str, Any]:
        """Collect Prometheus metrics for comparison."""
        try:
            # Mock Prometheus metrics collection
            metrics = {
                "api_request_duration_seconds": {
                    "p50": 0.2,
                    "p95": 0.5,
                    "p99": 1.0
                },
                "api_requests_total": {
                    "rate_per_second": 100.0
                },
                "api_errors_total": {
                    "rate_per_second": 1.0
                },
                "memory_usage_bytes": 50000000,  # 50MB
                "cpu_usage_percent": 15.0
            }

            return metrics

        except Exception as e:
            self.telemetry.error("Prometheus metrics collection failed", error=str(e))
            return {}

    async def compare_performance(self, baseline_test_id: str, current_test_id: str) -> str:
        """Compare two performance test results."""
        comparison_id = str(uuid4())

        baseline = self._test_results.get(baseline_test_id)
        current = self._test_results.get(current_test_id)

        if not baseline or not current:
            raise ValueError("Test results not found")

        # Compare metrics
        comparison_metrics = {}
        regression_detected = False
        improvement_areas = []
        degradation_areas = []

        for metric_name in ['p95_latency', 'p99_latency', 'error_rate', 'throughput_rps']:
            baseline_value = baseline.metrics.get(metric_name, 0)
            current_value = current.metrics.get(metric_name, 0)

            if baseline_value > 0:
                change_percent = ((current_value - baseline_value) / baseline_value) * 100
                comparison_metrics[f"{metric_name}_change_percent"] = change_percent

                # Check for regression
                if metric_name in ['p95_latency', 'p99_latency', 'error_rate']:
                    if change_percent > 10:  # 10% degradation threshold
                        regression_detected = True
                        degradation_areas.append(f"{metric_name} increased by {change_percent".1f"}%")
                elif metric_name == 'throughput_rps':
                    if change_percent < -10:  # 10% throughput decrease
                        regression_detected = True
                        degradation_areas.append(f"{metric_name} decreased by {abs(change_percent)".1f"}%")
                    elif change_percent > 10:
                        improvement_areas.append(f"{metric_name} increased by {change_percent".1f"}%")

        # Generate recommendations
        recommendations = []
        if regression_detected:
            recommendations.extend([
                "Performance regression detected - investigate recent changes",
                "Consider optimizing database queries",
                "Review caching strategies",
                "Check resource allocation and scaling"
            ])
        else:
            recommendations.append("Performance within acceptable limits")

        comparison = PerformanceComparison(
            comparison_id=comparison_id,
            baseline_test_id=baseline_test_id,
            current_test_id=current_test_id,
            comparison_metrics=comparison_metrics,
            regression_detected=regression_detected,
            improvement_areas=improvement_areas,
            degradation_areas=degradation_areas,
            recommendations=recommendations
        )

        self._comparisons[comparison_id] = comparison
        return comparison_id

    async def get_performance_dashboard(self) -> Dict[str, Any]:
        """Get comprehensive performance dashboard."""
        # Check current budget compliance
        budget_compliance = await self.check_performance_budgets()

        # Get recent test results
        recent_tests = [
            test for test in self._test_results.values()
            if test.status == "completed" and test.end_time
        ]
        recent_tests.sort(key=lambda x: x.end_time, reverse=True)
        recent_tests = recent_tests[:10]  # Last 10 tests

        # Calculate performance trends
        trends = self._calculate_performance_trends()

        return {
            "budget_compliance": budget_compliance,
            "recent_tests": [
                {
                    "test_id": test.test_id,
                    "scenario_id": test.scenario_id,
                    "status": test.status,
                    "metrics": test.metrics,
                    "start_time": test.start_time,
                    "end_time": test.end_time
                }
                for test in recent_tests
            ],
            "performance_trends": trends,
            "recommendations": self._generate_performance_recommendations(budget_compliance, trends),
            "last_updated": datetime.utcnow()
        }

    def _calculate_performance_trends(self) -> Dict[str, Any]:
        """Calculate performance trends from historical data."""
        trends = {}

        # Analyze metric history for trends
        for metric_name, history in self._metric_history.items():
            if len(history) >= 10:  # Need at least 10 data points
                timestamps, values = zip(*history)
                if len(values) > 1:
                    # Simple linear trend
                    x = list(range(len(values)))
                    trend_slope = np.polyfit(x, values, 1)[0]
                    trends[metric_name] = {
                        "slope": trend_slope,
                        "trend": "improving" if trend_slope < 0 else "degrading",
                        "recent_avg": np.mean(values[-5:]),
                        "overall_avg": np.mean(values)
                    }

        return trends

    def _generate_performance_recommendations(self, budget_compliance: Dict[str, bool], trends: Dict[str, Any]) -> List[str]:
        """Generate performance optimization recommendations."""
        recommendations = []

        # Budget violation recommendations
        violations = [endpoint for endpoint, compliant in budget_compliance.items() if not compliant]
        if violations:
            recommendations.append(f"Performance budgets violated for: {', '.join(violations)}")
            recommendations.append("Investigate recent code changes that may impact performance")
            recommendations.append("Consider implementing caching for frequently accessed data")

        # Trend-based recommendations
        degrading_metrics = [metric for metric, trend in trends.items() if trend["trend"] == "degrading"]
        if degrading_metrics:
            recommendations.append(f"Performance degrading for: {', '.join(degrading_metrics)}")
            recommendations.append("Monitor for memory leaks or resource accumulation")

        if not recommendations:
            recommendations.append("Performance is within acceptable limits")
            recommendations.append("Continue monitoring and consider proactive optimization")

        return recommendations

    async def run_ci_performance_check(self) -> Dict[str, Any]:
        """Run performance check for CI/CD pipeline."""
        try:
            # Run smoke test
            smoke_test_id = await self.run_k6_scenario("api_smoke_test")

            # Wait for completion
            await asyncio.sleep(70)  # Wait for test to complete

            smoke_result = self._test_results.get(smoke_test_id)
            if not smoke_result or smoke_result.status != "completed":
                return {
                    "status": "failed",
                    "reason": "Smoke test did not complete successfully",
                    "test_id": smoke_test_id
                }

            # Check if metrics meet CI thresholds
            ci_passed = True
            violations = []

            for metric, threshold in self._ci_thresholds.items():
                if metric in smoke_result.metrics:
                    value = smoke_result.metrics[metric]
                    if value > threshold:
                        ci_passed = False
                        violations.append(f"{metric}: {value} > {threshold}")

            return {
                "status": "passed" if ci_passed else "failed",
                "test_id": smoke_test_id,
                "metrics": smoke_result.metrics,
                "violations": violations,
                "recommendation": "Fix performance issues before merging" if not ci_passed else "Performance check passed"
            }

        except Exception as e:
            return {
                "status": "error",
                "reason": str(e)
            }

    async def get_service_health(self) -> Dict[str, Any]:
        """Get service health status."""
        return {
            "status": "healthy",
            "budgets_configured": len(self._budgets),
            "scenarios_available": len(self._scenarios),
            "tests_run": len(self._test_results),
            "last_test": max([t.start_time for t in self._test_results.values()] if self._test_results else [datetime.utcnow()]),
            "budget_compliance": await self.check_performance_budgets()
        }


def get_performance_monitoring_service() -> PerformanceMonitoringService:
    """Get the global performance monitoring service instance."""
    return PerformanceMonitoringService()


async def run_performance_regression_check(baseline_test_id: str, current_test_id: str) -> Dict[str, Any]:
    """Run performance regression check between two test runs."""
    service = get_performance_monitoring_service()
    comparison_id = await service.compare_performance(baseline_test_id, current_test_id)

    comparison = service._comparisons[comparison_id]
    return {
        "comparison_id": comparison_id,
        "regression_detected": comparison.regression_detected,
        "comparison_metrics": comparison.comparison_metrics,
        "recommendations": comparison.recommendations
    }
