#!/usr/bin/env python3
"""Fault injection system for testing observability and alerting."""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aurum.observability.metrics import (
    increment_api_requests,
    observe_api_request_duration,
    set_data_freshness_hours,
    increment_ge_validation_runs,
    increment_canary_execution_status,
    observe_canary_execution_duration,
    set_system_health_score,
    set_component_health_status,
    PROMETHEUS_AVAILABLE,
)
from aurum.observability.slo_monitor import record_api_measurement, record_data_freshness_measurement
from aurum.observability.alert_manager import process_api_alert, process_data_freshness_alert, process_ge_validation_alert, process_canary_alert


class FaultInjector:
    """Inject faults to test monitoring and alerting systems."""

    def __init__(self, duration_minutes: int = 60):
        self.duration_minutes = duration_minutes
        self.start_time = datetime.now()
        self.end_time = self.start_time + timedelta(minutes=duration_minutes)

    async def run_fault_injection(self, fault_type: str, parameters: Dict[str, Any]) -> None:
        """Run fault injection scenario."""
        print(f"Starting fault injection: {fault_type}")
        print(f"Duration: {self.duration_minutes} minutes")
        print(f"Parameters: {parameters}")

        if fault_type == "api_high_error_rate":
            await self._inject_api_errors(parameters)
        elif fault_type == "api_high_latency":
            await self._inject_high_latency(parameters)
        elif fault_type == "data_staleness":
            await self._inject_data_staleness(parameters)
        elif fault_type == "ge_validation_failure":
            await self._inject_ge_validation_failure(parameters)
        elif fault_type == "canary_failure":
            await self._inject_canary_failure(parameters)
        elif fault_type == "system_health_degradation":
            await self._inject_system_health_degradation(parameters)
        elif fault_type == "combined_scenario":
            await self._run_combined_scenario(parameters)
        else:
            raise ValueError(f"Unknown fault type: {fault_type}")

        print(f"Completed fault injection: {fault_type}")

    async def _inject_api_errors(self, params: Dict[str, Any]) -> None:
        """Inject API errors to test error rate alerting."""
        error_rate = params.get("error_rate", 0.1)  # 10% error rate
        duration_minutes = params.get("duration_minutes", 10)
        method = params.get("method", "GET")
        path = params.get("path", "/api/test")

        end_time = datetime.now() + timedelta(minutes=duration_minutes)

        while datetime.now() < end_time:
            # Generate normal requests
            normal_requests = int(100 * (1 - error_rate))
            error_requests = int(100 * error_rate)

            for _ in range(normal_requests):
                await increment_api_requests(method, path, 200)
                await observe_api_request_duration(method, path, 200, 0.1)
                await record_api_measurement("api_availability", 0.1, 200)

            for _ in range(error_requests):
                await increment_api_requests(method, path, 500)
                await observe_api_request_duration(method, path, 500, 0.5)
                await record_api_measurement("api_availability", 0.5, 500)

            # Process alerts
            await process_api_alert(method, path, 500, 0.5)

            await asyncio.sleep(1)  # Inject 100 requests per second

    async def _inject_high_latency(self, params: Dict[str, Any]) -> None:
        """Inject high latency to test latency alerting."""
        latency_seconds = params.get("latency_seconds", 2.0)
        duration_minutes = params.get("duration_minutes", 5)
        method = params.get("method", "GET")
        path = params.get("path", "/api/slow")

        end_time = datetime.now() + timedelta(minutes=duration_minutes)

        while datetime.now() < end_time:
            await increment_api_requests(method, path, 200)
            await observe_api_request_duration(method, path, 200, latency_seconds)
            await record_api_measurement("api_latency", latency_seconds, latency_seconds <= 1.0)

            # Process alerts
            await process_api_alert(method, path, 200, latency_seconds)

            await asyncio.sleep(0.1)  # One request per 100ms

    async def _inject_data_staleness(self, params: Dict[str, Any]) -> None:
        """Inject data staleness to test freshness alerting."""
        dataset = params.get("dataset", "test_dataset")
        source = params.get("source", "test_source")
        staleness_hours = params.get("staleness_hours", 30)

        await set_data_freshness_hours(dataset, source, staleness_hours)
        await set_data_freshness_score(dataset, source, 0.0)  # 0% freshness score

        # Process alerts
        await process_data_freshness_alert(dataset, source, staleness_hours)

    async def _inject_ge_validation_failure(self, params: Dict[str, Any]) -> None:
        """Inject Great Expectations validation failure."""
        dataset = params.get("dataset", "test_dataset")
        suite = params.get("suite", "test_suite")
        quality_score = params.get("quality_score", 0.5)

        await increment_ge_validation_runs(dataset, suite, "failure")
        await set_ge_validation_quality_score(dataset, suite, quality_score)

        # Process alerts
        await process_ge_validation_alert(dataset, suite, False, quality_score)

    async def _inject_canary_failure(self, params: Dict[str, Any]) -> None:
        """Inject canary failure."""
        canary_name = params.get("canary_name", "test_canary")
        execution_time_seconds = params.get("execution_time_seconds", 10.0)
        errors = params.get("errors", ["Simulated failure"])

        await increment_canary_execution_status(canary_name, "unhealthy")
        await observe_canary_execution_duration(canary_name, execution_time_seconds)

        # Process alerts
        await process_canary_alert(canary_name, "unhealthy", execution_time_seconds, errors)

    async def _inject_system_health_degradation(self, params: Dict[str, Any]) -> None:
        """Inject system health degradation."""
        component = params.get("component", "test_component")
        health_score = params.get("health_score", 0.5)

        await set_system_health_score(health_score)
        await set_component_health_status(component, "degraded")

    async def _run_combined_scenario(self, params: Dict[str, Any]) -> None:
        """Run a combined fault injection scenario."""
        scenario_duration = params.get("duration_minutes", 15)

        print("Starting combined fault injection scenario...")

        # Phase 1: Gradual degradation (0-5 minutes)
        print("Phase 1: Gradual degradation")
        await self._inject_system_health_degradation({"health_score": 0.8})

        # Phase 2: API errors and latency (5-10 minutes)
        print("Phase 2: API errors and latency")
        await asyncio.gather(
            self._inject_api_errors({"error_rate": 0.05, "duration_minutes": 5}),
            self._inject_high_latency({"latency_seconds": 1.5, "duration_minutes": 5})
        )

        # Phase 3: Data staleness (10-12 minutes)
        print("Phase 3: Data staleness")
        await self._inject_data_staleness({"staleness_hours": 25})

        # Phase 4: Validation failure (12-14 minutes)
        print("Phase 4: Validation failure")
        await self._inject_ge_validation_failure({"quality_score": 0.3})

        # Phase 5: Recovery (14-15 minutes)
        print("Phase 5: Recovery")
        await set_system_health_score(1.0)
        await set_data_freshness_hours("test_dataset", "test_source", 1)
        await set_data_freshness_score("test_dataset", "test_source", 1.0)


class FaultInjectionRunner:
    """Run fault injection scenarios."""

    def __init__(self):
        self.fault_injector = FaultInjector()

    async def run_scenario(self, scenario_name: str, parameters: Dict[str, Any]) -> None:
        """Run a fault injection scenario."""
        print(f"Running fault injection scenario: {scenario_name}")
        print(f"Parameters: {parameters}")

        await self.fault_injector.run_fault_injection(scenario_name, parameters)

        print(f"Completed scenario: {scenario_name}")

    async def run_test_suite(self) -> None:
        """Run comprehensive test suite."""
        scenarios = [
            ("api_high_error_rate", {"error_rate": 0.1, "duration_minutes": 5}),
            ("api_high_latency", {"latency_seconds": 2.0, "duration_minutes": 3}),
            ("data_staleness", {"staleness_hours": 30}),
            ("ge_validation_failure", {"quality_score": 0.4}),
            ("canary_failure", {"execution_time_seconds": 15.0}),
            ("system_health_degradation", {"health_score": 0.6}),
            ("combined_scenario", {"duration_minutes": 15})
        ]

        for scenario_name, params in scenarios:
            try:
                await self.run_scenario(scenario_name, params)
                await asyncio.sleep(2)  # Brief pause between scenarios
            except Exception as e:
                print(f"Scenario {scenario_name} failed: {e}")
                continue

        print("Completed test suite")

    def get_available_scenarios(self) -> List[str]:
        """Get list of available fault injection scenarios."""
        return [
            "api_high_error_rate",
            "api_high_latency",
            "data_staleness",
            "ge_validation_failure",
            "canary_failure",
            "system_health_degradation",
            "combined_scenario",
            "test_suite"
        ]


async def main():
    parser = argparse.ArgumentParser(description="Fault injection for testing observability")
    parser.add_argument(
        "scenario",
        choices=FaultInjectionRunner().get_available_scenarios(),
        help="Fault injection scenario to run"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration for individual fault injection (minutes)"
    )
    parser.add_argument(
        "--parameter",
        action="append",
        help="Parameter in key=value format (can be used multiple times)"
    )

    args = parser.parse_args()

    runner = FaultInjectionRunner()

    # Parse parameters
    parameters = {}
    if args.parameter:
        for param in args.parameter:
            if "=" in param:
                key, value = param.split("=", 1)
                # Try to parse as JSON, otherwise treat as string
                try:
                    parameters[key] = __import__("json").loads(value)
                except:
                    parameters[key] = value

    if args.scenario == "test_suite":
        await runner.run_test_suite()
    else:
        parameters.setdefault("duration_minutes", args.duration)
        await runner.run_scenario(args.scenario, parameters)


if __name__ == "__main__":
    asyncio.run(main())
