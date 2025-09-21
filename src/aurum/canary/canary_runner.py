"""Canary execution and result processing."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable

from .canary_manager import CanaryManager, CanaryDataset, CanaryStatus
from .api_health_checker import APIHealthChecker, APIHealthResult, APIHealthStatus
from ..logging import StructuredLogger, LogLevel, create_logger

# Optional Prometheus metrics (best-effort)
try:  # pragma: no cover
    from prometheus_client import Counter as _PromCounter, Histogram as _PromHistogram
except Exception:  # pragma: no cover
    _PromCounter = None  # type: ignore
    _PromHistogram = None  # type: ignore

CANARY_RUNS_TOTAL = (
    _PromCounter("aurum_canary_runs_total", "Total canary runs", ["canary", "status"]) if _PromCounter else None
)
CANARY_DURATION_SECONDS = (
    _PromHistogram(
        "aurum_canary_duration_seconds",
        "Canary execution duration (seconds)",
        labelnames=["canary"],
        buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
    )
    if _PromHistogram
    else None
)


@dataclass
class CanaryResult:
    """Result of canary execution."""

    canary_name: str
    execution_time_seconds: float
    status: CanaryStatus
    records_processed: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # API health details
    api_health_results: List[APIHealthResult] = field(default_factory=list)

    # Data validation results
    data_quality_score: Optional[float] = None
    validation_errors: List[str] = field(default_factory=list)

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def is_success(self) -> bool:
        """Check if canary execution was successful."""
        return self.status == CanaryStatus.HEALTHY

    def is_failure(self) -> bool:
        """Check if canary execution failed."""
        return self.status in [CanaryStatus.UNHEALTHY, CanaryStatus.TIMEOUT, CanaryStatus.ERROR]

    def has_warnings(self) -> bool:
        """Check if canary has warnings."""
        return len(self.warnings) > 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "canary_name": self.canary_name,
            "execution_time_seconds": self.execution_time_seconds,
            "status": self.status.value,
            "records_processed": self.records_processed,
            "errors": self.errors,
            "warnings": self.warnings,
            "api_health_results": [r.to_dict() for r in self.api_health_results],
            "data_quality_score": self.data_quality_score,
            "validation_errors": self.validation_errors,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
            "is_success": self.is_success(),
            "is_failure": self.is_failure(),
            "has_warnings": self.has_warnings()
        }


class CanaryRunner:
    """Execute canary checks and process results."""

    def __init__(self, canary_manager: CanaryManager, api_checker: APIHealthChecker):
        """Initialize canary runner.

        Args:
            canary_manager: Canary dataset manager
            api_checker: API health checker
        """
        self.canary_manager = canary_manager
        self.api_checker = api_checker

        self.logger = create_logger(
            source_name="canary_runner",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.canary.executions",
            dataset="canary_execution"
        )

    def run_canary(self, canary_name: str) -> CanaryResult:
        """Run a single canary check.

        Args:
            canary_name: Name of the canary to run

        Returns:
            Canary execution result
        """
        canary = self.canary_manager.get_canary(canary_name)
        if not canary:
            raise ValueError(f"Unknown canary: {canary_name}")

        if not canary.config.enabled:
            return CanaryResult(
                canary_name=canary_name,
                execution_time_seconds=0,
                status=CanaryStatus.DISABLED
            )

        start_time = time.time()
        errors = []
        warnings = []

        self.logger.log(
            LogLevel.INFO,
            f"Starting canary execution: {canary_name}",
            "canary_execution_start",
            canary_name=canary_name,
            source=canary.source,
            dataset=canary.dataset
        )

        try:
            # Run API health checks
            api_results = self._run_api_checks(canary)
            errors.extend([r.error_message for r in api_results if r.error_message])

            # Validate data
            validation_result = self._validate_canary_data(canary, api_results)
            if validation_result["errors"]:
                errors.extend(validation_result["errors"])
            if validation_result["warnings"]:
                warnings.extend(validation_result["warnings"])

            # Determine overall status
            status = self._determine_canary_status(canary, api_results, errors, warnings)

            # Calculate execution time
            execution_time = time.time() - start_time

            # Create result
            result = CanaryResult(
                canary_name=canary_name,
                execution_time_seconds=execution_time,
                status=status,
                records_processed=validation_result.get("records_processed", 0),
                errors=errors,
                warnings=warnings,
                api_health_results=api_results,
                data_quality_score=validation_result.get("quality_score"),
                validation_errors=validation_result.get("errors", []),
                metadata={
                    "source": canary.source,
                    "dataset": canary.dataset,
                    "api_checks_count": len(api_results)
                }
            )

            # Update canary status
            self.canary_manager.update_canary_status(
                canary_name,
                status,
                execution_time,
                result.records_processed,
                errors,
                warnings
            )

            # Emit metrics (best-effort)
            try:
                if CANARY_DURATION_SECONDS:
                    CANARY_DURATION_SECONDS.labels(canary=canary_name).observe(execution_time)
                if CANARY_RUNS_TOTAL:
                    CANARY_RUNS_TOTAL.labels(canary=canary_name, status=status.value).inc()
            except Exception:
                pass

            self.logger.log(
                LogLevel.INFO,
                f"Completed canary execution: {canary_name} ({status.value})",
                "canary_execution_complete",
                canary_name=canary_name,
                status=status.value,
                execution_time_seconds=execution_time,
                errors_count=len(errors),
                warnings_count=len(warnings)
            )

            return result

        except Exception as e:
            execution_time = time.time() - start_time

            # Emit failure metrics
            try:
                if CANARY_DURATION_SECONDS:
                    CANARY_DURATION_SECONDS.labels(canary=canary_name).observe(execution_time)
                if CANARY_RUNS_TOTAL:
                    CANARY_RUNS_TOTAL.labels(canary=canary_name, status=CanaryStatus.ERROR.value).inc()
            except Exception:
                pass

            self.logger.log(
                LogLevel.ERROR,
                f"Canary execution failed: {canary_name}: {e}",
                "canary_execution_error",
                canary_name=canary_name,
                error=str(e)
            )

            # Update canary status
            self.canary_manager.update_canary_status(
                canary_name,
                CanaryStatus.ERROR,
                execution_time,
                0,
                [str(e)],
                []
            )

            return CanaryResult(
                canary_name=canary_name,
                execution_time_seconds=execution_time,
                status=CanaryStatus.ERROR,
                errors=[str(e)]
            )

    def run_all_canaries(self) -> Dict[str, CanaryResult]:
        """Run all enabled canaries.

        Returns:
            Dictionary mapping canary names to results
        """
        results = {}

        for canary_name, canary in self.canary_manager.canaries.items():
            if canary.config.enabled:
                try:
                    result = self.run_canary(canary_name)
                    results[canary_name] = result
                except Exception as e:
                    self.logger.log(
                        LogLevel.ERROR,
                        f"Failed to run canary {canary_name}: {e}",
                        "canary_batch_error",
                        canary_name=canary_name,
                        error=str(e)
                    )

        return results

    def _run_api_checks(self, canary: CanaryDataset) -> List[APIHealthResult]:
        """Run API health checks for a canary.

        Args:
            canary: Canary dataset

        Returns:
            List of API health check results
        """
        config = canary.config
        results = []

        # Create health check for the API endpoint
        health_check = APIHealthResult(
            name=f"{canary.name}_api_check",
            url=config.api_endpoint,
            method="GET",
            headers={"User-Agent": "Aurum-Canary/1.0"},
            params=config.api_params,
            expected_response_format=config.expected_response_format,
            expected_fields=config.expected_fields,
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries
        )

        self.api_checker.register_health_check(health_check)
        result = self.api_checker.check_api_health(health_check.name)
        results.append(result)

        return results

    def _validate_canary_data(
        self,
        canary: CanaryDataset,
        api_results: List[APIHealthResult]
    ) -> Dict[str, Any]:
        """Validate canary data.

        Args:
            canary: Canary dataset
            api_results: API health check results

        Returns:
            Validation results
        """
        result = {
            "records_processed": 0,
            "quality_score": 1.0,
            "errors": [],
            "warnings": []
        }

        # Check if API calls were successful
        failed_apis = [r for r in api_results if r.is_unhealthy()]
        if failed_apis:
            result["errors"].extend([
                f"API health check failed: {r.check_name}" for r in failed_apis
            ])
            result["quality_score"] = 0.0
            return result

        # For successful APIs, we would validate the actual data
        # This is a simplified version - in practice, you'd parse the response
        # and validate data quality, field presence, etc.

        result["records_processed"] = 1  # At least the API is responding
        result["quality_score"] = 1.0

        return result

    def _determine_canary_status(
        self,
        canary: CanaryDataset,
        api_results: List[APIHealthResult],
        errors: List[str],
        warnings: List[str]
    ) -> CanaryStatus:
        """Determine canary status based on results.

        Args:
            canary: Canary dataset
            api_results: API health check results
            errors: List of errors
            warnings: List of warnings

        Returns:
            Canary status
        """
        # Check for errors
        if errors:
            return CanaryStatus.ERROR

        # Check API health
        unhealthy_apis = [r for r in api_results if r.is_unhealthy()]
        if unhealthy_apis:
            return CanaryStatus.UNHEALTHY

        # Check for timeouts
        timed_out_apis = [r for r in api_results if r.status == APIHealthStatus.TIMEOUT]
        if timed_out_apis:
            return CanaryStatus.TIMEOUT

        # Check for degraded performance
        degraded_apis = [r for r in api_results if r.is_degraded()]
        if degraded_apis:
            # If only warnings and API is degraded, return unhealthy
            if warnings:
                return CanaryStatus.UNHEALTHY
            else:
                return CanaryStatus.HEALTHY

        # All checks passed
        return CanaryStatus.HEALTHY

    def get_runner_status(self) -> Dict[str, Any]:
        """Get status of the canary runner.

        Returns:
            Dictionary with runner status
        """
        canary_health = self.canary_manager.get_canary_health_summary()

        return {
            "total_canaries": len(self.canary_manager.canaries),
            "healthy_canaries": len([c for c in self.canary_manager.canaries.values() if c.is_healthy()]),
            "unhealthy_canaries": len([c for c in self.canary_manager.canaries.values() if c.is_unhealthy()]),
            "canary_health_summary": canary_health,
            "api_checker_status": {
                "total_checks": len(self.api_checker.health_checks),
                "unhealthy_apis": len(self.api_checker.get_unhealthy_apis())
            }
        }
