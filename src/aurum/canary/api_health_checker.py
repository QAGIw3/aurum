"""API health checking for canary datasets."""

from __future__ import annotations

import json
import time
import requests
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any

from ..logging import StructuredLogger, LogLevel, create_logger


class APIHealthStatus(str, Enum):
    """API health status levels."""
    HEALTHY = "healthy"         # API responding normally
    DEGRADED = "degraded"       # API responding but slow or incomplete
    UNHEALTHY = "unhealthy"     # API not responding or returning errors
    TIMEOUT = "timeout"         # API request timed out
    ERROR = "error"            # Error during health check


@dataclass
class APIHealthCheck:
    """Configuration for API health check."""

    name: str
    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None
    expected_status_codes: List[int] = field(default_factory=lambda: [200, 201, 202])

    # Validation
    expected_response_format: str = "json"
    min_response_time_ms: Optional[int] = None
    max_response_time_ms: Optional[int] = None
    expected_fields: List[str] = field(default_factory=list)

    # Timeouts and retries
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_delay_seconds: int = 1

    # Metadata
    description: str = ""
    tags: List[str] = field(default_factory=list)


@dataclass
class APIHealthResult:
    """Result of API health check."""

    check_name: str
    status: APIHealthStatus
    response_time_ms: float
    timestamp: str

    # Response details
    status_code: Optional[int] = None
    response_size_bytes: Optional[int] = None
    error_message: Optional[str] = None

    # Validation results
    response_format_valid: bool = True
    required_fields_present: bool = True
    data_quality_issues: List[str] = field(default_factory=list)

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_healthy(self) -> bool:
        """Check if API health check passed."""
        return self.status == APIHealthStatus.HEALTHY

    def is_degraded(self) -> bool:
        """Check if API is degraded."""
        return self.status == APIHealthStatus.DEGRADED

    def is_unhealthy(self) -> bool:
        """Check if API is unhealthy."""
        return self.status in [APIHealthStatus.UNHEALTHY, APIHealthStatus.TIMEOUT, APIHealthStatus.ERROR]

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "check_name": self.check_name,
            "status": self.status.value,
            "response_time_ms": self.response_time_ms,
            "timestamp": self.timestamp,
            "status_code": self.status_code,
            "response_size_bytes": self.response_size_bytes,
            "error_message": self.error_message,
            "response_format_valid": self.response_format_valid,
            "required_fields_present": self.required_fields_present,
            "data_quality_issues": self.data_quality_issues,
            "metadata": self.metadata,
            "is_healthy": self.is_healthy(),
            "is_degraded": self.is_degraded(),
            "is_unhealthy": self.is_unhealthy()
        }


class APIHealthChecker:
    """Check API health for canary monitoring."""

    def __init__(self):
        """Initialize API health checker."""
        self.logger = create_logger(
            source_name="api_health_checker",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.api_health.events",
            dataset="api_health_checking"
        )

        # Session for connection pooling
        self.session = requests.Session()

        # Health check registry
        self.health_checks: Dict[str, APIHealthCheck] = {}

    def register_health_check(self, check: APIHealthCheck) -> None:
        """Register an API health check.

        Args:
            check: Health check configuration
        """
        self.health_checks[check.name] = check

        self.logger.log(
            LogLevel.INFO,
            f"Registered API health check: {check.name}",
            "health_check_registered",
            check_name=check.name,
            url=check.url,
            method=check.method
        )

    def check_api_health(self, check_name: str) -> APIHealthResult:
        """Run a single API health check.

        Args:
            check_name: Name of the health check

        Returns:
            API health check result
        """
        if check_name not in self.health_checks:
            raise ValueError(f"Unknown health check: {check_name}")

        check = self.health_checks[check_name]
        start_time = time.time()

        try:
            # Make the request
            response = self._make_request(check)

            # Calculate response time
            response_time_ms = (time.time() - start_time) * 1000

            # Validate response
            result = self._validate_response(check, response, response_time_ms)

            self.logger.log(
                LogLevel.DEBUG,
                f"API health check {check_name}: {result.status.value}",
                "api_health_check",
                check_name=check_name,
                status=result.status.value,
                response_time_ms=result.response_time_ms
            )

            return result

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000

            result = APIHealthResult(
                check_name=check_name,
                status=APIHealthStatus.ERROR,
                response_time_ms=response_time_ms,
                timestamp=datetime.now().isoformat(),
                error_message=str(e),
                response_format_valid=False,
                required_fields_present=False,
                metadata={"error_type": e.__class__.__name__}
            )

            self.logger.log(
                LogLevel.ERROR,
                f"API health check {check_name} failed: {e}",
                "api_health_check_error",
                check_name=check_name,
                error=str(e)
            )

            return result

    def check_all_apis(self) -> Dict[str, APIHealthResult]:
        """Run all registered API health checks.

        Returns:
            Dictionary mapping check names to results
        """
        results = {}

        for check_name in self.health_checks.keys():
            try:
                result = self.check_api_health(check_name)
                results[check_name] = result
            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error running health check {check_name}: {e}",
                    "health_check_batch_error",
                    check_name=check_name,
                    error=str(e)
                )

        return results

    def get_unhealthy_apis(self) -> List[APIHealthResult]:
        """Get all APIs that are currently unhealthy.

        Returns:
            List of unhealthy API health results
        """
        unhealthy_apis = []

        for check_name in self.health_checks.keys():
            try:
                result = self.check_api_health(check_name)
                if result.is_unhealthy():
                    unhealthy_apis.append(result)
            except Exception:
                # If we can't even run the check, consider it unhealthy
                unhealthy_apis.append(APIHealthResult(
                    check_name=check_name,
                    status=APIHealthStatus.ERROR,
                    response_time_ms=0,
                    timestamp=datetime.now().isoformat(),
                    error_message="Health check failed to execute",
                    response_format_valid=False,
                    required_fields_present=False
                ))

        return unhealthy_apis

    def _make_request(self, check: APIHealthCheck) -> requests.Response:
        """Make HTTP request for health check.

        Args:
            check: Health check configuration

        Returns:
            HTTP response
        """
        for attempt in range(check.max_retries + 1):
            try:
                response = self.session.request(
                    method=check.method,
                    url=check.url,
                    headers=check.headers,
                    params=check.params,
                    json=check.body,
                    timeout=check.timeout_seconds
                )

                response.raise_for_status()
                return response

            except requests.RequestException as e:
                if attempt == check.max_retries:
                    raise e

                # Exponential backoff
                delay = check.retry_delay_seconds * (2 ** attempt)
                time.sleep(delay)

        # This should never be reached
        raise RuntimeError("Max retries exceeded")

    def _validate_response(
        self,
        check: APIHealthCheck,
        response: requests.Response,
        response_time_ms: float
    ) -> APIHealthResult:
        """Validate API response.

        Args:
            check: Health check configuration
            response: HTTP response
            response_time_ms: Response time in milliseconds

        Returns:
            API health result
        """
        result = APIHealthResult(
            check_name=check.name,
            status=APIHealthStatus.HEALTHY,
            response_time_ms=response_time_ms,
            timestamp=datetime.now().isoformat(),
            status_code=response.status_code,
            response_size_bytes=len(response.content)
        )

        # Check response time
        if check.min_response_time_ms and response_time_ms < check.min_response_time_ms:
            result.status = APIHealthStatus.DEGRADED
            result.data_quality_issues.append(f"Response too fast: {response_time_ms}ms")

        if check.max_response_time_ms and response_time_ms > check.max_response_time_ms:
            result.status = APIHealthStatus.DEGRADED
            result.data_quality_issues.append(f"Response too slow: {response_time_ms}ms")

        # Check status code
        if response.status_code not in check.expected_status_codes:
            result.status = APIHealthStatus.UNHEALTHY
            result.error_message = f"Unexpected status code: {response.status_code}"

        # Validate response format
        if not self._validate_response_format(check, response):
            result.status = APIHealthStatus.DEGRADED
            result.response_format_valid = False

        # Check required fields
        if not self._validate_required_fields(check, response):
            result.status = APIHealthStatus.DEGRADED
            result.required_fields_present = False

        return result

    def _validate_response_format(self, check: APIHealthCheck, response: requests.Response) -> bool:
        """Validate response format.

        Args:
            check: Health check configuration
            response: HTTP response

        Returns:
            True if format is valid
        """
        try:
            if check.expected_response_format == "json":
                json.loads(response.text)
            elif check.expected_response_format == "xml":
                # Basic XML validation
                if not response.text.strip().startswith("<"):
                    raise ValueError("Response does not appear to be XML")
            # Add more format validations as needed

            return True

        except Exception as e:
            return False

    def _validate_required_fields(self, check: APIHealthCheck, response: requests.Response) -> bool:
        """Validate that required fields are present in response.

        Args:
            check: Health check configuration
            response: HTTP response

        Returns:
            True if all required fields are present
        """
        if not check.expected_fields:
            return True

        try:
            if check.expected_response_format == "json":
                data = json.loads(response.text)
                if isinstance(data, dict):
                    missing_fields = [field for field in check.expected_fields if field not in data]
                elif isinstance(data, list) and data:
                    # Check first item in array
                    missing_fields = [field for field in check.expected_fields if field not in data[0]]
                else:
                    missing_fields = check.expected_fields

                if missing_fields:
                    return False

            return True

        except Exception:
            return False
