"""Mock objects for testing external data ingestion system."""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime

from ..external.collect import HttpRequest, HttpResponse


@dataclass
class MockHttpResponse:
    """Mock HTTP response for testing."""
    status: int = 200
    content: bytes = b'{"test": "data"}'
    headers: Dict[str, str] = field(default_factory=dict)
    json_data: Optional[Dict[str, Any]] = None

    def json(self) -> Dict[str, Any]:
        """Return JSON data if available, otherwise parse content."""
        if self.json_data:
            return self.json_data
        try:
            return {"parsed": "from_content"}
        except:
            return {}


class MockKafkaProducer:
    """Mock Kafka producer for testing."""

    def __init__(self):
        self.messages: List[Dict[str, Any]] = []
        self.flushed = False
        self.closed = False

    def produce(self, topic: str, value: Any, key: str = None, **kwargs):
        """Produce a message to the mock producer."""
        self.messages.append({
            "topic": topic,
            "value": value,
            "key": key,
            "kwargs": kwargs
        })

    def flush(self, timeout: float = 1.0) -> int:
        """Flush messages."""
        self.flushed = True
        return len(self.messages)

    def close(self):
        """Close the producer."""
        self.closed = True


class MockDatabaseConnection:
    """Mock database connection for testing."""

    def __init__(self):
        self.queries: List[str] = []
        self.connected = True

    async def execute(self, query: str, *args) -> List[Dict[str, Any]]:
        """Execute a mock query."""
        self.queries.append(query)
        return [{"mock": "result"}]

    async def fetchall(self, query: str, *args) -> List[Dict[str, Any]]:
        """Fetch all results from mock query."""
        self.queries.append(query)
        return [{"mock": "data"}]

    async def close(self):
        """Close the connection."""
        self.connected = False


class MockVaultClient:
    """Mock Vault client for testing."""

    def __init__(self, credentials: Dict[str, Any] = None):
        self.credentials = credentials or {
            "CAISO": {"key": "test_caiso_key"},
            "MISO": {"key": "test_miso_key"},
            "PJM": {"key": "test_pjm_key"},
            "ERCOT": {"key": "test_ercot_key"},
            "SPP": {"key": "test_spp_key"}
        }
        self.authenticated = True

    async def get_credential(self, path: str, key: str, cache: bool = True) -> Optional[str]:
        """Get a mock credential."""
        if path.startswith("api_keys/"):
            iso_code = path.split("/")[-1].upper()
            return self.credentials.get(iso_code, {}).get("key")
        return f"mock_{key}"

    async def get_api_key(self, iso_code: str) -> Optional[str]:
        """Get API key for an ISO."""
        return self.credentials.get(iso_code.upper(), {}).get("key")

    async def health_check(self) -> Dict[str, Any]:
        """Health check for mock vault."""
        return {"status": "healthy", "authenticated": True}


class MockCircuitBreaker:
    """Mock circuit breaker for testing."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.is_open_state = False
        self.last_failure_time = None

    def is_open(self) -> bool:
        """Check if circuit breaker is open."""
        return self.is_open_state

    def allow(self, now: float) -> bool:
        """Check if request is allowed."""
        return not self.is_open_state

    def record_success(self):
        """Record successful operation."""
        self.failure_count = 0
        self.is_open_state = False

    def record_failure(self):
        """Record failed operation."""
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.is_open_state = True


class MockExternalCollector:
    """Mock external collector for testing."""

    def __init__(self, responses: List[MockHttpResponse] = None):
        self.responses = responses or []
        self.requests: List[HttpRequest] = []
        self.response_index = 0

    def request(self, http_request: HttpRequest) -> MockHttpResponse:
        """Make a mock HTTP request."""
        self.requests.append(http_request)

        if self.response_index < len(self.responses):
            response = self.responses[self.response_index]
            self.response_index += 1
            return response

        # Default response
        return MockHttpResponse(status=200, content=b'{"success": true}')


class MockMetricsClient:
    """Mock metrics client for testing."""

    def __init__(self):
        self.counters: Dict[str, int] = {}
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}

    def increment_counter(self, name: str, value: int = 1, tags: Dict[str, str] = None):
        """Increment a counter."""
        key = f"{name}:{tags}" if tags else name
        self.counters[key] = self.counters.get(key, 0) + value

    def gauge(self, name: str, value: float, tags: Dict[str, str] = None):
        """Set a gauge."""
        key = f"{name}:{tags}" if tags else name
        self.gauges[key] = value

    def histogram(self, name: str, value: float, tags: Dict[str, str] = None):
        """Record histogram value."""
        key = f"{name}:{tags}" if tags else name
        if key not in self.histograms:
            self.histograms[key] = []
        self.histograms[key].append(value)


class MockAsyncContext:
    """Mock async context manager for testing."""

    def __init__(self, return_value: Any = None):
        self.return_value = return_value
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self.return_value

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.exited = True


def create_mock_http_response(
    status: int = 200,
    json_data: Dict[str, Any] = None,
    content: str = None
) -> MockHttpResponse:
    """Create a mock HTTP response."""
    if json_data:
        content = json.dumps(json_data).encode('utf-8')

    return MockHttpResponse(
        status=status,
        content=content.encode('utf-8') if isinstance(content, str) else content,
        json_data=json_data
    )


def create_successful_caiso_response(records: List[Dict[str, Any]]) -> MockHttpResponse:
    """Create a successful CAISO API response."""
    return create_mock_http_response(
        status=200,
        json_data={
            "PRC_LMP": {
                "results": {
                    "items": records
                }
            }
        }
    )


def create_successful_miso_response(data: List[Dict[str, Any]]) -> MockHttpResponse:
    """Create a successful MISO API response."""
    return create_mock_http_response(
        status=200,
        json_data={"data": data}
    )


def create_error_response(status: int = 500, message: str = "Internal Server Error") -> MockHttpResponse:
    """Create an error HTTP response."""
    return create_mock_http_response(
        status=status,
        json_data={"error": message}
    )


def create_rate_limited_response() -> MockHttpResponse:
    """Create a rate limited HTTP response."""
    return create_mock_http_response(
        status=429,
        json_data={"error": "Rate limit exceeded"}
    )
