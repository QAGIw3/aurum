"""Hermetic test fixtures for external services and dependencies."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, Generator, List, Optional
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, make_mocked_request


class MockExternalService:
    """Mock external service for testing without network dependencies."""

    def __init__(self, service_name: str, base_url: str, responses: Dict[str, Any]):
        self.service_name = service_name
        self.base_url = base_url
        self.responses = responses
        self.request_history: List[Dict[str, Any]] = []

    def get_response(self, endpoint: str, method: str = "GET") -> Any:
        """Get mock response for endpoint."""
        key = f"{method}:{endpoint}"
        return self.responses.get(key, {"error": "Mock not configured"})

    def record_request(self, endpoint: str, method: str = "GET", **kwargs) -> None:
        """Record a request for testing."""
        self.request_history.append({
            "endpoint": endpoint,
            "method": method,
            "kwargs": kwargs,
            "timestamp": asyncio.get_event_loop().time()
        })

    def clear_history(self) -> None:
        """Clear request history."""
        self.request_history.clear()

    def get_request_count(self, endpoint: Optional[str] = None) -> int:
        """Get request count for endpoint."""
        if endpoint:
            return sum(1 for req in self.request_history if req["endpoint"] == endpoint)
        return len(self.request_history)


class MockEIAProvider(MockExternalService):
    """Mock EIA (Energy Information Administration) API provider."""

    def __init__(self):
        responses = {
            "GET:/api/v1/series": {
                "status": "success",
                "data": {
                    "series": [
                        {
                            "series_id": "ELEC.PRICE.NY-RES.M",
                            "name": "New York Residential Electric Price",
                            "units": "cents per kilowatthour",
                            "frequency": "Monthly",
                            "start_date": "2001-01-01",
                            "end_date": "2024-01-01"
                        }
                    ]
                }
            },
            "GET:/api/v1/series/ELEC.PRICE.NY-RES.M/data": {
                "status": "success",
                "data": {
                    "series": [
                        {
                            "series_id": "ELEC.PRICE.NY-RES.M",
                            "data": [
                                {"period": "2024-01", "value": "12.5"},
                                {"period": "2023-12", "value": "12.3"},
                                {"period": "2023-11", "value": "12.1"}
                            ]
                        }
                    ]
                }
            },
            "POST:/api/v1/series": {
                "status": "error",
                "error": "Method not allowed"
            }
        }
        super().__init__("EIA", "https://api.eia.gov", responses)


class MockTrinoService(MockExternalService):
    """Mock Trino database service."""

    def __init__(self):
        responses = {
            "POST:/v1/statement": {
                "id": "test_query_id",
                "infoUri": "http://localhost:8080/v1/query/test_query_id",
                "nextUri": "http://localhost:8080/v1/query/test_query_id/1",
                "stats": {
                    "state": "FINISHED",
                    "queued": False,
                    "scheduled": True,
                    "nodes": 1,
                    "totalSplits": 1,
                    "queuedSplits": 0,
                    "runningSplits": 0,
                    "completedSplits": 1
                },
                "data": [
                    ["column1", "column2", "column3"],
                    ["value1", "value2", "value3"],
                    ["value4", "value5", "value6"]
                ]
            }
        }
        super().__init__("Trino", "http://localhost:8080", responses)


class MockKafkaService(MockExternalService):
    """Mock Kafka service."""

    def __init__(self):
        responses = {
            "GET:/brokers": {
                "brokers": [
                    {"id": 1, "host": "localhost", "port": 9092},
                    {"id": 2, "host": "localhost", "port": 9093}
                ]
            },
            "GET:/topics": {
                "topics": [
                    {"name": "test_topic", "partitions": 3},
                    {"name": "aurum_events", "partitions": 6}
                ]
            }
        }
        super().__init__("Kafka", "http://localhost:8080", responses)


class HermeticTestEnvironment:
    """Hermetic test environment with all external dependencies mocked."""

    def __init__(self):
        self.mock_services: Dict[str, MockExternalService] = {}
        self.temp_dirs: List[Path] = []
        self.temp_files: List[Path] = []

    def add_mock_service(self, service: MockExternalService) -> None:
        """Add a mock service to the environment."""
        self.mock_services[service.service_name] = service

    def get_mock_service(self, service_name: str) -> MockExternalService:
        """Get a mock service by name."""
        if service_name not in self.mock_services:
            raise ValueError(f"Mock service '{service_name}' not found")
        return self.mock_services[service_name]

    def setup_standard_mocks(self) -> None:
        """Set up standard mock services for most tests."""
        self.add_mock_service(MockEIAProvider())
        self.add_mock_service(MockTrinoService())
        self.add_mock_service(MockKafkaService())

    def create_temp_dir(self, prefix: str = "test_") -> Path:
        """Create a temporary directory."""
        temp_dir = Path(tempfile.mkdtemp(prefix=prefix))
        self.temp_dirs.append(temp_dir)
        return temp_dir

    def create_temp_file(self, content: str = "", suffix: str = ".txt", prefix: str = "test_") -> Path:
        """Create a temporary file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix=suffix, prefix=prefix, delete=False) as f:
            f.write(content)
            temp_file = Path(f.name)
        self.temp_files.append(temp_file)
        return temp_file

    def create_temp_json_file(self, data: Dict[str, Any], suffix: str = ".json") -> Path:
        """Create a temporary JSON file."""
        content = json.dumps(data, indent=2)
        return self.create_temp_file(content, suffix)

    def cleanup(self) -> None:
        """Clean up temporary files and directories."""
        for temp_dir in self.temp_dirs:
            if temp_dir.exists():
                import shutil
                shutil.rmtree(temp_dir)

        for temp_file in self.temp_files:
            if temp_file.exists():
                temp_file.unlink()

        self.temp_dirs.clear()
        self.temp_files.clear()

        # Clear request history in all services
        for service in self.mock_services.values():
            service.clear_history()


@contextmanager
def hermetic_environment() -> Generator[HermeticTestEnvironment, None, None]:
    """Context manager for hermetic test environment."""
    env = HermeticTestEnvironment()
    env.setup_standard_mocks()

    try:
        yield env
    finally:
        env.cleanup()


@asynccontextmanager
async def hermetic_async_environment() -> Generator[HermeticTestEnvironment, None, None]:
    """Async context manager for hermetic test environment."""
    env = HermeticTestEnvironment()
    env.setup_standard_mocks()

    try:
        yield env
    finally:
        env.cleanup()


class MockHTTPClient:
    """Mock HTTP client that uses hermetic services."""

    def __init__(self, hermetic_env: HermeticTestEnvironment):
        self.env = hermetic_env

    async def get(self, url: str, **kwargs) -> Dict[str, Any]:
        """Mock GET request."""
        service_name, endpoint = self._parse_url(url)
        service = self.env.get_mock_service(service_name)
        service.record_request(endpoint, "GET", **kwargs)
        return service.get_response(endpoint, "GET")

    async def post(self, url: str, **kwargs) -> Dict[str, Any]:
        """Mock POST request."""
        service_name, endpoint = self._parse_url(url)
        service = self.env.get_mock_service(service_name)
        service.record_request(endpoint, "POST", **kwargs)
        return service.get_response(endpoint, "POST")

    async def put(self, url: str, **kwargs) -> Dict[str, Any]:
        """Mock PUT request."""
        service_name, endpoint = self._parse_url(url)
        service = self.env.get_mock_service(service_name)
        service.record_request(endpoint, "PUT", **kwargs)
        return service.get_response(endpoint, "PUT")

    def _parse_url(self, url: str) -> tuple[str, str]:
        """Parse URL to extract service name and endpoint."""
        # Simple URL parsing for mocking
        if "eia.gov" in url:
            service_name = "EIA"
            endpoint = url.split("eia.gov")[1]
        elif "trino" in url.lower():
            service_name = "Trino"
            endpoint = url.split("8080")[1] if "8080" in url else "/v1/statement"
        elif "kafka" in url.lower():
            service_name = "Kafka"
            endpoint = url.split("8080")[1] if "8080" in url else "/brokers"
        else:
            raise ValueError(f"Unknown service for URL: {url}")

        return service_name, endpoint


class TestDataGenerator:
    """Generate test data for hermetic tests."""

    @staticmethod
    def generate_scenario_data(tenant_id: str = "test_tenant") -> Dict[str, Any]:
        """Generate test scenario data."""
        return {
            "id": "test_scenario_id",
            "name": "Test Scenario",
            "description": "A test scenario for unit testing",
            "tenant_id": tenant_id,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
            "parameters": {
                "input_data": "test_data",
                "output_format": "json",
                "timeout": 300
            }
        }

    @staticmethod
    def generate_eia_series_data() -> Dict[str, Any]:
        """Generate test EIA series data."""
        return {
            "series_id": "ELEC.PRICE.NY-RES.M",
            "name": "New York Residential Electric Price",
            "data": [
                {"period": "2024-01", "value": "12.5"},
                {"period": "2023-12", "value": "12.3"},
                {"period": "2023-11", "value": "12.1"}
            ]
        }

    @staticmethod
    def generate_kafka_message(topic: str = "test_topic") -> Dict[str, Any]:
        """Generate test Kafka message."""
        return {
            "topic": topic,
            "key": "test_key",
            "value": {"message": "test message", "timestamp": "2024-01-01T00:00:00Z"},
            "headers": {"source": "test"}
        }

    @staticmethod
    def generate_trino_query_result() -> Dict[str, Any]:
        """Generate test Trino query result."""
        return {
            "columns": ["id", "name", "value"],
            "data": [
                [1, "test1", 100.0],
                [2, "test2", 200.0],
                [3, "test3", 300.0]
            ],
            "stats": {
                "state": "FINISHED",
                "processedRows": 3,
                "processedBytes": 1024
            }
        }


# Pytest fixtures
@pytest.fixture
def hermetic_env():
    """Pytest fixture for hermetic test environment."""
    env = HermeticTestEnvironment()
    env.setup_standard_mocks()
    yield env
    env.cleanup()


@pytest.fixture
def mock_http_client(hermetic_env):
    """Pytest fixture for mock HTTP client."""
    return MockHTTPClient(hermetic_env)


@pytest.fixture
def test_scenario_data():
    """Pytest fixture for test scenario data."""
    return TestDataGenerator.generate_scenario_data()


@pytest.fixture
def test_eia_data():
    """Pytest fixture for test EIA data."""
    return TestDataGenerator.generate_eia_series_data()


@pytest.fixture
def test_kafka_message():
    """Pytest fixture for test Kafka message."""
    return TestDataGenerator.generate_kafka_message()


@pytest.fixture
def test_trino_result():
    """Pytest fixture for test Trino query result."""
    return TestDataGenerator.generate_trino_query_result()


# Export all fixtures and utilities
__all__ = [
    "MockExternalService",
    "MockEIAProvider",
    "MockTrinoService",
    "MockKafkaService",
    "HermeticTestEnvironment",
    "hermetic_environment",
    "hermetic_async_environment",
    "MockHTTPClient",
    "TestDataGenerator",
    "hermetic_env",
    "mock_http_client",
    "test_scenario_data",
    "test_eia_data",
    "test_kafka_message",
    "test_trino_result",
]
