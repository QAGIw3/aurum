"""Enhanced testing utilities and fixtures."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Type, TypeVar
from unittest.mock import AsyncMock, MagicMock, Mock

from fastapi.testclient import TestClient
from pydantic import BaseModel

from .container import ServiceProvider
from .exceptions import AurumAPIException

T = TypeVar('T')


class TestDataFactory:
    """Factory for generating realistic test data."""

    @staticmethod
    def create_curve_point(
        iso: str = "PJM",
        market: str = "DAY_AHEAD",
        location: str = "HUB",
        asof_date: str = "2024-01-01",
        mid: float = 50.0,
        bid: Optional[float] = 49.5,
        ask: Optional[float] = 50.5,
    ) -> Dict[str, Any]:
        """Create a realistic curve point for testing."""
        return {
            "iso": iso,
            "market": market,
            "location": location,
            "asof_date": asof_date,
            "tenor_label": "2024-01",
            "mid": mid,
            "bid": bid,
            "ask": ask,
            "price_type": "MID",
            "curve_key": f"{iso}_{market}_{location}",
        }

    @staticmethod
    def create_curve_points(
        count: int = 10,
        start_mid: float = 50.0,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Create multiple curve points for testing."""
        return [
            TestDataFactory.create_curve_point(
                mid=start_mid + i * 0.1,
                **kwargs
            )
            for i in range(count)
        ]

    @staticmethod
    def create_scenario_data(
        tenant_id: str = "test-tenant",
        name: str = "Test Scenario",
        assumptions: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Create scenario data for testing."""
        if assumptions is None:
            assumptions = [
                {
                    "driver_type": "policy",
                    "payload": {
                        "policy_name": "RPS",
                        "start_year": 2030,
                    }
                }
            ]

        return {
            "tenant_id": tenant_id,
            "name": name,
            "assumptions": assumptions,
        }

    @staticmethod
    def create_metadata_dimensions() -> Dict[str, List[str]]:
        """Create metadata dimensions for testing."""
        return {
            "iso": ["PJM", "CAISO", "ERCOT", "NYISO", "MISO"],
            "market": ["DAY_AHEAD", "REAL_TIME", "FIFTEEN_MINUTE"],
            "location": ["HUB", "ZONE_A", "ZONE_B", "INTERFACE_1"],
            "product": ["ENERGY", "CAPACITY", "ANCILLARY"],
            "block": ["ON_PEAK", "OFF_PEAK", "2X16"],
        }


class MockServiceProvider(ServiceProvider):
    """Mock service provider for testing."""

    def __init__(self):
        self._mocks: Dict[Type, Any] = {}

    def register_mock(self, service_type: Type[T], mock_instance: T) -> None:
        """Register a mock instance for a service type."""
        self._mocks[service_type] = mock_instance

    def get(self, service_type: Type[T]) -> T:
        """Get a service instance, returning mock if available."""
        if service_type in self._mocks:
            return self._mocks[service_type]

        # Create a default mock
        mock = MagicMock(spec=service_type)
        self._mocks[service_type] = mock
        return mock

    def reset(self) -> None:
        """Reset all mocks."""
        for mock in self._mocks.values():
            if hasattr(mock, 'reset_mock'):
                mock.reset_mock()
        self._mocks.clear()


class AsyncTestClient:
    """Async test client with enhanced testing capabilities."""

    def __init__(self, app):
        self.client = TestClient(app)
        self.async_client = AsyncMock()

    def get(self, path: str, **kwargs) -> Any:
        """Make GET request."""
        return self.client.get(path, **kwargs)

    def post(self, path: str, **kwargs) -> Any:
        """Make POST request."""
        return self.client.post(path, **kwargs)

    def put(self, path: str, **kwargs) -> Any:
        """Make PUT request."""
        return self.client.put(path, **kwargs)

    def patch(self, path: str, **kwargs) -> Any:
        """Make PATCH request."""
        return self.client.patch(path, **kwargs)

    def delete(self, path: str, **kwargs) -> Any:
        """Make DELETE request."""
        return self.client.delete(path, **kwargs)

    async def get_async(self, path: str, **kwargs) -> Any:
        """Make async GET request."""
        # This would integrate with httpx AsyncClient in real implementation
        return await self.async_client.get(path, **kwargs)

    async def post_async(self, path: str, **kwargs) -> Any:
        """Make async POST request."""
        return await self.async_client.post(path, **kwargs)


class PerformanceTestHelper:
    """Helper for performance testing."""

    def __init__(self):
        self.metrics = []

    def measure_execution_time(self, func):
        """Decorator to measure function execution time."""
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time

            self.metrics.append({
                "function": func.__name__,
                "execution_time": execution_time,
                "args": str(args),
                "kwargs": str(kwargs),
            })

            return result

        return wrapper

    def get_metrics(self) -> List[Dict[str, Any]]:
        """Get performance metrics."""
        return self.metrics.copy()

    def get_average_time(self, function_name: str) -> float:
        """Get average execution time for a function."""
        times = [
            m["execution_time"]
            for m in self.metrics
            if m["function"] == function_name
        ]
        return sum(times) / len(times) if times else 0


class DatabaseTestHelper:
    """Helper for database testing."""

    def __init__(self):
        self.test_data = {}

    def setup_test_data(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Setup test data for a table."""
        self.test_data[table_name] = data

    def get_test_data(self, table_name: str) -> List[Dict[str, Any]]:
        """Get test data for a table."""
        return self.test_data.get(table_name, [])

    def clear_test_data(self, table_name: str) -> None:
        """Clear test data for a table."""
        self.test_data.pop(table_name, None)


class APITestCase:
    """Base test case with common testing utilities."""

    def __init__(self):
        self.data_factory = TestDataFactory()
        self.service_provider = MockServiceProvider()
        self.performance_helper = PerformanceTestHelper()
        self.db_helper = DatabaseTestHelper()

    def create_test_curve_data(self, count: int = 5) -> List[Dict[str, Any]]:
        """Create test curve data."""
        return self.data_factory.create_curve_points(count)

    def create_test_scenario(self) -> Dict[str, Any]:
        """Create test scenario data."""
        return self.data_factory.create_scenario_data()

    def mock_service(self, service_type: Type[T], mock_instance: T) -> T:
        """Register a mock service."""
        self.service_provider.register_mock(service_type, mock_instance)
        return mock_instance

    def measure_performance(self, func):
        """Measure performance of a function."""
        return self.performance_helper.measure_execution_time(func)


class LoadTestHelper:
    """Helper for load testing."""

    def __init__(self, concurrency: int = 10):
        self.concurrency = concurrency
        self.results = []

    async def run_load_test(
        self,
        test_func,
        num_requests: int = 100,
        **kwargs
    ) -> Dict[str, Any]:
        """Run a load test with concurrent requests."""
        start_time = time.time()

        # Create tasks
        tasks = [
            test_func(**kwargs)
            for _ in range(num_requests)
        ]

        # Execute concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_time = time.time() - start_time

        # Analyze results
        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = len(results) - successful

        return {
            "total_requests": num_requests,
            "successful_requests": successful,
            "failed_requests": failed,
            "success_rate": successful / num_requests,
            "total_time": total_time,
            "requests_per_second": num_requests / total_time,
            "average_time_per_request": total_time / num_requests,
        }


# Test fixtures
def create_test_app():
    """Create a test FastAPI application."""
    from fastapi import FastAPI
    from .health import router as health_router
    from .curves import router as curves_router
    from .metadata import router as metadata_router

    app = FastAPI(title="Aurum API - Test")
    app.include_router(health_router)
    app.include_router(curves_router)
    app.include_router(metadata_router)

    return app


def create_test_client():
    """Create a test client."""
    app = create_test_app()
    return AsyncTestClient(app)


def create_mock_cache_service():
    """Create a mock cache service."""
    cache_service = Mock()
    cache_service.get = AsyncMock(return_value=None)
    cache_service.set = AsyncMock()
    cache_service.delete = AsyncMock()
    return cache_service


def create_mock_curve_service():
    """Create a mock curve service."""
    service = Mock()
    service.fetch_curves = AsyncMock()
    service.fetch_curve_diff = AsyncMock()
    service.fetch_curve_strips = AsyncMock()
    return service


def create_mock_metadata_service():
    """Create a mock metadata service."""
    service = Mock()
    service.get_dimensions = AsyncMock()
    service.get_iso_locations = AsyncMock()
    service.get_units_canonical = AsyncMock()
    return service
