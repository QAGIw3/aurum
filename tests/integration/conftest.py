"""Configuration for integration tests."""

import asyncio
import pytest
import httpx
from typing import AsyncGenerator, Dict, Any
from pathlib import Path

from aurum.telemetry.context import correlation_context


@pytest.fixture(scope="session")
async def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def api_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Create an authenticated API client for testing."""
    async with httpx.AsyncClient(
        base_url="http://localhost:8000",
        timeout=30.0
    ) as client:
        # Wait for API to be ready
        for i in range(60):
            try:
                response = await client.get("/health")
                if response.status_code == 200:
                    break
            except Exception:
                pass
            await asyncio.sleep(2)
        else:
            pytest.fail("API service did not become ready")

        yield client


@pytest.fixture(scope="session")
async def kafka_client() -> AsyncGenerator[Dict[str, Any], None]:
    """Create Kafka client for testing."""
    # For integration tests, we might use a test Kafka client
    # or mock the Kafka interactions
    yield {
        "type": "test_client",
        "bootstrap_servers": "localhost:9092"
    }


@pytest.fixture(scope="session")
async def trino_client() -> AsyncGenerator[Dict[str, Any], None]:
    """Create Trino client for testing."""
    yield {
        "type": "test_client",
        "host": "localhost",
        "port": 8080
    }


@pytest.fixture(scope="session")
async def postgres_client() -> AsyncGenerator[Dict[str, Any], None]:
    """Create PostgreSQL client for testing."""
    yield {
        "type": "test_client",
        "host": "localhost",
        "port": 15432,
        "database": "aurum_test",
        "user": "aurum_test",
        "password": "aurum_test"
    }


@pytest.fixture(scope="session")
async def test_tenant_context():
    """Provide test tenant context for multi-tenant testing."""
    tenants = [
        {
            "id": "test-tenant-001",
            "name": "Test Tenant 1",
            "users": ["user-001", "user-002"]
        },
        {
            "id": "test-tenant-002",
            "name": "Test Tenant 2",
            "users": ["user-003", "user-004"]
        },
        {
            "id": "test-tenant-003",
            "name": "Test Tenant 3",
            "users": ["user-005", "user-006"]
        }
    ]

    for tenant in tenants:
        yield tenant


@pytest.fixture(scope="function")
async def authenticated_api_client(api_client: httpx.AsyncClient, test_tenant_context):
    """Create an authenticated API client with tenant context."""
    tenant = test_tenant_context

    # Set up authentication headers
    auth_headers = {
        "X-Aurum-Tenant": tenant["id"],
        "X-User-ID": tenant["users"][0],
        "X-Correlation-ID": f"test-{tenant['id']}-{asyncio.current_task().get_name()}"
    }

    # Update client with auth headers
    api_client.headers.update(auth_headers)

    yield api_client


@pytest.fixture(scope="function")
async def scenario_test_data():
    """Provide test data for scenario testing."""
    return {
        "scenarios": [
            {
                "name": "Revenue Forecast Q1",
                "description": "Quarterly revenue forecasting scenario",
                "assumptions": [
                    {"type": "market_growth", "value": 0.05},
                    {"type": "discount_rate", "value": 0.08}
                ],
                "parameters": {
                    "forecast_period_months": 12,
                    "confidence_interval": 0.95,
                    "num_simulations": 1000
                }
            },
            {
                "name": "Stress Test - Recession",
                "description": "Stress testing scenario for recession conditions",
                "assumptions": [
                    {"type": "market_growth", "value": -0.02},
                    {"type": "volatility_multiplier", "value": 2.0}
                ],
                "parameters": {
                    "forecast_period_months": 24,
                    "confidence_interval": 0.99,
                    "num_simulations": 5000
                }
            }
        ],
        "run_options": {
            "monte_carlo": {
                "scenario_type": "monte_carlo",
                "parameters": {
                    "num_simulations": 100,
                    "confidence_level": 0.95
                }
            },
            "forecasting": {
                "scenario_type": "forecasting",
                "parameters": {
                    "forecast_period_months": 12,
                    "confidence_interval": 0.95
                }
            }
        }
    }


@pytest.fixture(scope="session")
async def test_database_setup():
    """Set up test database with required fixtures."""
    fixtures_dir = Path(__file__).parent / "fixtures"

    # Load and execute test fixtures
    for fixture_file in fixtures_dir.glob("*.sql"):
        # Execute SQL fixture files
        pass

    yield

    # Cleanup after tests
    # Drop test data, reset sequences, etc.


@pytest.fixture(scope="function")
async def clean_scenario_state(api_client: httpx.AsyncClient):
    """Clean up scenario state after each test."""
    yield

    # Clean up scenarios and runs created during test
    try:
        # This would clean up test data
        pass
    except Exception:
        pass
