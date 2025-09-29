"""Configuration for integration tests with containers."""

import pytest
import httpx
from typing import Dict, Any, List
from fastapi import FastAPI

from tests.common import create_test_app, TestAppConfig
from tests.integration.containers import (
    postgres_dsn,
    timescale_dsn,
    kafka_bootstrap_servers,
    clickhouse_dsn,
    trino_dsn,
    database_urls,
)


@pytest.fixture(scope="function")
def integration_app_settings(database_urls: Dict[str, str]) -> TestAppConfig:
    """App settings configured for integration testing with containers."""
    settings = TestAppConfig()

    # Configure database connections
    if "postgres" in database_urls:
        # Parse postgres URL and configure settings
        pg_url = database_urls["postgres"]
        # In real implementation, parse the URL to extract host, port, etc.
        settings.backend_type = "postgres"

    if "kafka" in database_urls:
        settings.kafka_bootstrap_servers = [database_urls["kafka"]]
        settings.kafka_enabled = True

    return settings


@pytest.fixture(scope="function")
def integration_api_app(integration_app_settings: TestAppConfig) -> FastAPI:
    """FastAPI app configured for integration testing."""
    return create_test_app(integration_app_settings)


@pytest.fixture(scope="function")
def integration_api_client(integration_api_app: FastAPI) -> httpx.AsyncClient:
    """HTTP client for integration testing."""
    return httpx.AsyncClient(app=integration_api_app, base_url="http://test")


@pytest.fixture(scope="function")
def authenticated_integration_client(integration_api_client: httpx.AsyncClient) -> httpx.AsyncClient:
    """Authenticated HTTP client for integration testing."""
    # Add authentication headers
    integration_api_client.headers.update({
        "X-Aurum-Tenant": "test-tenant-integration",
        "X-User-ID": "test-user-integration",
        "X-Correlation-ID": "test-correlation-integration"
    })

    return integration_api_client


@pytest.fixture(scope="function")
def test_tenants() -> List[Dict[str, Any]]:
    """Test tenant configurations for multi-tenant testing."""
    return [
        {
            "id": "test-tenant-001",
            "name": "Test Tenant 1",
            "users": ["user-001", "user-002"],
            "settings": {"feature_flags": ["advanced_analytics"]}
        },
        {
            "id": "test-tenant-002",
            "name": "Test Tenant 2",
            "users": ["user-003", "user-004"],
            "settings": {"feature_flags": ["basic_analytics"]}
        },
        {
            "id": "test-tenant-003",
            "name": "Test Tenant 3",
            "users": ["user-005", "user-006"],
            "settings": {"feature_flags": ["experimental_features"]}
        }
    ]


@pytest.fixture(scope="function")
def test_scenarios() -> Dict[str, Any]:
    """Test scenario data for integration testing."""
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


@pytest.fixture(scope="function")
def database_setup(database_urls: Dict[str, str]) -> None:
    """Set up test databases with required fixtures."""
    # This would execute SQL fixtures against the containerized databases
    # For now, this is a placeholder that would be implemented based on
    # the actual database schema and fixtures needed

    # Example: Create tables, insert test data, etc.
    pass


@pytest.fixture(scope="function")
def clean_database_state(database_urls: Dict[str, str]) -> None:
    """Clean up database state after each test."""
    # This would clean up test data from the containerized databases
    # For now, this is a placeholder
    pass


# Integration test marker and configuration

def pytest_configure(config):
    """Configure pytest for integration tests."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test requiring containers"
    )


def pytest_collection_modifyitems(config, items):
    """Skip integration tests by default unless explicitly requested."""
    skip_integration = pytest.mark.skip(reason="integration tests require --integration flag")

    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
