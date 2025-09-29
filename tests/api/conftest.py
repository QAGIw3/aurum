from __future__ import annotations

"""API tests configuration shim.

Provides a local `pytest_plugins` hook to avoid missing module errors when
plugins are referenced as `api.conftest` in legacy tests.
"""

pytest_plugins = []

import pytest
import httpx
from typing import Dict, Any, Optional
from fastapi import FastAPI

from tests.common import create_test_app, TestAppConfig


@pytest.fixture(scope="function")
def app_settings() -> TestAppConfig:
    """Base settings for API tests."""
    return TestAppConfig()


@pytest.fixture(scope="function")
def api_app(app_settings: TestAppConfig) -> FastAPI:
    """Create a FastAPI app for API testing."""
    return create_test_app(app_settings)


@pytest.fixture(scope="function")
def api_client(api_app: FastAPI):
    """Create an HTTP client for testing the API app."""
    from fastapi.testclient import TestClient
    return TestClient(api_app)


@pytest.fixture(scope="function")
def api_client_with_auth(api_app: FastAPI, settings_override):
    """Create an authenticated HTTP client for testing."""
    # Set up authentication environment
    settings_override["set"]("AURUM_API_AUTH_DISABLED", "0")

    from fastapi.testclient import TestClient
    client = TestClient(api_app)

    # Add authentication headers
    client.headers.update({
        "X-Aurum-Tenant": "test-tenant",
        "X-User-ID": "test-user",
        "X-Correlation-ID": "test-correlation-id"
    })

    yield client

    # Cleanup
    settings_override["del"]("AURUM_API_AUTH_DISABLED")
