"""Shared fixtures for service tests."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from aurum.services.base import ServiceContext


@pytest.fixture
def service_context():
    """Create a test service context."""
    return ServiceContext(
        tenant_id="test-tenant",
        user_id="test-user",
        request_id="test-request-123",
        correlation_id="test-correlation-456"
    )


@pytest.fixture
def mock_curve_repo():
    """Mock curve repository for service tests."""
    repo = AsyncMock()
    repo.find_by_filters = AsyncMock(return_value=[])
    repo.find_by_key = AsyncMock(return_value=[])
    repo.get_latest_asof = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def mock_scenario_repo():
    """Mock scenario repository for service tests."""
    repo = AsyncMock()
    repo.find_by_id = AsyncMock(return_value=None)
    repo.list_scenarios = AsyncMock(return_value=[])
    repo.create_scenario = AsyncMock(return_value={})
    repo.get_scenario_outputs = AsyncMock(return_value=[])
    return repo


@pytest.fixture
def mock_metadata_repo():
    """Mock metadata repository for service tests."""
    repo = AsyncMock()
    repo.get_dimensions = AsyncMock(return_value=[])
    repo.get_all_dimensions = AsyncMock(return_value={})
    repo.search_metadata = AsyncMock(return_value=[])
    return repo

