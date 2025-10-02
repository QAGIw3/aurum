"""Unit tests for ScenarioService."""

import pytest
from unittest.mock import AsyncMock
from uuid import uuid4

from aurum.services.core import ScenarioService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError


@pytest.fixture
def mock_scenario_repo():
    """Mock scenario repository."""
    repo = AsyncMock()
    repo.find_by_id = AsyncMock(return_value=None)
    repo.list_scenarios = AsyncMock(return_value=[])
    repo.create_scenario = AsyncMock(return_value={})
    repo.get_scenario_outputs = AsyncMock(return_value=[])
    return repo


@pytest.fixture
def scenario_service(mock_scenario_repo):
    """Create service with mocked repository."""
    return ScenarioService(mock_scenario_repo)


@pytest.fixture
def service_context():
    """Create test service context."""
    return ServiceContext(tenant_id="test-tenant", user_id="test-user")


@pytest.mark.asyncio
async def test_create_scenario_success(scenario_service, mock_scenario_repo, service_context):
    """Test successful scenario creation."""
    # Arrange
    scenario_id = str(uuid4())
    mock_scenario_repo.create_scenario.return_value = {
        "id": scenario_id,
        "name": "Test Scenario",
        "description": "Test description",
        "tenant_id": "test-tenant"
    }
    
    # Act
    result = await scenario_service.create_scenario(
        name="Test Scenario",
        description="Test description",
        assumptions={"price_increase": 0.1},
        context=service_context
    )
    
    # Assert
    assert result.success
    assert result.data["name"] == "Test Scenario"
    assert result.metadata["created"] is True
    mock_scenario_repo.create_scenario.assert_called_once()


@pytest.mark.asyncio
async def test_create_scenario_invalid_name(scenario_service):
    """Test scenario creation with invalid name."""
    with pytest.raises(ValidationError) as exc:
        await scenario_service.create_scenario(name="")
    
    assert "name is required" in str(exc.value).lower()


@pytest.mark.asyncio
async def test_create_scenario_name_too_long(scenario_service):
    """Test scenario creation with name too long."""
    long_name = "x" * 300
    
    with pytest.raises(ValidationError) as exc:
        await scenario_service.create_scenario(name=long_name)
    
    assert "255 characters" in str(exc.value)


@pytest.mark.asyncio
async def test_get_scenario_success(scenario_service, mock_scenario_repo):
    """Test successful scenario retrieval."""
    # Arrange
    scenario_id = str(uuid4())
    mock_scenario_repo.find_by_id.return_value = {
        "id": scenario_id,
        "name": "Test Scenario",
        "tenant_id": "test-tenant"
    }
    
    # Act
    result = await scenario_service.get_scenario(scenario_id)
    
    # Assert
    assert result.success
    assert result.data["id"] == scenario_id


@pytest.mark.asyncio
async def test_get_scenario_invalid_uuid(scenario_service):
    """Test getting scenario with invalid UUID."""
    with pytest.raises(ValidationError) as exc:
        await scenario_service.get_scenario("not-a-uuid")
    
    assert "Invalid scenario ID" in str(exc.value)


@pytest.mark.asyncio
async def test_get_scenario_not_found(scenario_service, mock_scenario_repo):
    """Test getting scenario that doesn't exist."""
    # Arrange
    scenario_id = str(uuid4())
    mock_scenario_repo.find_by_id.return_value = None
    
    # Act & Assert
    with pytest.raises(NotFoundError) as exc:
        await scenario_service.get_scenario(scenario_id)
    
    assert exc.value.resource == "scenario"
    assert exc.value.identifier == scenario_id


@pytest.mark.asyncio
async def test_get_scenario_wrong_tenant(scenario_service, mock_scenario_repo, service_context):
    """Test getting scenario from different tenant."""
    # Arrange
    scenario_id = str(uuid4())
    mock_scenario_repo.find_by_id.return_value = {
        "id": scenario_id,
        "tenant_id": "other-tenant"  # Different tenant
    }
    
    # Act & Assert
    with pytest.raises(NotFoundError):
        await scenario_service.get_scenario(scenario_id, context=service_context)


@pytest.mark.asyncio
async def test_list_scenarios(scenario_service, mock_scenario_repo, service_context):
    """Test listing scenarios."""
    # Arrange
    mock_scenario_repo.list_scenarios.return_value = [
        {"id": str(uuid4()), "name": "Scenario 1"},
        {"id": str(uuid4()), "name": "Scenario 2"}
    ]
    
    # Act
    result = await scenario_service.list_scenarios(
        limit=100,
        offset=0,
        context=service_context
    )
    
    # Assert
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["count"] == 2


@pytest.mark.asyncio
async def test_list_scenarios_invalid_limit(scenario_service):
    """Test listing with invalid limit."""
    with pytest.raises(ValidationError) as exc:
        await scenario_service.list_scenarios(limit=2000)
    
    assert "between 1 and 1000" in str(exc.value)


@pytest.mark.asyncio
async def test_get_scenario_outputs(scenario_service, mock_scenario_repo):
    """Test getting scenario outputs."""
    # Arrange
    scenario_id = str(uuid4())
    mock_scenario_repo.find_by_id.return_value = {"id": scenario_id}
    mock_scenario_repo.get_scenario_outputs.return_value = [
        {"output_id": 1, "value": 100},
        {"output_id": 2, "value": 200}
    ]
    
    # Act
    result = await scenario_service.get_scenario_outputs(scenario_id)
    
    # Assert
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["output_count"] == 2

