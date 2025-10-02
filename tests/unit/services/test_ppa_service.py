"""Unit tests for PpaService."""

import pytest
from unittest.mock import AsyncMock
from datetime import date

from aurum.services.core import PpaService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError


@pytest.fixture
def mock_ppa_repo():
    """Mock PPA repository."""
    repo = AsyncMock()
    repo.get_ppa_contracts = AsyncMock(return_value=[])
    repo.get_ppa_valuations = AsyncMock(return_value=[])
    repo.get_ppa_risk_metrics = AsyncMock(return_value=[])
    repo.calculate_ppa_valuation = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def ppa_service(mock_ppa_repo):
    """Create service with mocked repository."""
    return PpaService(mock_ppa_repo)


@pytest.fixture
def service_context():
    """Create test service context."""
    return ServiceContext(tenant_id="test-tenant", user_id="test-user")


@pytest.mark.asyncio
async def test_get_ppa_contracts_success(ppa_service, mock_ppa_repo, service_context):
    """Test successful PPA contracts query."""
    # Arrange
    mock_ppa_repo.get_ppa_contracts.return_value = [
        {"contract_id": "PPA-001", "counterparty": "Test Corp"},
        {"contract_id": "PPA-002", "counterparty": "Another Corp"}
    ]

    # Act
    result = await ppa_service.get_ppa_contracts(
        counterparty="Test Corp",
        limit=100,
        context=service_context
    )

    # Assert
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["contract_count"] == 2

    mock_ppa_repo.get_ppa_contracts.assert_called_once()


@pytest.mark.asyncio
async def test_get_ppa_contracts_invalid_limit(ppa_service):
    """Test contracts query with invalid limit."""
    with pytest.raises(ValidationError) as exc:
        await ppa_service.get_ppa_contracts(limit=2000)

    assert "between 1 and 1000" in str(exc.value)


@pytest.mark.asyncio
async def test_get_ppa_contracts_invalid_date_range(ppa_service):
    """Test contracts query with invalid date range."""
    future_date = date(2025, 12, 31)
    past_date = date(2024, 1, 1)

    with pytest.raises(ValidationError) as exc:
        await ppa_service.get_ppa_contracts(
            start_date=future_date,
            end_date=past_date
        )

    assert "Start date must be before end date" in str(exc.value)


@pytest.mark.asyncio
async def test_get_ppa_valuations_success(ppa_service, mock_ppa_repo):
    """Test successful PPA valuations query."""
    # Arrange
    mock_ppa_repo.get_ppa_valuations.return_value = [
        {"contract_id": "PPA-001", "valuation_date": "2024-01-15", "value": 100000},
        {"contract_id": "PPA-001", "valuation_date": "2024-01-16", "value": 105000}
    ]

    # Act
    result = await ppa_service.get_ppa_valuations(
        contract_id="PPA-001",
        limit=100
    )

    # Assert
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["contract_id"] == "PPA-001"


@pytest.mark.asyncio
async def test_get_ppa_valuations_invalid_contract_id(ppa_service):
    """Test valuations query with invalid contract ID."""
    with pytest.raises(ValidationError) as exc:
        await ppa_service.get_ppa_valuations(contract_id="")

    assert "Contract ID is required" in str(exc.value)


@pytest.mark.asyncio
async def test_calculate_contract_valuation_success(ppa_service, mock_ppa_repo):
    """Test successful contract valuation calculation."""
    # Arrange
    contract = {"contract_id": "PPA-001", "start_date": "2024-01-01", "end_date": "2025-12-31"}
    mock_ppa_repo.get_ppa_contracts.return_value = [contract]
    mock_ppa_repo.calculate_ppa_valuation.return_value = {
        "contract_id": "PPA-001",
        "asof_date": "2024-01-15",
        "contract_value": 500000.0
    }

    # Act
    result = await ppa_service.calculate_contract_valuation(
        contract_id="PPA-001",
        asof_date=date(2024, 1, 15)
    )

    # Assert
    assert result.success
    assert result.data["contract_id"] == "PPA-001"
    assert "var_95" in result.data  # Risk metrics added


@pytest.mark.asyncio
async def test_calculate_contract_valuation_contract_not_found(ppa_service, mock_ppa_repo):
    """Test valuation calculation for non-existent contract."""
    # Arrange
    mock_ppa_repo.get_ppa_contracts.return_value = []

    # Act & Assert
    with pytest.raises(NotFoundError) as exc:
        await ppa_service.calculate_contract_valuation(
            contract_id="NONEXISTENT",
            asof_date=date(2024, 1, 15)
        )

    assert exc.value.resource == "ppa_contract"
    assert exc.value.identifier == "NONEXISTENT"


@pytest.mark.asyncio
async def test_calculate_contract_valuation_inactive_contract(ppa_service, mock_ppa_repo):
    """Test valuation for inactive contract."""
    # Arrange - contract not active on valuation date
    contract = {"contract_id": "PPA-001", "start_date": "2024-06-01", "end_date": "2024-12-31"}
    mock_ppa_repo.get_ppa_contracts.return_value = [contract]

    # Act & Assert
    with pytest.raises(ValidationError) as exc:
        await ppa_service.calculate_contract_valuation(
            contract_id="PPA-001",
            asof_date=date(2024, 1, 15)  # Before contract starts
        )

    assert "not active" in str(exc.value)


@pytest.mark.asyncio
async def test_get_contract_risk_metrics_success(ppa_service, mock_ppa_repo):
    """Test successful risk metrics query."""
    # Arrange
    mock_ppa_repo.get_ppa_risk_metrics.return_value = [
        {"contract_id": "PPA-001", "risk_metric": "VaR", "value": 50000},
        {"contract_id": "PPA-001", "risk_metric": "CVaR", "value": 80000}
    ]

    # Act
    result = await ppa_service.get_contract_risk_metrics(
        contract_id="PPA-001",
        risk_metrics=["VaR", "CVaR"]
    )

    # Assert
    assert result.success
    assert result.data["contract_id"] == "PPA-001"
    assert "portfolio_var" in result.data["risk_metrics"]


@pytest.mark.asyncio
async def test_get_contract_risk_metrics_invalid_metric(ppa_service):
    """Test risk metrics with invalid metric name."""
    with pytest.raises(ValidationError) as exc:
        await ppa_service.get_contract_risk_metrics(
            contract_id="PPA-001",
            risk_metrics=["invalid_metric"]
        )

    assert "Invalid risk metric" in str(exc.value)

