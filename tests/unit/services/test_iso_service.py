"""Unit tests for IsoService."""

import pytest
from unittest.mock import AsyncMock
from datetime import date

from aurum.services.core import IsoService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError


@pytest.fixture
def mock_metadata_repo():
    """Mock metadata repository."""
    repo = AsyncMock()
    repo.get_dimensions = AsyncMock(return_value=["PJM", "ERCOT", "CAISO"])
    return repo


@pytest.fixture
def iso_service(mock_metadata_repo):
    """Create service with mocked repository."""
    return IsoService(mock_metadata_repo)


@pytest.fixture
def service_context():
    """Create test service context."""
    return ServiceContext(tenant_id="test-tenant", user_id="test-user")


@pytest.mark.asyncio
async def test_get_lmp_data_success(iso_service, service_context):
    """Test successful LMP data query."""
    # Act
    result = await iso_service.get_lmp_data(
        iso="PJM",
        node="PJM_HUB",
        market_type="DA",
        limit=100,
        context=service_context
    )

    # Assert
    assert result.success
    assert result.metadata["iso"] == "PJM"
    assert result.metadata["node"] == "PJM_HUB"
    assert result.metadata["market_type"] == "DA"


@pytest.mark.asyncio
async def test_get_lmp_data_invalid_iso(iso_service):
    """Test LMP data query with invalid ISO."""
    with pytest.raises(ValidationError) as exc:
        await iso_service.get_lmp_data(iso="")

    assert "ISO identifier is required" in str(exc.value)


@pytest.mark.asyncio
async def test_get_lmp_data_invalid_date_range(iso_service):
    """Test LMP data query with invalid date range."""
    future_date = date(2025, 12, 31)
    past_date = date(2024, 1, 1)

    with pytest.raises(ValidationError) as exc:
        await iso_service.get_lmp_data(
            iso="PJM",
            start_date=future_date,
            end_date=past_date
        )

    assert "Start date must be before end date" in str(exc.value)


@pytest.mark.asyncio
async def test_get_lmp_data_invalid_limit(iso_service):
    """Test LMP data query with invalid limit."""
    with pytest.raises(ValidationError) as exc:
        await iso_service.get_lmp_data(iso="PJM", limit=20000)

    assert "between 1 and 10000" in str(exc.value)


@pytest.mark.asyncio
async def test_get_iso_markets_success(iso_service):
    """Test successful ISO markets query."""
    # Act
    result = await iso_service.get_iso_markets(iso="PJM")

    # Assert
    assert result.success
    assert result.data["iso"] == "PJM"
    assert "markets" in result.data
    assert result.data["market_count"] >= 2  # PJM should have DA and RT


@pytest.mark.asyncio
async def test_get_iso_markets_invalid_iso(iso_service):
    """Test markets query with invalid ISO."""
    with pytest.raises(ValidationError) as exc:
        await iso_service.get_iso_markets(iso="INVALID_ISO")

    assert "ISO identifier is required" in str(exc.value)


@pytest.mark.asyncio
async def test_get_iso_nodes_success(iso_service):
    """Test successful ISO nodes query."""
    # Act
    result = await iso_service.get_iso_nodes(iso="PJM", limit=50)

    # Assert
    assert result.success
    assert result.data["iso"] == "PJM"
    assert "nodes" in result.data
    assert result.data["node_count"] >= 1


@pytest.mark.asyncio
async def test_get_iso_nodes_invalid_market_type(iso_service):
    """Test nodes query with invalid market type."""
    with pytest.raises(ValidationError) as exc:
        await iso_service.get_iso_nodes(iso="PJM", market_type="INVALID")

    assert "Invalid market type" in str(exc.value)


@pytest.mark.asyncio
async def test_get_market_summary_success(iso_service):
    """Test successful market summary query."""
    # Act
    result = await iso_service.get_market_summary(iso="PJM")

    # Assert
    assert result.success
    assert result.data["iso"] == "PJM"
    assert "total_volume_mwh" in result.data
    assert "avg_price_dollar_per_mwh" in result.data


@pytest.mark.asyncio
async def test_get_market_summary_invalid_iso(iso_service):
    """Test market summary with invalid ISO."""
    with pytest.raises(ValidationError) as exc:
        await iso_service.get_market_summary(iso="")

    assert "ISO identifier is required" in str(exc.value)

