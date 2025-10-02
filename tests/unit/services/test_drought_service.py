"""Unit tests for DroughtService."""

import pytest
from unittest.mock import AsyncMock
from datetime import date

from aurum.services.core import DroughtService
from aurum.services.base import ServiceContext, ValidationError


@pytest.fixture
def mock_drought_repo():
    """Mock drought repository."""
    repo = AsyncMock()
    repo.query_drought_indices = AsyncMock(return_value=[])
    repo.query_usdm_data = AsyncMock(return_value=[])
    repo.get_drought_statistics = AsyncMock(return_value={})
    repo.get_latest_drought_data = AsyncMock(return_value=[])
    return repo


@pytest.fixture
def drought_service(mock_drought_repo):
    """Create service with mocked repository."""
    return DroughtService(mock_drought_repo)


@pytest.fixture
def service_context():
    """Create test service context."""
    return ServiceContext(tenant_id="test-tenant", user_id="test-user")


@pytest.mark.asyncio
async def test_get_drought_indices_success(drought_service, mock_drought_repo, service_context):
    """Test successful drought indices query."""
    # Arrange
    mock_drought_repo.query_drought_indices.return_value = [
        {"region_id": "CA", "index_value": -1.5, "date": "2024-01-15"},
        {"region_id": "CA", "index_value": -2.0, "date": "2024-01-16"}
    ]

    # Act
    result = await drought_service.get_drought_indices(
        region_type="state",
        region_id="CA",
        dataset="spi",
        limit=100,
        context=service_context
    )

    # Assert
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["region_type"] == "state"
    assert result.metadata["dataset"] == "spi"

    # Check that data was enriched with drought classification
    assert "drought_classification" in result.data[0]
    assert "trend" in result.data[0]


@pytest.mark.asyncio
async def test_get_drought_indices_invalid_region_type(drought_service):
    """Test drought indices query with invalid region type."""
    with pytest.raises(ValidationError) as exc:
        await drought_service.get_drought_indices(region_type="invalid")

    assert "Invalid region type" in str(exc.value)


@pytest.mark.asyncio
async def test_get_drought_indices_invalid_limit(drought_service):
    """Test drought indices query with invalid limit."""
    with pytest.raises(ValidationError) as exc:
        await drought_service.get_drought_indices(limit=2000)

    assert "between 1 and 1000" in str(exc.value)


@pytest.mark.asyncio
async def test_get_usdm_data_success(drought_service, mock_drought_repo):
    """Test successful USDM data query."""
    # Arrange
    mock_drought_repo.query_usdm_data.return_value = [
        {"region_id": "CA", "drought_category": 2, "valid_date": "2024-01-15"}
    ]

    # Act
    result = await drought_service.get_usdm_data(
        region_type="state",
        region_id="CA",
        limit=100
    )

    # Assert
    assert result.success
    assert len(result.data) == 1
    assert result.metadata["region_type"] == "state"

    # Check that data was enriched with drought descriptions
    assert "drought_description" in result.data[0]


@pytest.mark.asyncio
async def test_get_drought_statistics_success(drought_service, mock_drought_repo):
    """Test successful drought statistics query."""
    # Arrange
    mock_drought_repo.get_drought_statistics.return_value = {
        "total_observations": 100,
        "avg_index_value": -0.5,
        "drought_episodes": 25,
        "severe_drought_episodes": 5
    }

    # Act
    result = await drought_service.get_drought_statistics(
        region_type="state",
        region_id="CA",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31)
    )

    # Assert
    assert result.success
    assert result.data["total_observations"] == 100
    assert result.data["drought_frequency"] == 0.25  # 25/100
    assert "analysis_period_days" in result.data


@pytest.mark.asyncio
async def test_get_drought_statistics_invalid_date_range(drought_service):
    """Test drought statistics with invalid date range."""
    future_date = date(2025, 12, 31)
    past_date = date(2024, 1, 1)

    with pytest.raises(ValidationError) as exc:
        await drought_service.get_drought_statistics(
            region_type="state",
            region_id="CA",
            start_date=future_date,
            end_date=past_date
        )

    assert "Start date must be before end date" in str(exc.value)


@pytest.mark.asyncio
async def test_get_latest_drought_data_success(drought_service, mock_drought_repo):
    """Test successful latest drought data query."""
    # Arrange
    mock_drought_repo.get_latest_drought_data.return_value = [
        {"region_id": "CA", "index_value": -1.0, "date": "2024-01-15"}
    ]

    # Act
    result = await drought_service.get_latest_drought_data(
        region_type="state",
        limit=50
    )

    # Assert
    assert result.success
    assert len(result.data) == 1
    assert result.metadata["region_type"] == "state"

    # Check that status indicators were added
    assert "status_as_of" in result.data[0]
    assert "alert_level" in result.data[0]


@pytest.mark.asyncio
async def test_get_latest_drought_data_invalid_limit(drought_service):
    """Test latest drought data query with invalid limit."""
    with pytest.raises(ValidationError) as exc:
        await drought_service.get_latest_drought_data(limit=2000)

    assert "between 1 and 1000" in str(exc.value)


@pytest.mark.asyncio
async def test_drought_classification_logic(drought_service):
    """Test drought classification business logic."""
    # This tests the private methods indirectly through the service
    mock_drought_repo.query_drought_indices.return_value = [
        {"region_id": "CA", "index_value": 1.5, "date": "2024-01-15"},  # Wet
        {"region_id": "CA", "index_value": 0.2, "date": "2024-01-16"},  # Normal
        {"region_id": "CA", "index_value": -0.8, "date": "2024-01-17"}, # Mild drought
        {"region_id": "CA", "index_value": -1.2, "date": "2024-01-18"}, # Moderate drought
        {"region_id": "CA", "index_value": -2.5, "date": "2024-01-19"}, # Severe drought
        {"region_id": "CA", "index_value": -3.0, "date": "2024-01-20"}, # Extreme drought
    ]

    result = await drought_service.get_drought_indices(
        region_type="state",
        region_id="CA",
        limit=10
    )

    assert result.success
    assert len(result.data) == 6

    # Check classifications
    classifications = [item["drought_classification"] for item in result.data]
    expected = ["wet", "normal", "mild_drought", "moderate_drought", "severe_drought", "extreme_drought"]
    assert classifications == expected


@pytest.mark.asyncio
async def test_usdm_enrichment(drought_service, mock_drought_repo):
    """Test USDM data enrichment."""
    # Arrange
    mock_drought_repo.query_usdm_data.return_value = [
        {"region_id": "CA", "drought_category": 0, "valid_date": "2024-01-15"},  # Abnormally Dry
        {"region_id": "CA", "drought_category": 1, "valid_date": "2024-01-16"},  # Moderate Drought
        {"region_id": "CA", "drought_category": 2, "valid_date": "2024-01-17"},  # Severe Drought
    ]

    # Act
    result = await drought_service.get_usdm_data(
        region_type="state",
        region_id="CA",
        limit=10
    )

    # Assert
    assert result.success
    assert len(result.data) == 3

    # Check descriptions
    descriptions = [item["drought_description"] for item in result.data]
    expected = ["Abnormally Dry", "Moderate Drought", "Severe Drought"]
    assert descriptions == expected

