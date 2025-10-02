"""Unit tests for MetadataService."""

import pytest
from unittest.mock import AsyncMock

from aurum.services.core import MetadataService
from aurum.services.base import ValidationError


@pytest.fixture
def mock_metadata_repo():
    """Mock metadata repository."""
    repo = AsyncMock()
    repo.get_dimensions = AsyncMock(return_value=[])
    repo.get_all_dimensions = AsyncMock(return_value={})
    repo.search_metadata = AsyncMock(return_value=[])
    return repo


@pytest.fixture
def metadata_service(mock_metadata_repo):
    """Create service with mocked repository."""
    return MetadataService(mock_metadata_repo)


@pytest.mark.asyncio
async def test_get_dimensions_success(metadata_service, mock_metadata_repo):
    """Test successful dimension query."""
    # Arrange
    mock_metadata_repo.get_dimensions.return_value = ["PJM", "ERCOT", "CAISO"]
    
    # Act
    result = await metadata_service.get_dimensions(
        dataset="curves",
        dimension="iso"
    )
    
    # Assert
    assert result.success
    assert len(result.data) == 3
    assert "PJM" in result.data
    assert result.metadata["dimension"] == "iso"
    assert result.metadata["count"] == 3


@pytest.mark.asyncio
async def test_get_dimensions_invalid_dataset(metadata_service):
    """Test dimension query with invalid dataset."""
    with pytest.raises(ValidationError) as exc:
        await metadata_service.get_dimensions(
            dataset="invalid_dataset",
            dimension="iso"
        )
    
    assert "Invalid dataset" in str(exc.value)


@pytest.mark.asyncio
async def test_get_dimensions_sql_injection_attempt(metadata_service):
    """Test that SQL injection attempts are rejected."""
    with pytest.raises(ValidationError) as exc:
        await metadata_service.get_dimensions(
            dataset="curves",
            dimension="iso; DROP TABLE curves"
        )
    
    assert "Invalid dimension name" in str(exc.value)


@pytest.mark.asyncio
async def test_get_all_dimensions(metadata_service, mock_metadata_repo):
    """Test getting all dimensions for a dataset."""
    # Arrange
    mock_metadata_repo.get_all_dimensions.return_value = {
        "iso": ["PJM", "ERCOT"],
        "market": ["DA", "RT"],
        "location": ["HUB", "ZONE"]
    }
    
    # Act
    result = await metadata_service.get_all_dimensions("curves")
    
    # Assert
    assert result.success
    assert len(result.data) == 3
    assert result.metadata["dimension_count"] == 3
    assert result.metadata["total_values"] == 7  # 2+2+3


@pytest.mark.asyncio
async def test_search_metadata_success(metadata_service, mock_metadata_repo):
    """Test metadata search."""
    # Arrange
    mock_metadata_repo.search_metadata.return_value = [
        {"dataset": "curves", "name": "PJM DA prices"},
        {"dataset": "curves", "name": "PJM RT prices"}
    ]
    
    # Act
    result = await metadata_service.search_metadata(
        search_term="PJM",
        limit=100
    )
    
    # Assert
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["search_term"] == "PJM"


@pytest.mark.asyncio
async def test_search_metadata_too_short(metadata_service):
    """Test search with too short search term."""
    with pytest.raises(ValidationError) as exc:
        await metadata_service.search_metadata(search_term="a")
    
    assert "at least 2 characters" in str(exc.value)


@pytest.mark.asyncio
async def test_search_metadata_invalid_limit(metadata_service):
    """Test search with invalid limit."""
    with pytest.raises(ValidationError) as exc:
        await metadata_service.search_metadata(
            search_term="test",
            limit=2000  # Too high
        )
    
    assert "between 1 and 1000" in str(exc.value)


@pytest.mark.asyncio
async def test_get_dataset_info(metadata_service, mock_metadata_repo):
    """Test getting dataset information."""
    # Arrange
    mock_metadata_repo.get_all_dimensions.return_value = {
        "iso": ["PJM"],
        "market": ["DA"]
    }
    
    # Act
    result = await metadata_service.get_dataset_info("curves")
    
    # Assert
    assert result.success
    assert result.data["dataset"] == "curves"
    assert result.data["dimension_count"] == 2
    assert result.data["available"] is True

