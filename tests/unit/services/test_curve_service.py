"""Unit tests for CurveService.

Tests business logic with mocked repositories.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import date

from aurum.services.core import CurveService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError


@pytest.fixture
def mock_curve_repo():
    """Mock curve repository."""
    repo = AsyncMock()
    return repo


@pytest.fixture
def curve_service(mock_curve_repo):
    """Create service with mocked repository."""
    return CurveService(mock_curve_repo)


@pytest.mark.asyncio
async def test_get_curves_success(curve_service, mock_curve_repo):
    """Test successful curve query."""
    # Arrange
    mock_curve_repo.find_by_filters.return_value = [
        {"curve_key": "PJM_DA_TEST", "value": 100.0},
        {"curve_key": "PJM_DA_TEST", "value": 105.0},
    ]
    
    # Act
    result = await curve_service.get_curves(
        iso="PJM",
        market="DA",
        limit=100
    )
    
    # Assert
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["count"] == 2
    assert result.metadata["has_more"] is False
    
    # Verify repository was called correctly
    mock_curve_repo.find_by_filters.assert_called_once_with(
        iso="PJM",
        market="DA",
        location=None,
        product=None,
        asof=None,
        limit=100,
        offset=0
    )


@pytest.mark.asyncio
async def test_get_curves_validates_limit(curve_service):
    """Test that invalid limits are rejected."""
    # Too small
    with pytest.raises(ValidationError) as exc:
        await curve_service.get_curves(limit=0)
    assert "at least 1" in str(exc.value)
    
    # Too large
    with pytest.raises(ValidationError) as exc:
        await curve_service.get_curves(limit=20000)
    assert "cannot exceed 10000" in str(exc.value)


@pytest.mark.asyncio
async def test_get_curves_requires_filter(curve_service):
    """Test that at least one filter is required."""
    with pytest.raises(ValidationError) as exc:
        await curve_service.get_curves()
    
    assert "at least one filter" in str(exc.value)


@pytest.mark.asyncio
async def test_get_curves_enforces_max_limit(curve_service, mock_curve_repo):
    """Test that limit is capped at 1000."""
    mock_curve_repo.find_by_filters.return_value = []
    
    await curve_service.get_curves(iso="PJM", limit=5000)
    
    # Should call with max 1000
    call_args = mock_curve_repo.find_by_filters.call_args
    assert call_args.kwargs["limit"] == 1000


@pytest.mark.asyncio
async def test_get_curves_with_context(curve_service, mock_curve_repo):
    """Test curve query with service context."""
    mock_curve_repo.find_by_filters.return_value = [
        {"curve_key": "test", "value": 100}
    ]
    
    context = ServiceContext(
        tenant_id="tenant-123",
        user_id="user-456",
        request_id="req-789"
    )
    
    result = await curve_service.get_curves(
        iso="PJM",
        context=context
    )
    
    assert result.success
    # Context should be logged but not affect results in this implementation


@pytest.mark.asyncio
async def test_get_curve_by_key_success(curve_service, mock_curve_repo):
    """Test getting curve by key."""
    mock_curve_repo.find_by_key.return_value = [
        {"curve_key": "TEST_CURVE", "interval_start": "2024-01-01", "value": 100}
    ]
    
    result = await curve_service.get_curve_by_key("TEST_CURVE")
    
    assert result.success
    assert len(result.data) == 1
    assert result.metadata["curve_key"] == "TEST_CURVE"
    
    mock_curve_repo.find_by_key.assert_called_once_with(
        curve_key="TEST_CURVE",
        asof=None,
        limit=1000
    )


@pytest.mark.asyncio
async def test_get_curve_by_key_not_found(curve_service, mock_curve_repo):
    """Test curve not found error."""
    mock_curve_repo.find_by_key.return_value = []
    
    with pytest.raises(NotFoundError) as exc:
        await curve_service.get_curve_by_key("NONEXISTENT")
    
    assert exc.value.resource == "curve"
    assert exc.value.identifier == "NONEXISTENT"


@pytest.mark.asyncio
async def test_get_latest_asof(curve_service, mock_curve_repo):
    """Test getting latest as-of date."""
    latest_date = date(2024, 1, 15)
    mock_curve_repo.get_latest_asof.return_value = latest_date
    
    result = await curve_service.get_latest_asof(iso="PJM")
    
    assert result.success
    assert result.data == latest_date
    assert result.metadata["has_data"] is True
    
    mock_curve_repo.get_latest_asof.assert_called_once_with(iso="PJM")


@pytest.mark.asyncio
async def test_get_latest_asof_no_data(curve_service, mock_curve_repo):
    """Test latest as-of when no data exists."""
    mock_curve_repo.get_latest_asof.return_value = None
    
    result = await curve_service.get_latest_asof()
    
    assert result.success
    assert result.data is None
    assert result.metadata["has_data"] is False


@pytest.mark.asyncio
async def test_compare_curves_success(curve_service, mock_curve_repo):
    """Test curve comparison."""
    # Mock both curves
    mock_curve_repo.find_by_key.side_effect = [
        [{"curve_key": "CURVE1", "value": 100}],  # First call
        [{"curve_key": "CURVE2", "value": 105}],  # Second call
    ]
    
    result = await curve_service.compare_curves(
        curve_key_1="CURVE1",
        curve_key_2="CURVE2"
    )
    
    assert result.success
    assert "curve1_count" in result.data
    assert "curve2_count" in result.data
    assert result.metadata["curve_key_1"] == "CURVE1"
    assert result.metadata["curve_key_2"] == "CURVE2"


@pytest.mark.asyncio
async def test_compare_curves_first_not_found(curve_service, mock_curve_repo):
    """Test comparison when first curve not found."""
    mock_curve_repo.find_by_key.return_value = []
    
    with pytest.raises(NotFoundError) as exc:
        await curve_service.compare_curves(
            curve_key_1="NONEXISTENT",
            curve_key_2="CURVE2"
        )
    
    assert exc.value.identifier == "NONEXISTENT"


@pytest.mark.asyncio
async def test_compare_curves_second_not_found(curve_service, mock_curve_repo):
    """Test comparison when second curve not found."""
    mock_curve_repo.find_by_key.side_effect = [
        [{"curve_key": "CURVE1"}],  # First exists
        [],  # Second doesn't exist
    ]
    
    with pytest.raises(NotFoundError) as exc:
        await curve_service.compare_curves(
            curve_key_1="CURVE1",
            curve_key_2="NONEXISTENT"
        )
    
    assert exc.value.identifier == "NONEXISTENT"


@pytest.mark.asyncio
async def test_error_handling(curve_service, mock_curve_repo):
    """Test that repository errors are handled."""
    mock_curve_repo.find_by_filters.side_effect = Exception("Database error")
    
    with pytest.raises(Exception):  # Should raise ServiceError in real implementation
        await curve_service.get_curves(iso="PJM")


def test_service_creation():
    """Test service can be created with dependencies."""
    repo = AsyncMock()
    service = CurveService(repo)
    
    assert service.curve_repo is repo
    assert service.logger is not None

