"""Tests for EIA service layer implementation."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from aurum.api.services.eia_service import EiaService
from aurum.api.dao.eia_dao import EiaDao


@pytest.fixture
def mock_dao():
    """Mock EIA DAO for testing."""
    return MagicMock(spec=EiaDao)


@pytest.fixture
def eia_service(mock_dao):
    """EIA service with mocked DAO."""
    service = EiaService()
    service._dao = mock_dao
    return service


class TestEiaService:
    """Test cases for EIA service."""
    
    @pytest.mark.asyncio
    async def test_list_datasets_success(self, eia_service, mock_dao):
        """Test successful dataset listing."""
        # Arrange
        mock_series_data = [
            {"dataset": "ELEC", "series_id": "ELEC.GEN.ALL-US-99.A"},
            {"dataset": "NG", "series_id": "NG.RNGC1.A"},
            {"dataset": "ELEC", "series_id": "ELEC.GEN.COW-US-99.A"},  # Duplicate dataset
        ]
        mock_dao.query_eia_series = AsyncMock(return_value=mock_series_data)
        
        # Act
        datasets, count = await eia_service.list_datasets(offset=0, limit=10)
        
        # Assert
        mock_dao.query_eia_series.assert_called_once_with(offset=0, limit=10)
        assert count == 2  # Only unique datasets
        assert len(datasets) == 2
        
        dataset_names = {ds["dataset"] for ds in datasets}
        assert "ELEC" in dataset_names
        assert "NG" in dataset_names
    
    @pytest.mark.asyncio
    async def test_get_series_success(self, eia_service, mock_dao):
        """Test successful series data retrieval."""
        # Arrange
        series_id = "ELEC.GEN.ALL-US-99.A"
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        
        mock_series_data = [
            {
                "series_id": series_id,
                "timestamp_utc": "2023-01-01T00:00:00Z",
                "value": 100.5,
                "frequency": "A",
                "dataset": "ELEC"
            }
        ]
        mock_dao.query_eia_series = AsyncMock(return_value=mock_series_data)
        
        # Act
        result = await eia_service.get_series(
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            offset=0,
            limit=100
        )
        
        # Assert
        mock_dao.query_eia_series.assert_called_once_with(
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            offset=0,
            limit=100
        )
        assert result == mock_series_data
    
    @pytest.mark.asyncio
    async def test_get_series_dimensions_success(self, eia_service, mock_dao):
        """Test successful dimensions retrieval."""
        # Arrange
        mock_dimensions = {
            "frequency": ["A", "M", "W", "D"],
            "area": ["US", "CA", "TX"],
            "sector": ["electric", "natural_gas"],
            "dataset": ["ELEC", "NG"],
            "unit": ["thousand kilowatthours", "million cubic feet"],
            "canonical_unit": ["kWh", "mcf"],
            "canonical_currency": ["USD"],
            "source": ["EIA"]
        }
        mock_dao.query_eia_series_dimensions = AsyncMock(return_value=mock_dimensions)
        
        # Act
        result = await eia_service.get_series_dimensions(
            frequency="A",
            area="US"
        )
        
        # Assert
        mock_dao.query_eia_series_dimensions.assert_called_once_with(
            series_id=None,
            frequency="A",
            area="US",
            sector=None,
            dataset=None,
            unit=None,
            canonical_unit=None,
            canonical_currency=None,
            source=None
        )
        assert result == mock_dimensions
    
    @pytest.mark.asyncio
    async def test_invalidate_cache_success(self, eia_service, mock_dao):
        """Test successful cache invalidation."""
        # Arrange
        mock_invalidation_result = {"eia_series": 42}
        mock_dao.invalidate_eia_series_cache = AsyncMock(return_value=mock_invalidation_result)
        
        # Act
        result = await eia_service.invalidate_cache()
        
        # Assert
        mock_dao.invalidate_eia_series_cache.assert_called_once()
        assert result == mock_invalidation_result
    
    @pytest.mark.asyncio
    async def test_list_datasets_empty_result(self, eia_service, mock_dao):
        """Test dataset listing with empty result."""
        # Arrange
        mock_dao.query_eia_series = AsyncMock(return_value=[])
        
        # Act
        datasets, count = await eia_service.list_datasets(offset=0, limit=10)
        
        # Assert
        assert datasets == []
        assert count == 0


class TestEiaDao:
    """Test cases for EIA DAO."""
    
    def test_build_sql_eia_series_basic(self):
        """Test basic SQL building for EIA series."""
        # Arrange
        dao = EiaDao()
        
        # Act
        query, params = dao._build_sql_eia_series(
            series_id="ELEC.GEN.ALL-US-99.A",
            offset=0,
            limit=100
        )
        
        # Assert
        assert "SELECT" in query
        assert "FROM timescale.eia.series" in query
        assert "series_id = %(series_id)s" in query
        assert "ORDER BY series_id, timestamp_utc" in query
        assert "OFFSET %(offset)s LIMIT %(limit)s" in query
        
        assert params["series_id"] == "ELEC.GEN.ALL-US-99.A"
        assert params["offset"] == 0
        assert params["limit"] == 100
    
    def test_build_sql_eia_series_with_filters(self):
        """Test SQL building with multiple filters."""
        # Arrange
        dao = EiaDao()
        
        # Act
        query, params = dao._build_sql_eia_series(
            frequency="A",
            area="US",
            dataset="ELEC",
            start_date="2023-01-01",
            end_date="2023-12-31",
            offset=10,
            limit=50
        )
        
        # Assert
        assert "frequency = %(frequency)s" in query
        assert "area = %(area)s" in query
        assert "dataset = %(dataset)s" in query
        assert "timestamp_utc >= %(start_date)s::timestamp" in query
        assert "timestamp_utc <= %(end_date)s::timestamp" in query
        
        assert params["frequency"] == "A"
        assert params["area"] == "US"
        assert params["dataset"] == "ELEC"
        assert params["start_date"] == "2023-01-01"
        assert params["end_date"] == "2023-12-31"
        assert params["offset"] == 10
        assert params["limit"] == 50
    
    def test_build_sql_eia_series_dimensions_basic(self):
        """Test basic SQL building for EIA series dimensions."""
        # Arrange
        dao = EiaDao()
        
        # Act
        query, params = dao._build_sql_eia_series_dimensions()
        
        # Assert
        assert "SELECT DISTINCT" in query
        assert "frequency" in query
        assert "area" in query
        assert "dataset" in query
        assert "FROM timescale.eia.series" in query
        assert "ORDER BY frequency, area, sector, dataset, unit" in query
        assert params == {}
    
    def test_build_sql_eia_series_dimensions_with_filters(self):
        """Test dimensions SQL building with filters."""
        # Arrange
        dao = EiaDao()
        
        # Act
        query, params = dao._build_sql_eia_series_dimensions(
            frequency="A",
            area="US"
        )
        
        # Assert
        assert "frequency = %(frequency)s" in query
        assert "area = %(area)s" in query
        assert params["frequency"] == "A"
        assert params["area"] == "US"
    
    @pytest.mark.asyncio
    @patch('aurum.api.dao.eia_dao.get_trino_client')
    async def test_query_eia_series_integration(self, mock_get_client):
        """Test EIA series query integration with Trino client."""
        # Arrange
        mock_client = AsyncMock()
        mock_rows = [
            {
                "series_id": "ELEC.GEN.ALL-US-99.A",
                "timestamp_utc": "2023-01-01T00:00:00Z",
                "value": 100.5
            }
        ]
        mock_client.execute_query = AsyncMock(return_value=mock_rows)
        mock_get_client.return_value = mock_client
        
        dao = EiaDao()
        
        # Act
        result = await dao.query_eia_series(series_id="ELEC.GEN.ALL-US-99.A")
        
        # Assert
        mock_client.execute_query.assert_called_once()
        assert len(result) == 1
        assert result[0]["series_id"] == "ELEC.GEN.ALL-US-99.A"
    
    @pytest.mark.asyncio
    @patch('aurum.api.dao.eia_dao.get_trino_client')
    async def test_query_eia_series_dimensions_integration(self, mock_get_client):
        """Test EIA series dimensions query integration."""
        # Arrange
        mock_client = AsyncMock()
        mock_rows = [
            {"frequency": "A", "area": "US", "sector": "electric", "dataset": "ELEC", 
             "unit": "kWh", "canonical_unit": "kWh", "canonical_currency": "USD", "source": "EIA"},
            {"frequency": "M", "area": "CA", "sector": "natural_gas", "dataset": "NG", 
             "unit": "mcf", "canonical_unit": "mcf", "canonical_currency": "USD", "source": "EIA"}
        ]
        mock_client.execute_query = AsyncMock(return_value=mock_rows)
        mock_get_client.return_value = mock_client
        
        dao = EiaDao()
        
        # Act
        result = await dao.query_eia_series_dimensions()
        
        # Assert
        mock_client.execute_query.assert_called_once()
        assert "frequency" in result
        assert "area" in result
        assert sorted(result["frequency"]) == ["A", "M"]
        assert sorted(result["area"]) == ["CA", "US"]