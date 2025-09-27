"""Tests for Curves service layer implementation."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from aurum.api.services.curves_service import CurvesService
from aurum.api.dao.curves_dao import CurvesDao


@pytest.fixture
def mock_dao():
    """Mock Curves DAO for testing."""
    return MagicMock(spec=CurvesDao)


@pytest.fixture
def curves_service(mock_dao):
    """Curves service with mocked DAO."""
    service = CurvesService()
    service._dao = mock_dao
    return service


class TestCurvesService:
    """Test cases for Curves service."""
    
    @pytest.mark.asyncio
    async def test_list_curves_success(self, curves_service, mock_dao):
        """Test successful curves listing."""
        # Arrange
        mock_curve_data = [
            {
                "iso": "CAISO",
                "market": "DA",
                "location": "SP15",
                "product": "ENERGY",
                "asof": "2023-12-01",
                "forward_date": "2023-12-02",
                "forward_value": 45.5
            },
            {
                "iso": "PJM",
                "market": "DA",
                "location": "AEP_ZONE",
                "product": "ENERGY",
                "asof": "2023-12-01", 
                "forward_date": "2023-12-02",
                "forward_value": 38.2
            }
        ]
        mock_dao.query_curves = AsyncMock(return_value=mock_curve_data)
        
        # Act
        result = await curves_service.list_curves(offset=0, limit=10)
        
        # Assert
        mock_dao.query_curves.assert_called_once_with(
            iso=None,
            market=None,
            location=None,
            product=None,
            block=None,
            asof=None,
            strip=None,
            offset=0,
            limit=10
        )
        assert result == mock_curve_data
    
    @pytest.mark.asyncio
    async def test_list_curves_with_name_filter(self, curves_service, mock_dao):
        """Test curves listing with name filter."""
        # Arrange
        mock_curve_data = [
            {
                "iso": "CAISO",
                "market": "DA",
                "location": "SP15",
                "product": "ENERGY",
                "asof": "2023-12-01",
                "forward_date": "2023-12-02",
                "forward_value": 45.5
            }
        ]
        mock_dao.query_curves = AsyncMock(return_value=mock_curve_data)
        
        # Act
        result = await curves_service.list_curves(offset=0, limit=10, name_filter="CAISO")
        
        # Assert
        mock_dao.query_curves.assert_called_once_with(
            iso="CAISO",
            market=None,
            location=None,
            product=None,
            block=None,
            asof=None,
            strip=None,
            offset=0,
            limit=10
        )
        assert result == mock_curve_data

    @pytest.mark.asyncio
    async def test_get_curve_diff_success(self, curves_service, mock_dao):
        """Test successful curve diff retrieval."""
        # Arrange
        curve_id = "CAISO:DA:SP15:ENERGY"
        from_timestamp = "2023-12-01"
        to_timestamp = "2023-12-02"
        
        mock_diff_data = [
            {
                "iso": "CAISO",
                "market": "DA",
                "location": "SP15",
                "product": "ENERGY",
                "forward_date": "2023-12-02",
                "from_value": 45.5,
                "to_value": 47.2,
                "diff_value": 1.7,
                "pct_change": 3.73
            }
        ]
        mock_dao.query_curves_diff = AsyncMock(return_value=mock_diff_data)
        
        # Act
        result = await curves_service.get_curve_diff(
            curve_id=curve_id,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp
        )
        
        # Assert
        mock_dao.query_curves_diff.assert_called_once_with(
            iso="CAISO",
            from_asof=from_timestamp,
            to_asof=to_timestamp,
            limit=1000
        )
        assert result["diff_data"] == mock_diff_data
        assert result["from_asof"] == from_timestamp
        assert result["to_asof"] == to_timestamp

    @pytest.mark.asyncio
    async def test_query_data_with_filters(self, curves_service, mock_dao):
        """Test query_data with filtering."""
        # Arrange
        filters = {
            "iso": "PJM",
            "market": "RT",
            "location": "PSEG_ZONE",
            "asof": "2023-12-01"
        }
        mock_curve_data = [
            {
                "iso": "PJM",
                "market": "RT",
                "location": "PSEG_ZONE",
                "product": "ENERGY",
                "asof": "2023-12-01"
            }
        ]
        mock_dao.query_curves = AsyncMock(return_value=mock_curve_data)
        
        # Act
        result = await curves_service.query_data(offset=5, limit=20, filters=filters)
        
        # Assert
        mock_dao.query_curves.assert_called_once_with(
            iso="PJM",
            market="RT",
            location="PSEG_ZONE",
            product=None,
            block=None,
            asof="2023-12-01",
            strip=None,
            offset=5,
            limit=20
        )
        assert result == mock_curve_data

    @pytest.mark.asyncio
    async def test_export_data_json_format(self, curves_service, mock_dao):
        """Test data export in JSON format."""
        # Arrange
        mock_chunk1 = [{"iso": "CAISO", "value": 45.5}]
        mock_chunk2 = [{"iso": "PJM", "value": 38.2}]
        mock_chunk3 = []  # Empty chunk to end iteration
        
        mock_dao.query_curves = AsyncMock(side_effect=[mock_chunk1, mock_chunk2, mock_chunk3])
        
        # Act
        results = []
        async for item in curves_service.export_data(format="json", chunk_size=1):
            results.append(item)
        
        # Assert
        assert len(results) == 2
        assert results[0] == {"iso": "CAISO", "value": 45.5}
        assert results[1] == {"iso": "PJM", "value": 38.2}
        assert mock_dao.query_curves.call_count == 3  # Two data chunks + one empty

    @pytest.mark.asyncio
    async def test_export_data_other_format(self, curves_service, mock_dao):
        """Test data export in non-JSON format."""
        # Arrange
        mock_chunk = [{"iso": "CAISO", "value": 45.5}]
        mock_dao.query_curves = AsyncMock(side_effect=[mock_chunk, []])
        
        # Act
        results = []
        async for item in curves_service.export_data(format="csv", chunk_size=1):
            results.append(item)
        
        # Assert
        assert len(results) == 1
        assert results[0]["format"] == "csv"
        assert results[0]["data"] == mock_chunk
        assert results[0]["offset"] == 0

    @pytest.mark.asyncio
    async def test_stream_curve_export(self, curves_service, mock_dao):
        """Test legacy stream_curve_export method."""
        # Arrange
        mock_chunk = [{"iso": "CAISO", "value": 45.5}]
        mock_dao.query_curves = AsyncMock(side_effect=[mock_chunk, []])
        
        # Act
        results = []
        async for item in curves_service.stream_curve_export(
            asof="2023-12-01",
            iso="CAISO",
            chunk_size=1
        ):
            results.append(item)
        
        # Assert
        assert len(results) == 1
        assert results[0] == {"iso": "CAISO", "value": 45.5}

    @pytest.mark.asyncio
    async def test_invalidate_cache_success(self, curves_service, mock_dao):
        """Test successful cache invalidation."""
        # Arrange
        mock_invalidation_result = {"curves": 15}
        mock_dao.invalidate_curve_cache = AsyncMock(return_value=mock_invalidation_result)
        
        # Act
        result = await curves_service.invalidate_cache()
        
        # Assert
        mock_dao.invalidate_curve_cache.assert_called_once()
        assert result == mock_invalidation_result


class TestCurvesDao:
    """Test cases for Curves DAO."""
    
    def test_build_curve_query_basic(self):
        """Test basic curve query building."""
        # Arrange
        dao = CurvesDao()
        
        # Act
        query, params = dao._build_curve_query(
            iso="CAISO",
            market="DA",
            offset=0,
            limit=100
        )
        
        # Assert
        assert "SELECT" in query
        assert "FROM iceberg.market.curve_observation" in query
        assert "iso = %(iso)s" in query
        assert "market = %(market)s" in query
        assert "ORDER BY iso, market, location, product, asof, forward_date" in query
        assert "OFFSET %(offset)s LIMIT %(limit)s" in query
        
        assert params["iso"] == "CAISO"
        assert params["market"] == "DA"
        assert params["offset"] == 0
        assert params["limit"] == 100

    def test_build_curve_diff_query_basic(self):
        """Test basic curve diff query building."""
        # Arrange
        dao = CurvesDao()
        
        # Act
        query, params = dao._build_curve_diff_query(
            iso="PJM", 
            from_asof="2023-12-01",
            to_asof="2023-12-02",
            offset=0,
            limit=100
        )
        
        # Assert
        assert "WITH from_curves AS" in query
        assert "WITH to_curves AS" in query  # This should actually be comma-separated in CTE
        assert "FULL OUTER JOIN" in query
        assert "asof = %(from_asof)s" in query
        assert "asof = %(to_asof)s" in query
        assert "COALESCE(f.iso, t.iso) = %(iso)s" in query
        
        assert params["from_asof"] == "2023-12-01"
        assert params["to_asof"] == "2023-12-02"
        assert params["iso"] == "PJM"

    @pytest.mark.asyncio
    @patch('aurum.api.dao.curves_dao.get_trino_client')
    async def test_query_curves_integration(self, mock_get_client):
        """Test curves query integration with Trino client."""
        # Arrange
        mock_client = AsyncMock()
        mock_rows = [
            {
                "iso": "CAISO",
                "market": "DA",
                "location": "SP15",
                "forward_value": 45.5
            }
        ]
        mock_client.execute_query = AsyncMock(return_value=mock_rows)
        mock_get_client.return_value = mock_client
        
        dao = CurvesDao()
        
        # Act
        result = await dao.query_curves(iso="CAISO", market="DA")
        
        # Assert
        mock_client.execute_query.assert_called_once()
        assert len(result) == 1
        assert result[0]["iso"] == "CAISO"
        assert result[0]["market"] == "DA"

    @pytest.mark.asyncio
    @patch('aurum.api.dao.curves_dao.get_trino_client')
    async def test_query_curves_diff_integration(self, mock_get_client):
        """Test curves diff query integration."""
        # Arrange
        mock_client = AsyncMock()
        mock_rows = [
            {
                "iso": "CAISO",
                "from_value": 45.5,
                "to_value": 47.2,
                "diff_value": 1.7,
                "pct_change": 3.73
            }
        ]
        mock_client.execute_query = AsyncMock(return_value=mock_rows)
        mock_get_client.return_value = mock_client
        
        dao = CurvesDao()
        
        # Act
        result = await dao.query_curves_diff(
            iso="CAISO",
            from_asof="2023-12-01",
            to_asof="2023-12-02"
        )
        
        # Assert
        mock_client.execute_query.assert_called_once()
        assert len(result) == 1
        assert result[0]["iso"] == "CAISO"
        assert result[0]["diff_value"] == 1.7