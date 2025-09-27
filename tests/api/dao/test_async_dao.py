"""Tests for async DAO implementations with connection pooling."""

import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import datetime, timedelta

from aurum.api.dao import (
    BaseAsyncDao,
    TrinoAsyncDao,
    ClickHouseAsyncDao,
    TimescaleAsyncDao,
    EiaAsyncDao
)
from aurum.data import QueryResult, ConnectionConfig, PoolConfig
from aurum.core import AurumSettings


@pytest.fixture
def mock_settings():
    """Mock settings for testing."""
    settings = MagicMock()
    settings.data_backend.backend_type.value = "trino"
    settings.data_backend.connection_pool_min_size = 5
    settings.data_backend.connection_pool_max_size = 20
    settings.data_backend.connection_pool_max_idle_time = 300
    settings.data_backend.connection_pool_timeout_seconds = 10.0
    settings.data_backend.connection_pool_acquire_timeout_seconds = 30.0
    return settings


@pytest.fixture
def mock_backend():
    """Mock backend for testing."""
    backend = AsyncMock()
    backend.name = "trino"
    backend.execute_query = AsyncMock()
    return backend


@pytest.fixture  
def mock_backend_adapter():
    """Mock backend adapter for testing."""
    adapter = AsyncMock()
    adapter.get_backend = AsyncMock()
    adapter.close = AsyncMock()
    return adapter


class TestBaseAsyncDao:
    """Test BaseAsyncDao abstract base class."""
    
    @pytest.mark.asyncio
    async def test_execute_query_success(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test successful query execution."""
        # Mock the query result
        query_result = QueryResult(
            columns=["id", "name", "value"],
            rows=[(1, "test", 100.0), (2, "test2", 200.0)],
            metadata={"backend": "trino"}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        # Create a concrete implementation for testing
        class TestDao(BaseAsyncDao):
            @property
            def dao_name(self) -> str:
                return "test"
        
        dao = TestDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.execute_query("SELECT * FROM test", {"param": "value"})
        
        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "test", "value": 100.0}
        assert result[1] == {"id": 2, "name": "test2", "value": 200.0}
        
        mock_backend.execute_query.assert_called_once_with("SELECT * FROM test", {"param": "value"})
    
    @pytest.mark.asyncio
    async def test_execute_query_empty_result(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test query execution with empty result."""
        query_result = QueryResult(columns=[], rows=[], metadata={})
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        class TestDao(BaseAsyncDao):
            @property
            def dao_name(self) -> str:
                return "test"
        
        dao = TestDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.execute_query("SELECT * FROM empty_table")
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_execute_count_query(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test count query execution."""
        query_result = QueryResult(
            columns=["total"],
            rows=[(42,)],
            metadata={}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        class TestDao(BaseAsyncDao):
            @property
            def dao_name(self) -> str:
                return "test"
        
        dao = TestDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        count = await dao.execute_count_query("SELECT * FROM test WHERE id > 0")
        
        assert count == 42
        
        # Verify the count query was properly constructed
        expected_query = "SELECT COUNT(*) as total FROM (SELECT * FROM test WHERE id > 0) AS count_subquery"
        mock_backend.execute_query.assert_called_once_with(expected_query, None)


    @pytest.mark.asyncio
    async def test_context_manager(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test async context manager functionality."""
        query_result = QueryResult(
            columns=["id", "name"],
            rows=[(1, "test")],
            metadata={}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        class TestDao(BaseAsyncDao):
            @property
            def dao_name(self) -> str:
                return "test"
        
        # Test context manager
        async with TestDao(mock_settings) as dao:
            dao._backend_adapter = mock_backend_adapter
            result = await dao.execute_query("SELECT * FROM test")
            assert len(result) == 1
            assert result[0]["name"] == "test"
        
        # Verify close was called
        mock_backend_adapter.close.assert_called_once()


class TestTrinoAsyncDao:
    """Test TrinoAsyncDao implementation."""
    
    @pytest.mark.asyncio
    async def test_execute_federated_query(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test federated query execution."""
        query_result = QueryResult(
            columns=["catalog", "schema", "table"],
            rows=[("iceberg", "mart", "energy_data")],
            metadata={"backend": "trino"}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        dao = TrinoAsyncDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.execute_federated_query(
            "SELECT * FROM iceberg.mart.energy_data",
            catalogs=["iceberg"],
            schemas=["mart"]
        )
        
        assert len(result) == 1
        assert result[0]["catalog"] == "iceberg"
        assert result[0]["schema"] == "mart"
        assert result[0]["table"] == "energy_data"
    
    @pytest.mark.asyncio
    async def test_query_iceberg_tables(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test querying Iceberg tables metadata."""
        query_result = QueryResult(
            columns=["table_catalog", "table_schema", "table_name", "table_type"],
            rows=[("iceberg", "mart", "energy_data", "BASE TABLE")],
            metadata={}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        dao = TrinoAsyncDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.query_iceberg_tables(
            catalog="iceberg",
            schema="mart",
            table_pattern="energy%"
        )
        
        assert len(result) == 1
        assert result[0]["table_name"] == "energy_data"


class TestClickHouseAsyncDao:
    """Test ClickHouseAsyncDao implementation."""
    
    @pytest.mark.asyncio
    async def test_execute_analytical_query(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test analytical query execution with optimization."""
        mock_backend.name = "clickhouse"
        query_result = QueryResult(
            columns=["metric", "value"],
            rows=[("avg_price", 45.67)],
            metadata={"backend": "clickhouse"}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        dao = ClickHouseAsyncDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.execute_analytical_query(
            "SELECT avg(price) as avg_price FROM energy_data",
            optimize_for="speed"
        )
        
        assert len(result) == 1
        assert result[0]["metric"] == "avg_price"
        assert result[0]["value"] == 45.67
        
        # Verify optimization settings were added
        called_query = mock_backend.execute_query.call_args[0][0]
        assert "SETTINGS" in called_query
        assert "max_threads = 0" in called_query
    
    @pytest.mark.asyncio
    async def test_query_time_series_data(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test time series data querying."""
        mock_backend.name = "clickhouse"
        start_time = datetime(2024, 1, 1, 0, 0, 0)
        end_time = datetime(2024, 1, 2, 0, 0, 0)
        
        query_result = QueryResult(
            columns=["time_bucket", "value_avg"],
            rows=[(start_time, 100.0), (start_time + timedelta(hours=1), 105.0)],
            metadata={}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        dao = ClickHouseAsyncDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.query_time_series_data(
            table="energy_prices",
            start_time=start_time,
            end_time=end_time,
            group_by_interval="1 HOUR"
        )
        
        assert len(result) == 2
        assert result[0]["value_avg"] == 100.0
        assert result[1]["value_avg"] == 105.0


class TestTimescaleAsyncDao:
    """Test TimescaleAsyncDao implementation."""
    
    @pytest.mark.asyncio
    async def test_query_time_bucket_data(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test time bucket data querying."""
        mock_backend.name = "timescale"
        start_time = datetime(2024, 1, 1, 0, 0, 0)
        
        query_result = QueryResult(
            columns=["time_bucket", "value_avg"],
            rows=[(start_time, 50.0), (start_time + timedelta(hours=1), 55.0)],
            metadata={}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        dao = TimescaleAsyncDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.query_time_bucket_data(
            table="sensor_data",
            bucket_width="1 hour",
            start_time=start_time,
            aggregation="avg"
        )
        
        assert len(result) == 2
        assert result[0]["value_avg"] == 50.0
        
        # Verify time_bucket function was used
        called_query = mock_backend.execute_query.call_args[0][0]
        assert "time_bucket(" in called_query
    
    @pytest.mark.asyncio
    async def test_get_hypertable_info(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test getting hypertable information."""
        mock_backend.name = "timescale"
        query_result = QueryResult(
            columns=["hypertable_name", "num_chunks", "compression_enabled"],
            rows=[("sensor_data", 24, True)],
            metadata={}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        dao = TimescaleAsyncDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.get_hypertable_info("sensor_data")
        
        assert result["hypertable_name"] == "sensor_data"
        assert result["num_chunks"] == 24
        assert result["compression_enabled"] is True


class TestEiaAsyncDao:
    """Test EiaAsyncDao implementation."""
    
    @pytest.mark.asyncio
    async def test_query_eia_series(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test EIA series data querying."""
        query_result = QueryResult(
            columns=["series_id", "timestamp_utc", "value", "unit"],
            rows=[("EIA.SERIES.1", datetime(2024, 1, 1), 100.0, "USD")],
            metadata={}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        dao = EiaAsyncDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.query_eia_series(
            series_id="EIA.SERIES.1",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert len(result) == 1
        assert result[0]["series_id"] == "EIA.SERIES.1"
        assert result[0]["value"] == 100.0
        assert result[0]["unit"] == "USD"
    
    @pytest.mark.asyncio
    async def test_query_eia_series_dimensions(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test EIA series dimensions querying."""
        query_result = QueryResult(
            columns=["frequency", "area", "sector"],
            rows=[("monthly", "US", "electricity"), ("daily", "TX", "oil")],
            metadata={}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter.get_backend.return_value = mock_backend
        
        dao = EiaAsyncDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        result = await dao.query_eia_series_dimensions(dataset="electricity")
        
        assert "frequency" in result
        assert "monthly" in result["frequency"]
        assert "daily" in result["frequency"]
        assert "area" in result
        assert "US" in result["area"]
        assert "TX" in result["area"]
    
    @pytest.mark.asyncio
    async def test_get_eia_series_count(self, mock_settings, mock_backend, mock_backend_adapter):
        """Test getting EIA series count."""
        query_result = QueryResult(
            columns=["total"],
            rows=[(150,)],
            metadata={}
        )
        mock_backend.execute_query.return_value = query_result
        mock_backend_adapter = mock_backend_adapter
        
        dao = EiaAsyncDao(mock_settings)
        dao._backend_adapter = mock_backend_adapter
        
        count = await dao.get_eia_series_count(dataset="electricity")
        
        assert count == 150