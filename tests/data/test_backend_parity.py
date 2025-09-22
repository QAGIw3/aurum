"""Test parity of results across different data backends."""

from __future__ import annotations

import pytest
import pytest_asyncio
from unittest.mock import Mock, patch
from typing import Any, Dict, List

from aurum.core import AurumSettings
from aurum.data import (
    TrinoBackend, ClickHouseBackend, TimescaleBackend,
    ConnectionConfig, QueryResult
)
from aurum.data.backend_adapter import BackendAdapter


@pytest_asyncio.asyncio
class TestBackendParity:
    """Test that different backends produce consistent results."""

    @pytest.fixture
    def sample_query(self):
        """Sample query for testing."""
        return "SELECT * FROM test_table WHERE tenant_id = $1 LIMIT 10"

    @pytest.fixture
    def sample_params(self):
        """Sample parameters for testing."""
        return {"1": "test-tenant-123"}

    @pytest.fixture
    def sample_rows(self):
        """Sample query results."""
        return [
            ("test-id-1", "test-tenant-123", "test-value-1"),
            ("test-id-2", "test-tenant-123", "test-value-2"),
            ("test-id-3", "test-tenant-123", "test-value-3"),
        ]

    @pytest.fixture
    def sample_columns(self):
        """Sample column names."""
        return ["id", "tenant_id", "value"]

    @pytest_asyncio.fixture
    async def mock_trino_backend(self, sample_rows, sample_columns):
        """Mock Trino backend."""
        backend = Mock(spec=TrinoBackend)
        backend.name = "trino"
        backend.supports_cursor_pagination = True

        # Mock query execution
        async def mock_execute_query(query, params=None):
            return QueryResult(
                columns=sample_columns,
                rows=sample_rows,
                metadata={"backend": "trino", "query_id": "test-query-123"}
            )

        backend.execute_query = mock_execute_query
        backend.connect = Mock()
        backend.close = Mock()

        return backend

    @pytest_asyncio.fixture
    async def mock_clickhouse_backend(self, sample_rows, sample_columns):
        """Mock ClickHouse backend."""
        backend = Mock(spec=ClickHouseBackend)
        backend.name = "clickhouse"
        backend.supports_cursor_pagination = True

        # Mock query execution
        async def mock_execute_query(query, params=None):
            return QueryResult(
                columns=sample_columns,
                rows=sample_rows,
                metadata={"backend": "clickhouse", "rows_affected": len(sample_rows)}
            )

        backend.execute_query = mock_execute_query
        backend.connect = Mock()
        backend.close = Mock()

        return backend

    @pytest_asyncio.fixture
    async def mock_timescale_backend(self, sample_rows, sample_columns):
        """Mock Timescale backend."""
        backend = Mock(spec=TimescaleBackend)
        backend.name = "timescale"
        backend.supports_cursor_pagination = True

        # Mock query execution
        async def mock_execute_query(query, params=None):
            return QueryResult(
                columns=sample_columns,
                rows=sample_rows,
                metadata={"backend": "timescale", "rows_affected": len(sample_rows)}
            )

        backend.execute_query = mock_execute_query
        backend.connect = Mock()
        backend.close = Mock()

        return backend

    async def test_backend_adapter_creation(self, sample_query, sample_params):
        """Test that BackendAdapter can be created with different backends."""
        settings = AurumSettings.from_env()

        # Test Trino backend
        settings.data_backend.backend_type = "trino"
        adapter = BackendAdapter(settings)
        assert adapter.backend_type == "trino"

        # Test ClickHouse backend
        settings.data_backend.backend_type = "clickhouse"
        adapter = BackendAdapter(settings)
        assert adapter.backend_type == "clickhouse"

        # Test Timescale backend
        settings.data_backend.backend_type = "timescale"
        adapter = BackendAdapter(settings)
        assert adapter.backend_type == "timescale"

    async def test_backend_result_consistency(
        self,
        mock_trino_backend,
        mock_clickhouse_backend,
        mock_timescale_backend,
        sample_query,
        sample_params,
        sample_rows,
        sample_columns
    ):
        """Test that all backends return consistent results."""
        # Test that all backends return the same data structure
        trino_result = await mock_trino_backend.execute_query(sample_query, sample_params)
        clickhouse_result = await mock_clickhouse_backend.execute_query(sample_query, sample_params)
        timescale_result = await mock_timescale_backend.execute_query(sample_query, sample_params)

        # All results should have the same structure
        assert trino_result.columns == sample_columns
        assert clickhouse_result.columns == sample_columns
        assert timescale_result.columns == sample_columns

        assert trino_result.rows == sample_rows
        assert clickhouse_result.rows == sample_rows
        assert timescale_result.rows == sample_rows

        # Each should have backend-specific metadata
        assert trino_result.metadata["backend"] == "trino"
        assert clickhouse_result.metadata["backend"] == "clickhouse"
        assert timescale_result.metadata["backend"] == "timescale"

    async def test_backend_adapter_query_execution(
        self,
        mock_trino_backend,
        sample_query,
        sample_params,
        sample_rows,
        sample_columns
    ):
        """Test that BackendAdapter correctly executes queries and formats results."""
        with patch('aurum.data.backend_adapter.get_backend') as mock_get_backend:
            mock_get_backend.return_value = mock_trino_backend

            settings = AurumSettings.from_env()
            adapter = BackendAdapter(settings)

            # Execute query
            result = await adapter.execute_query(sample_query, sample_params)

            # Verify result format
            assert "columns" in result
            assert "rows" in result
            assert "metadata" in result

            assert result["columns"] == sample_columns
            assert result["rows"] == sample_rows
            assert result["metadata"]["backend"] == "trino"

    async def test_backend_error_handling(self, sample_query):
        """Test that backends handle errors gracefully."""
        settings = AurumSettings.from_env()
        adapter = BackendAdapter(settings)

        # Test unsupported backend
        with pytest.raises(ValueError, match="Unsupported backend type"):
            await adapter._create_connection_config()

        # Test missing dependencies
        settings.data_backend.backend_type = "trino"

        with patch('aurum.data.backend_adapter.get_backend') as mock_get_backend:
            mock_get_backend.side_effect = RuntimeError("Trino dependencies not installed")

            with pytest.raises(RuntimeError, match="Trino dependencies not installed"):
                await adapter.get_backend()

    async def test_backend_context_manager(self):
        """Test the BackendContext context manager."""
        from aurum.data.backend_adapter import BackendContext

        settings = AurumSettings.from_env()
        settings.data_backend.backend_type = "trino"

        async with BackendContext(settings) as adapter:
            assert adapter is not None
            assert adapter.backend_type == "trino"

        # Backend should be closed after context exit
        assert adapter._backend is None

    async def test_backend_configuration_parsing(self):
        """Test that backend configuration is parsed correctly."""
        settings = AurumSettings.from_env()

        # Test default configuration
        assert settings.data_backend.backend_type.value == "trino"
        assert settings.data_backend.trino_host == "localhost"
        assert settings.data_backend.trino_port == 8080

        # Test environment variable override
        with patch.dict('os.environ', {'AURUM_API_BACKEND': 'clickhouse'}):
            new_settings = AurumSettings.from_env()
            assert new_settings.data_backend.backend_type.value == "clickhouse"

    async def test_backend_factory_functions(self):
        """Test backend factory functions."""
        from aurum.data.backend_adapter import get_backend_info, validate_backend_support

        # Test backend info
        info = get_backend_info()
        assert "current" in info
        assert "available" in info
        assert "trino" in info["available"]
        assert "clickhouse" in info["available"]
        assert "timescale" in info["available"]

        # Test backend validation
        assert validate_backend_support("trino") is True
        assert validate_backend_support("clickhouse") is True
        assert validate_backend_support("timescale") is True
        assert validate_backend_support("unsupported") is False

    async def test_backend_connection_config_creation(self):
        """Test that connection configurations are created correctly for each backend."""
        settings = AurumSettings.from_env()

        # Test Trino config
        settings.data_backend.backend_type = "trino"
        settings.data_backend.trino_host = "trino.example.com"
        settings.data_backend.trino_port = 8080
        settings.data_backend.trino_catalog = "iceberg"
        settings.data_backend.trino_schema = "market"

        adapter = BackendAdapter(settings)
        config = adapter._create_connection_config()

        assert config.host == "trino.example.com"
        assert config.port == 8080
        assert config.database == "iceberg.market"
        assert config.username == "aurum"

        # Test ClickHouse config
        settings.data_backend.backend_type = "clickhouse"
        settings.data_backend.clickhouse_host = "clickhouse.example.com"
        settings.data_backend.clickhouse_port = 9000
        settings.data_backend.clickhouse_database = "aurum"

        adapter = BackendAdapter(settings)
        config = adapter._create_connection_config()

        assert config.host == "clickhouse.example.com"
        assert config.port == 9000
        assert config.database == "aurum"
        assert config.username == "aurum"

        # Test Timescale config
        settings.data_backend.backend_type = "timescale"
        settings.data_backend.timescale_host = "timescale.example.com"
        settings.data_backend.timescale_port = 5432
        settings.data_backend.timescale_database = "aurum"

        adapter = BackendAdapter(settings)
        config = adapter._create_connection_config()

        assert config.host == "timescale.example.com"
        assert config.port == 5432
        assert config.database == "aurum"
        assert config.username == "aurum"

    async def test_backend_cursor_pagination_support(self):
        """Test that all backends support cursor pagination."""
        backends = [TrinoBackend, ClickHouseBackend, TimescaleBackend]

        for backend_class in backends:
            # Create mock config
            config = ConnectionConfig(
                host="localhost",
                port=8080,
                database="test",
                username="test",
                password="test"
            )

            backend = backend_class(config)
            assert backend.supports_cursor_pagination is True

    async def test_backend_adapter_error_conversion(
        self,
        mock_trino_backend,
        sample_query,
        sample_params
    ):
        """Test that BackendAdapter correctly converts QueryResult to service format."""
        with patch('aurum.data.backend_adapter.get_backend') as mock_get_backend:
            mock_get_backend.return_value = mock_trino_backend

            settings = AurumSettings.from_env()
            adapter = BackendAdapter(settings)

            # Execute query
            result = await adapter.execute_query(sample_query, sample_params)

            # Verify format conversion
            assert isinstance(result, dict)
            assert "columns" in result
            assert "rows" in result
            assert "metadata" in result

            # Verify metadata includes backend info
            assert result["metadata"]["backend"] == "trino"

    async def test_backend_pool_config_creation(self):
        """Test that pool configs are created correctly."""
        settings = AurumSettings.from_env()
        adapter = BackendAdapter(settings)

        pool_config = adapter._create_pool_config()
        assert pool_config.min_size == 5
        assert pool_config.max_size == 20
        assert pool_config.max_idle_time == 300
        assert pool_config.connection_timeout == 30.0
        assert pool_config.acquire_timeout == 10.0

    async def test_backend_pool_metrics_exposure(self):
        """Test that pool metrics are exposed from backends."""
        settings = AurumSettings.from_env()
        adapter = BackendAdapter(settings)

        # Get backend to initialize it
        backend = await adapter.get_backend()
        assert backend is not None

        # Check that pool metrics are available
        metrics = backend.pool_metrics
        assert metrics is not None
        assert hasattr(metrics, 'connections_created')
        assert hasattr(metrics, 'connections_destroyed')
        assert hasattr(metrics, 'connections_acquired')
        assert hasattr(metrics, 'connections_released')

    async def test_backend_adapter_pool_config_creation(self):
        """Test that BackendAdapter creates proper pool configurations."""
        settings = AurumSettings.from_env()
        adapter = BackendAdapter(settings)

        pool_config = adapter._create_pool_config()
        assert pool_config.min_size == 5
        assert pool_config.max_size == 20
        assert pool_config.max_idle_time == 300
        assert pool_config.connection_timeout == 30.0
        assert pool_config.acquire_timeout == 10.0
