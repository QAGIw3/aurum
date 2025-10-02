"""Integration tests for the unified database connection management system."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from aurum.database import (
    ConnectionConfig,
    ConnectionManagerRegistry,
    DatabasePoolFactory,
    PoolConfig,
    PoolMetrics,
    TrinoPoolManager,
    TimescalePoolManager,
    get_connection_manager_registry,
)
from aurum.core import AurumSettings


class TestDatabaseConnectionManagement:
    """Test the unified database connection management system."""

    @pytest.fixture
    def mock_settings(self):
        """Mock AurumSettings for testing."""
        settings = MagicMock(spec=AurumSettings)
        settings.data_backend = MagicMock()

        # Configure mock backend settings
        settings.data_backend.trino_host = "localhost"
        settings.data_backend.trino_port = 8080
        settings.data_backend.trino_catalog = "test"
        settings.data_backend.trino_user = "test_user"
        settings.data_backend.trino_password = "test_password"
        settings.data_backend.trino_use_ssl = False

        settings.data_backend.timescale_host = "localhost"
        settings.data_backend.timescale_port = 5432
        settings.data_backend.timescale_database = "test"
        settings.data_backend.timescale_user = "test_user"
        settings.data_backend.timescale_password = "test_password"
        settings.data_backend.timescale_use_ssl = False

        return settings

    @pytest.fixture
    def connection_config(self):
        """Create test connection configuration."""
        return ConnectionConfig(
            host="localhost",
            port=8080,
            database="test",
            user="test_user",
            password="test_password",
            ssl=False,
        )

    @pytest.fixture
    def pool_config(self):
        """Create test pool configuration."""
        return PoolConfig(
            min_size=1,
            max_size=5,
            max_idle=2,
            acquire_timeout_seconds=5.0,
            query_timeout_seconds=30.0,
        )

    @pytest.mark.asyncio
    async def test_connection_manager_registry(self):
        """Test the connection manager registry."""
        registry = ConnectionManagerRegistry()

        # Test registering a pool manager
        mock_pool = AsyncMock()
        await registry.register_pool("test_pool", mock_pool)

        # Test retrieving a pool manager
        retrieved_pool = await registry.get_pool("test_pool")
        assert retrieved_pool == mock_pool

        # Test getting all pools
        all_pools = await registry.get_all_pools()
        assert "test_pool" in all_pools
        assert all_pools["test_pool"] == mock_pool

    @pytest.mark.asyncio
    async def test_trino_pool_manager_creation(self, connection_config, pool_config):
        """Test Trino pool manager creation and basic operations."""
        with patch('aurum.database.trino_pool_manager.trino') as mock_trino:
            # Mock Trino connection
            mock_connection = MagicMock()
            mock_trino.dbapi.connect.return_value = mock_connection

            # Create pool manager
            pool_manager = TrinoPoolManager(connection_config, pool_config)

            # Test initialization
            await pool_manager.initialize()

            # Verify connection was created
            assert pool_manager._is_initialized
            assert len(pool_manager._connections) == pool_config.min_size

            # Test acquiring and releasing connection
            connection = await pool_manager.acquire_connection()
            assert connection is not None

            # Test pool metrics
            metrics = await pool_manager.get_pool_metrics()
            assert isinstance(metrics, PoolMetrics)
            assert metrics.active_connections == 1
            assert metrics.idle_connections == pool_config.min_size - 1

            # Release connection
            await pool_manager.release_connection(connection)

            # Test cleanup
            await pool_manager.close()
            assert pool_manager._is_closed

    @pytest.mark.asyncio
    async def test_timescale_pool_manager_creation(self, connection_config, pool_config):
        """Test TimescaleDB pool manager creation and basic operations."""
        with patch('aurum.database.timescale_pool_manager.psycopg') as mock_psycopg:
            # Mock psycopg AsyncConnectionPool
            mock_pool = AsyncMock()
            mock_psycopg.AsyncConnectionPool.return_value = mock_pool

            # Create pool manager
            pool_manager = TimescalePoolManager(connection_config, pool_config)

            # Test initialization
            await pool_manager.initialize()

            # Verify pool was created
            assert pool_manager._is_initialized
            assert pool_manager._pool == mock_pool

            # Test pool metrics
            mock_pool.get_stats.return_value = MagicMock(
                active=1,
                idle=2,
                total=3,
                utilization=0.6
            )

            metrics = await pool_manager.get_pool_metrics()
            assert isinstance(metrics, PoolMetrics)
            assert metrics.active_connections == 1
            assert metrics.idle_connections == 2

            # Test cleanup
            await pool_manager.close()
            assert pool_manager._is_closed

    @pytest.mark.asyncio
    async def test_database_pool_factory(self, mock_settings):
        """Test the database pool factory."""
        # Test creating Trino pool manager
        trino_pool = DatabasePoolFactory.create_pool_manager("trino", mock_settings)
        assert isinstance(trino_pool, TrinoPoolManager)

        # Test creating TimescaleDB pool manager
        timescale_pool = DatabasePoolFactory.create_pool_manager("timescale", mock_settings)
        assert isinstance(timescale_pool, TimescalePoolManager)

        # Test unsupported database type
        with pytest.raises(ValueError, match="Unsupported database type"):
            DatabasePoolFactory.create_pool_manager("unsupported", mock_settings)

    @pytest.mark.asyncio
    async def test_pool_context_manager(self, connection_config, pool_config):
        """Test pool context manager functionality."""
        with patch('aurum.database.trino_pool_manager.trino') as mock_trino:
            # Mock Trino connection
            mock_connection = MagicMock()
            mock_trino.dbapi.connect.return_value = mock_connection

            pool_manager = TrinoPoolManager(connection_config, pool_config)
            await pool_manager.initialize()

            # Test context manager
            async with pool_manager.get_connection() as connection:
                assert connection is not None

            # Verify connection was properly released
            metrics = await pool_manager.get_pool_metrics()
            assert metrics.active_connections == 0

    @pytest.mark.asyncio
    async def test_health_check_functionality(self, connection_config, pool_config):
        """Test health check functionality."""
        with patch('aurum.database.trino_pool_manager.trino') as mock_trino:
            # Mock Trino connection
            mock_connection = MagicMock()
            mock_trino.dbapi.connect.return_value = mock_connection

            pool_manager = TrinoPoolManager(connection_config, pool_config)
            await pool_manager.initialize()

            # Test health check
            is_healthy = await pool_manager.health_check()
            assert is_healthy

    @pytest.mark.asyncio
    async def test_connection_pool_metrics(self, connection_config, pool_config):
        """Test connection pool metrics collection."""
        with patch('aurum.database.trino_pool_manager.trino') as mock_trino:
            # Mock Trino connection
            mock_connection = MagicMock()
            mock_trino.dbapi.connect.return_value = mock_connection

            pool_manager = TrinoPoolManager(connection_config, pool_config)
            await pool_manager.initialize()

            # Acquire some connections to change metrics
            conn1 = await pool_manager.acquire_connection()
            conn2 = await pool_manager.acquire_connection()

            metrics = await pool_manager.get_pool_metrics()
            assert metrics.active_connections == 2
            assert metrics.idle_connections == pool_config.min_size - 2

            # Release connections
            await pool_manager.release_connection(conn1)
            await pool_manager.release_connection(conn2)

            metrics = await pool_manager.get_pool_metrics()
            assert metrics.active_connections == 0

    @pytest.mark.asyncio
    async def test_connection_pool_error_handling(self, connection_config, pool_config):
        """Test error handling in connection pool operations."""
        with patch('aurum.database.trino_pool_manager.trino') as mock_trino:
            # Mock Trino connection to raise exception
            mock_trino.dbapi.connect.side_effect = Exception("Connection failed")

            pool_manager = TrinoPoolManager(connection_config, pool_config)

            # Test initialization failure
            with pytest.raises(Exception, match="Connection failed"):
                await pool_manager.initialize()

            # Test that pool is not initialized after failure
            assert not pool_manager._is_initialized

    @pytest.mark.asyncio
    async def test_connection_lifecycle(self, connection_config, pool_config):
        """Test complete connection lifecycle."""
        with patch('aurum.database.trino_pool_manager.trino') as mock_trino:
            # Mock Trino connection
            mock_connection = MagicMock()
            mock_trino.dbapi.connect.return_value = mock_connection

            pool_manager = TrinoPoolManager(connection_config, pool_config)

            # Initialize pool
            await pool_manager.initialize()
            assert pool_manager._is_initialized

            # Acquire connection
            conn = await pool_manager.acquire_connection()
            assert conn is not None

            # Verify connection is tracked as active
            metrics = await pool_manager.get_pool_metrics()
            assert metrics.active_connections == 1

            # Release connection
            await pool_manager.release_connection(conn)

            # Verify connection is returned to pool
            metrics = await pool_manager.get_pool_metrics()
            assert metrics.active_connections == 0
            assert metrics.idle_connections == pool_config.min_size

            # Close pool
            await pool_manager.close()
            assert pool_manager._is_closed

    @pytest.mark.asyncio
    async def test_concurrent_connection_acquisition(self, connection_config, pool_config):
        """Test concurrent connection acquisition and release."""
        with patch('aurum.database.trino_pool_manager.trino') as mock_trino:
            # Mock Trino connection
            mock_connection = MagicMock()
            mock_trino.dbapi.connect.return_value = mock_connection

            pool_manager = TrinoPoolManager(connection_config, pool_config)
            await pool_manager.initialize()

            # Test concurrent acquisition
            tasks = []
            for i in range(3):
                task = asyncio.create_task(pool_manager.acquire_connection())
                tasks.append(task)

            connections = await asyncio.gather(*tasks)

            # Verify all connections acquired
            assert len(connections) == 3
            metrics = await pool_manager.get_pool_metrics()
            assert metrics.active_connections == 3

            # Release all connections concurrently
            release_tasks = []
            for conn in connections:
                task = asyncio.create_task(pool_manager.release_connection(conn))
                release_tasks.append(task)

            await asyncio.gather(*release_tasks)

            # Verify all connections released
            metrics = await pool_manager.get_pool_metrics()
            assert metrics.active_connections == 0

    @pytest.mark.asyncio
    async def test_pool_exhaustion_handling(self, connection_config):
        """Test handling of pool exhaustion."""
        # Create small pool configuration
        small_pool_config = PoolConfig(
            min_size=1,
            max_size=2,
            max_idle=1,
        )

        with patch('aurum.database.trino_pool_manager.trino') as mock_trino:
            # Mock Trino connection
            mock_connection = MagicMock()
            mock_trino.dbapi.connect.return_value = mock_connection

            pool_manager = TrinoPoolManager(connection_config, small_pool_config)
            await pool_manager.initialize()

            # Acquire maximum connections
            conn1 = await pool_manager.acquire_connection()
            conn2 = await pool_manager.acquire_connection()

            # Try to acquire one more (should fail)
            with pytest.raises(RuntimeError, match="Connection pool exhausted"):
                await pool_manager.acquire_connection()

            # Release connections
            await pool_manager.release_connection(conn1)
            await pool_manager.release_connection(conn2)

    @pytest.mark.asyncio
    async def test_stale_connection_handling(self, connection_config, pool_config):
        """Test handling of stale connections."""
        with patch('aurum.database.trino_pool_manager.trino') as mock_trino:
            # Mock Trino connection
            mock_connection = MagicMock()
            mock_trino.dbapi.connect.return_value = mock_connection

            pool_manager = TrinoPoolManager(connection_config, pool_config)
            await pool_manager.initialize()

            # Acquire connection
            conn = await pool_manager.acquire_connection()

            # Simulate connection becoming stale (old age)
            conn._created_at = 0  # Very old timestamp

            # Release connection - should be closed due to age
            await pool_manager.release_connection(conn)

            # Verify connection was closed due to staleness
            assert mock_connection.close.called


class TestProductionHealthMonitoring:
    """Test production health monitoring and alerting."""

    @pytest.mark.asyncio
    async def test_health_monitor_initialization(self):
        """Test health monitor initialization."""
        from aurum.database.production_monitor import ProductionDatabaseMonitor, AlertConfig

        alert_config = AlertConfig(enabled=False)  # Disable for testing
        monitor = ProductionDatabaseMonitor(alert_config=alert_config)

        assert len(monitor.alert_rules) == 4  # Default rules
        assert len(monitor.alert_handlers) == 0  # No handlers when disabled

    @pytest.mark.asyncio
    async def test_alert_rule_evaluation(self):
        """Test alert rule condition evaluation."""
        from aurum.database.production_monitor import ProductionDatabaseMonitor, AlertRule

        monitor = ProductionDatabaseMonitor()

        # Test pool utilization rule
        rule = AlertRule(
            name="test_utilization",
            condition="pool_utilization > 0.8",
            severity="warning"
        )

        # Test with high utilization
        high_util_metrics = PoolMetrics(
            active_connections=9,
            idle_connections=1,
            total_connections=10,
            max_connections=10,
            pool_utilization=0.9,
        )

        assert monitor._evaluate_condition(rule.condition, high_util_metrics)

        # Test with low utilization
        low_util_metrics = PoolMetrics(
            active_connections=2,
            idle_connections=8,
            total_connections=10,
            max_connections=10,
            pool_utilization=0.2,
        )

        assert not monitor._evaluate_condition(rule.condition, low_util_metrics)

    @pytest.mark.asyncio
    async def test_alert_cooldown(self):
        """Test alert cooldown functionality."""
        from aurum.database.production_monitor import ProductionDatabaseMonitor, AlertRule

        monitor = ProductionDatabaseMonitor()

        # Create a rule with short cooldown
        rule = AlertRule(
            name="test_rule",
            condition="pool_utilization > 0.5",
            cooldown_seconds=1  # 1 second cooldown
        )

        # Create metrics that trigger alert
        metrics = PoolMetrics(
            active_connections=6,
            idle_connections=4,
            total_connections=10,
            max_connections=10,
            pool_utilization=0.6,
        )

        # First alert should trigger
        assert monitor._evaluate_condition(rule.condition, metrics)

        # Simulate alert being sent
        alert_key = f"test_pool:{rule.name}"
        monitor._last_alert_times[alert_key] = asyncio.get_event_loop().time

        # Wait for cooldown to pass
        await asyncio.sleep(1.1)

        # Alert should trigger again after cooldown
        assert monitor._evaluate_condition(rule.condition, metrics)

    @pytest.mark.asyncio
    async def test_monitoring_status_reporting(self):
        """Test monitoring status reporting."""
        from aurum.database.production_monitor import ProductionDatabaseMonitor

        monitor = ProductionDatabaseMonitor()

        # Mock registry with some pools
        mock_registry = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.get_pool_metrics.return_value = PoolMetrics(
            active_connections=2,
            idle_connections=3,
            total_connections=5,
            max_connections=10,
            pool_utilization=0.5,
        )

        mock_registry.get_all_pools.return_value = {"test_pool": mock_pool}
        monitor.registry = mock_registry

        status = await monitor.get_monitoring_status()

        assert status["monitoring_active"] is True
        assert status["pools_monitored"] == 1
        assert "test_pool" in status["pool_metrics"]
