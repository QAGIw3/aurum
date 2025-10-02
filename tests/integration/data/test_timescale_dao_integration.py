"""Integration tests for TimescaleDAO with real TimescaleDB database."""

import pytest
from aurum.data.dao import TimescaleDAO


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_timescale_dao_health_check():
    """Test TimescaleDB DAO can connect."""
    async with TimescaleDAO() as dao:
        healthy = await dao.health_check()
        assert healthy is True


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_timescale_dao_simple_query():
    """Test TimescaleDB DAO can execute simple query."""
    async with TimescaleDAO() as dao:
        results = await dao.execute_query("SELECT 1 as test")
        
        assert len(results) == 1
        assert results[0]["test"] == 1


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_timescale_dao_time_series_operations():
    """Test TimescaleDB-specific time-series operations."""
    async with TimescaleDAO() as dao:
        # Create hypertable
        await dao.execute_query("""
            CREATE TEMP TABLE test_timeseries (
                time TIMESTAMPTZ NOT NULL,
                sensor_id INT,
                temperature DOUBLE PRECISION,
                humidity DOUBLE PRECISION
            )
        """)
        
        # Insert time-series data
        await dao.execute_many(
            "INSERT INTO test_timeseries VALUES (:time, :sensor_id, :temperature, :humidity)",
            [
                {
                    "time": f"2024-01-01 {i:02d}:00:00",
                    "sensor_id": 1,
                    "temperature": 20.0 + i * 0.5,
                    "humidity": 50.0
                }
                for i in range(24)
            ]
        )
        
        # Query time-series data
        results = await dao.execute_query("""
            SELECT
                DATE_TRUNC('hour', time) as hour,
                AVG(temperature) as avg_temp,
                MAX(temperature) as max_temp,
                MIN(temperature) as min_temp
            FROM test_timeseries
            WHERE sensor_id = 1
            GROUP BY DATE_TRUNC('hour', time)
            ORDER BY hour
            LIMIT 24
        """)
        
        assert len(results) == 24


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_timescale_dao_batch_insert():
    """Test TimescaleDB bulk insert performance."""
    async with TimescaleDAO() as dao:
        # Create test table
        await dao.execute_query("""
            CREATE TEMP TABLE test_batch (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ,
                value DOUBLE PRECISION
            )
        """)
        
        # Batch insert
        rows = [
            {"timestamp": f"2024-01-01 00:{i:02d}:00", "value": float(i)}
            for i in range(1000)
        ]
        
        affected = await dao.execute_many(
            "INSERT INTO test_batch (timestamp, value) VALUES (:timestamp, :value)",
            rows,
            batch_size=100
        )
        
        # Verify count
        result = await dao.execute_query_single("SELECT COUNT(*) as count FROM test_batch")
        assert result["count"] == 1000

