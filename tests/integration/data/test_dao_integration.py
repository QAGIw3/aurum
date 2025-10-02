"""Integration tests for DAOs with real databases.

These tests require actual database connections.
Run with: pytest tests/integration/ -v -m integration
"""

import pytest
from aurum.data.dao import TrinoDAO, TimescaleDAO, PostgresDAO


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_health_check():
    """Test Trino DAO can connect and query."""
    async with TrinoDAO() as dao:
        healthy = await dao.health_check()
        assert healthy is True


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_simple_query():
    """Test Trino DAO can execute simple query."""
    async with TrinoDAO() as dao:
        result = await dao.execute_query("SELECT 1 as test")
        assert len(result) == 1
        assert result[0]["test"] == 1


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
async def test_postgres_dao_health_check():
    """Test PostgreSQL DAO can connect."""
    async with PostgresDAO() as dao:
        healthy = await dao.health_check()
        assert healthy is True


# Add more integration tests as needed

