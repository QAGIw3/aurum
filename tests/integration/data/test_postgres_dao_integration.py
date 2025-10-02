"""Integration tests for PostgresDAO with real PostgreSQL database."""

import pytest
from aurum.data.dao import PostgresDAO


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_postgres_dao_health_check():
    """Test PostgreSQL DAO can connect."""
    async with PostgresDAO() as dao:
        healthy = await dao.health_check()
        assert healthy is True


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_postgres_dao_simple_query():
    """Test PostgreSQL DAO can execute simple query."""
    async with PostgresDAO() as dao:
        results = await dao.execute_query("SELECT 1 as test")
        
        assert len(results) == 1
        assert results[0]["test"] == 1


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_postgres_dao_parameterized_query():
    """Test PostgreSQL DAO parameterized queries."""
    async with PostgresDAO() as dao:
        query = "SELECT :value as test_value"
        results = await dao.execute_query(query, {"value": 42})
        
        assert len(results) == 1
        assert results[0]["test_value"] == 42


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_postgres_dao_transaction():
    """Test PostgreSQL DAO transaction support."""
    async with PostgresDAO() as dao:
        # Create test table
        await dao.execute_query("""
            CREATE TEMP TABLE test_transactions (
                id SERIAL PRIMARY KEY,
                value TEXT
            )
        """)
        
        # Use transaction
        async with dao.transaction() as conn:
            await conn.execute(
                "INSERT INTO test_transactions (value) VALUES ($1)",
                "test_value"
            )
            
            # Query within transaction
            result = await conn.fetchrow("SELECT * FROM test_transactions WHERE value = $1", "test_value")
            assert result is not None
        
        # Transaction committed - verify data persists
        results = await dao.execute_query("SELECT * FROM test_transactions")
        assert len(results) == 1


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_postgres_dao_streaming():
    """Test PostgreSQL DAO streaming."""
    async with PostgresDAO() as dao:
        # Create test data
        await dao.execute_query("""
            CREATE TEMP TABLE test_stream (id INT, value TEXT)
        """)
        
        # Insert test data
        await dao.execute_many(
            "INSERT INTO test_stream VALUES (:id, :value)",
            [{"id": i, "value": f"row_{i}"} for i in range(100)]
        )
        
        # Stream results
        total_rows = 0
        async for chunk in dao.stream_query(
            "SELECT * FROM test_stream ORDER BY id",
            chunk_size=25
        ):
            total_rows += len(chunk)
        
        assert total_rows == 100

