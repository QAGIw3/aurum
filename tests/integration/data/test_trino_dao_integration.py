"""Integration tests for TrinoDAO with real Trino database."""

import pytest
from aurum.data.dao import TrinoDAO


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_initialization():
    """Test TrinoDAO can be initialized."""
    dao = TrinoDAO()
    assert dao is not None
    assert dao._is_initialized is False


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_health_check():
    """Test Trino DAO can connect and perform health check."""
    async with TrinoDAO() as dao:
        healthy = await dao.health_check()
        assert healthy is True


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_simple_query():
    """Test Trino DAO can execute simple query."""
    async with TrinoDAO() as dao:
        results = await dao.execute_query("SELECT 1 as test, 'hello' as message")
        
        assert len(results) == 1
        assert results[0]["test"] == 1
        assert results[0]["message"] == "hello"


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_parameterized_query():
    """Test Trino DAO can execute parameterized query."""
    async with TrinoDAO() as dao:
        query = "SELECT :value as test_value"
        results = await dao.execute_query(query, {"value": 42})
        
        assert len(results) == 1
        assert results[0]["test_value"] == 42


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_execute_query_single():
    """Test TrinoDAO single result query."""
    async with TrinoDAO() as dao:
        result = await dao.execute_query_single("SELECT 100 as value")
        
        assert result is not None
        assert result["value"] == 100


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_streaming():
    """Test TrinoDAO streaming for large result sets."""
    async with TrinoDAO() as dao:
        # Generate larger result set
        query = """
            SELECT seq as id, seq * 2 as value
            FROM UNNEST(SEQUENCE(1, 100)) AS t(seq)
        """
        
        total_rows = 0
        chunk_sizes = []
        
        async for chunk in dao.stream_query(query, chunk_size=25):
            chunk_sizes.append(len(chunk))
            total_rows += len(chunk)
        
        assert total_rows == 100
        # Should have 4 chunks of 25 rows each
        assert len(chunk_sizes) == 4
        assert all(size == 25 for size in chunk_sizes)


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_context_manager():
    """Test TrinoDAO context manager properly initializes and closes."""
    dao = TrinoDAO()
    assert dao._is_initialized is False
    
    async with dao:
        assert dao._is_initialized is True
        result = await dao.health_check()
        assert result is True
    
    # Context manager should have cleaned up
    assert dao._is_initialized is False


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_dao_catalog_schemas():
    """Test querying Trino catalogs and schemas."""
    async with TrinoDAO() as dao:
        # Query available catalogs
        catalogs = await dao.execute_query("SHOW CATALOGS")
        assert len(catalogs) > 0
        
        # Query schemas in iceberg catalog
        schemas = await dao.execute_query("SHOW SCHEMAS FROM iceberg")
        assert len(schemas) > 0


@pytest.mark.integration
@pytest.mark.database
@pytest.mark.slow
@pytest.mark.asyncio
async def test_trino_dao_concurrent_queries():
    """Test TrinoDAO can handle concurrent queries."""
    async with TrinoDAO() as dao:
        # Execute multiple queries concurrently
        queries = [
            dao.execute_query(f"SELECT {i} as value")
            for i in range(10)
        ]
        
        import asyncio
        results = await asyncio.gather(*queries)
        
        assert len(results) == 10
        for i, result in enumerate(results):
            assert result[0]["value"] == i


# Add more Trino-specific tests as needed
# - Complex joins
# - Aggregations
# - Window functions
# - Iceberg table operations
# - Performance benchmarks

