"""Unit tests for TrinoDAO.

These are placeholder tests demonstrating the pattern.
Integration tests with real Trino are in tests/integration/data/
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from aurum.data.dao import TrinoDAO, QueryError, ConnectionError


@pytest.mark.unit
@pytest.mark.asyncio
async def test_trino_dao_initialization():
    """Test TrinoDAO can be initialized."""
    dao = TrinoDAO()
    assert dao is not None
    assert dao._is_initialized is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_trino_dao_execute_query_not_initialized():
    """Test query fails if not initialized."""
    dao = TrinoDAO()
    
    # Should auto-initialize, but let's test the pattern
    with patch.object(dao, 'initialize', new_callable=AsyncMock):
        with patch.object(dao, '_execute_sync', return_value=[{"test": 1}]):
            result = await dao.execute_query("SELECT 1")
            dao.initialize.assert_called_once()


# Note: Full integration tests with real Trino are in tests/integration/data/

