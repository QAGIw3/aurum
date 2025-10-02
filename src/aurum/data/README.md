# Data Access Layer

This package provides a unified, async-first data access layer for the Aurum platform.

## Architecture

The data layer follows a three-tier architecture:

```
Services (Business Logic)
    ↓
Repositories (Domain Logic)
    ↓
DAOs (Database Access)
    ↓
Databases (Trino, TimescaleDB, ClickHouse, Postgres)
```

### Design Principles

1. **Separation of Concerns**: Each layer has a single responsibility
2. **Async-First**: All operations use asyncio for optimal performance
3. **Connection Pooling**: Efficient resource management
4. **SOLID Principles**: Clean, maintainable, extensible code
5. **Repository Pattern**: Domain logic separate from data access

## Components

### DAOs (Data Access Objects)

Location: `dao/`

DAOs handle low-level database operations:

- **BaseAsyncDAO**: Abstract base class with common functionality
- **TrinoDAO**: Federated SQL queries, Iceberg tables, OLAP
- **TimescaleDAO**: Time-series data, high-frequency metrics
- **ClickHouseDAO**: Analytics, logs, high-cardinality data
- **PostgresDAO**: Operational data, transactions, metadata

**Example Usage:**

```python
from aurum.data.dao import TrinoDAO

async def query_curves():
    async with TrinoDAO() as dao:
        results = await dao.execute_query(
            "SELECT * FROM iceberg.market.curve_observation LIMIT 100"
        )
        return results
```

### Repositories

Location: `repositories/`

Repositories provide domain-specific operations:

- **CurveRepository**: Market curve operations
- **ScenarioRepository**: Scenario modeling operations
- **MetadataRepository**: Dimensions and catalog operations

**Example Usage:**

```python
from aurum.data.repositories import CurveRepository

async def get_latest_curves():
    async with CurveRepository() as repo:
        curves = await repo.find_by_filters(
            iso="PJM",
            market="DA",
            limit=100
        )
        return curves
```

## Migration from Legacy DAOs

### Old Pattern (Synchronous)

```python
from aurum.api.dao import CurvesDao

dao = CurvesDao()
results = dao.query_curves(iso="PJM", market="DA")
```

### New Pattern (Async)

```python
from aurum.data.repositories import CurveRepository

async with CurveRepository() as repo:
    results = await repo.find_by_filters(iso="PJM", market="DA")
```

### Migration Steps

1. Replace DAO imports with repository imports
2. Convert synchronous functions to async
3. Use async context managers for automatic resource cleanup
4. Update calling code to use `await`

## Database Configuration

Configure databases in `AurumSettings`:

```python
# settings.toml or environment variables
[data_backend]
backend_type = "trino"  # Primary backend

# Trino
trino_host = "localhost"
trino_port = 8080
trino_catalog = "iceberg"
trino_database_schema = "market"
trino_pool_size = 10

# TimescaleDB
timescale_host = "localhost"
timescale_port = 5432
timescale_database = "aurum"
timescale_pool_min_size = 2
timescale_pool_max_size = 20

# ClickHouse
clickhouse_host = "localhost"
clickhouse_port = 9000
clickhouse_database = "default"

# Postgres
postgres_host = "localhost"
postgres_port = 5432
postgres_database = "aurum"
postgres_pool_min_size = 2
postgres_pool_max_size = 20
```

## Best Practices

### 1. Use Context Managers

Always use async context managers for automatic resource cleanup:

```python
async with CurveRepository() as repo:
    # Do work
    pass
# Resources automatically cleaned up
```

### 2. Handle Errors Gracefully

```python
from aurum.data.dao import QueryError, ConnectionError

try:
    async with TrinoDAO() as dao:
        results = await dao.execute_query(query)
except QueryError as e:
    logger.error(f"Query failed: {e.query}")
except ConnectionError as e:
    logger.error(f"Connection failed: {e}")
```

### 3. Use Streaming for Large Results

```python
async with TrinoDAO() as dao:
    async for chunk in dao.stream_query(query, chunk_size=1000):
        # Process chunk
        process_rows(chunk)
```

### 4. Batch Operations

```python
params_list = [
    {"id": 1, "value": 100},
    {"id": 2, "value": 200},
    # ... many more
]

async with PostgresDAO() as dao:
    affected = await dao.execute_many(
        "INSERT INTO table VALUES (:id, :value)",
        params_list,
        batch_size=1000
    )
```

### 5. Transactions (Postgres only)

```python
async with PostgresDAO() as dao:
    async with dao.transaction() as conn:
        # All operations in this block are atomic
        await conn.execute("INSERT ...")
        await conn.execute("UPDATE ...")
        # Automatically committed on success, rolled back on error
```

## Testing

### Unit Tests

Mock DAOs for testing repositories:

```python
from unittest.mock import AsyncMock
from aurum.data.repositories import CurveRepository

async def test_find_curves():
    repo = CurveRepository()
    repo._trino_dao = AsyncMock()
    repo._trino_dao.execute_query.return_value = [
        {"curve_key": "test", "value": 100}
    ]
    
    results = await repo.find_by_key("test")
    assert len(results) == 1
```

### Integration Tests

Test against real databases (use test database):

```python
import pytest
from aurum.data.dao import TrinoDAO

@pytest.mark.integration
@pytest.mark.asyncio
async def test_trino_connection():
    async with TrinoDAO() as dao:
        result = await dao.execute_query("SELECT 1 as test")
        assert result[0]["test"] == 1
```

## Performance Considerations

### Connection Pooling

- Default pool sizes are optimized for typical workloads
- Adjust based on your concurrent query needs
- Monitor pool usage and tune as needed

### Query Optimization

- Use `execute_query_single()` when expecting one result
- Use `stream_query()` for large result sets
- Use `execute_many()` for bulk inserts/updates
- Add appropriate indexes on frequently queried columns

### Caching

Consider caching at the repository level for frequently accessed data:

```python
from functools import lru_cache

class CurveRepository:
    @lru_cache(maxsize=100)
    async def get_latest_asof(self, iso: str) -> date:
        # Cached for repeated calls
        ...
```

## Future Enhancements

- [ ] Read replicas support
- [ ] Query result caching
- [ ] Automatic retry with exponential backoff
- [ ] Circuit breaker pattern
- [ ] Distributed tracing integration
- [ ] Query performance metrics
- [ ] Connection health monitoring

## Related Documentation

- [Architecture Overview](../../../docs/architecture-overview.md)
- [Legacy Audit](../../../docs/refactoring/LEGACY_AUDIT.md)
- [Service Layer](../../services/README.md)

