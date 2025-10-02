# Migration Guide: Refactored Data Access Layer

This guide helps developers migrate from the legacy sync DAOs to the new async repository pattern.

## Overview

The new data access layer provides:
- ✅ Async-first design for better performance
- ✅ Connection pooling and resource management
- ✅ Clean separation of concerns (DAO → Repository → Service)
- ✅ SOLID principles throughout
- ✅ Better error handling and logging
- ✅ Support for streaming large result sets

## Quick Migration Examples

### Example 1: Simple Query

**Before (Legacy):**
```python
from aurum.api.dao import CurvesDao

dao = CurvesDao()
results = dao.query_curves(iso="PJM", market="DA", limit=100)
```

**After (New):**
```python
from aurum.data.repositories import CurveRepository

async def get_curves():
    async with CurveRepository() as repo:
        results = await repo.find_by_filters(
            iso="PJM",
            market="DA",
            limit=100
        )
    return results
```

### Example 2: Scenario Operations

**Before (Legacy):**
```python
from aurum.api.legacy.async_service_legacy import AsyncScenarioService

service = AsyncScenarioService(config)
scenario = service.create_scenario(name="Test", assumptions={...})
```

**After (New):**
```python
from aurum.data.repositories import ScenarioRepository

async def create_scenario():
    async with ScenarioRepository() as repo:
        scenario = await repo.create_scenario(
            name="Test",
            assumptions={...},
            tenant_id="tenant-123"
        )
    return scenario
```

### Example 3: Metadata Queries

**Before (Legacy):**
```python
from aurum.api.dao import MetadataDao

dao = MetadataDao()
dimensions = dao.get_dimensions(dataset="curves")
```

**After (New):**
```python
from aurum.data.repositories import MetadataRepository

async def get_dimensions():
    async with MetadataRepository() as repo:
        dimensions = await repo.get_all_dimensions(dataset="curves")
    return dimensions
```

## Service Layer Migration

### Pattern: Service Uses Repository

Services should use repositories, not DAOs directly.

**Before:**
```python
class CurveService:
    def __init__(self):
        self.dao = CurvesDao()  # Direct DAO usage
    
    def get_curves(self, iso: str):
        return self.dao.query_curves(iso=iso)
```

**After:**
```python
class CurveService:
    def __init__(self, curve_repo: CurveRepository):
        self.curve_repo = curve_repo  # Dependency injection
    
    async def get_curves(self, iso: str):
        return await self.curve_repo.find_by_filters(iso=iso)
```

## Error Handling

### New Error Types

```python
from aurum.data.dao import DAOError, ConnectionError, QueryError

try:
    async with TrinoDAO() as dao:
        results = await dao.execute_query(query)
except QueryError as e:
    # Query execution failed
    logger.error(f"Query failed: {e.query}")
    logger.error(f"Params: {e.params}")
except ConnectionError as e:
    # Database connection failed
    logger.error(f"Connection failed: {e}")
except DAOError as e:
    # Other DAO errors
    logger.error(f"DAO error: {e}")
```

## Async Patterns

### Context Managers

Always use async context managers for resource cleanup:

```python
# ✅ Good
async with CurveRepository() as repo:
    results = await repo.find_by_key("curve-123")
# Resources automatically cleaned up

# ❌ Bad
repo = CurveRepository()
await repo.initialize()
results = await repo.find_by_key("curve-123")
# Forgot to call repo.close()!
```

### Async Function Conversion

Convert sync functions to async:

```python
# Before
def get_curves(iso: str):
    dao = CurvesDao()
    return dao.query_curves(iso=iso)

# After
async def get_curves(iso: str):
    async with CurveRepository() as repo:
        return await repo.find_by_filters(iso=iso)

# Update callers
curves = await get_curves("PJM")
```

### Async in FastAPI Routes

FastAPI natively supports async:

```python
from fastapi import APIRouter, Depends
from aurum.data.repositories import CurveRepository

router = APIRouter()

async def get_curve_repo() -> CurveRepository:
    repo = CurveRepository()
    await repo.initialize()
    try:
        yield repo
    finally:
        await repo.close()

@router.get("/curves")
async def list_curves(
    iso: str,
    repo: CurveRepository = Depends(get_curve_repo)
):
    curves = await repo.find_by_filters(iso=iso)
    return curves
```

## Database Selection

### Choosing the Right Backend

- **TrinoDAO**: Large analytical queries, Iceberg tables, federated queries
- **TimescaleDAO**: Time-series data, high-frequency metrics, IoT data
- **ClickHouseDAO**: Logs, analytics, high-cardinality aggregations
- **PostgresDAO**: Transactional data, metadata, user management

### Example: Multi-Backend Repository

```python
class HybridRepository(BaseRepository):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._trino_dao = None
        self._postgres_dao = None
    
    async def initialize(self):
        # Use Trino for analytics
        self._trino_dao = TrinoDAO(self.settings)
        await self._trino_dao.initialize()
        
        # Use Postgres for metadata
        self._postgres_dao = PostgresDAO(self.settings)
        await self._postgres_dao.initialize()
    
    async def get_metadata(self, entity_id: str):
        # Fast lookup in Postgres
        return await self._postgres_dao.execute_query_single(
            "SELECT * FROM metadata WHERE id = :id",
            {"id": entity_id}
        )
    
    async def get_analytics(self, entity_id: str):
        # Large query in Trino
        return await self._trino_dao.execute_query(
            "SELECT * FROM iceberg.analytics WHERE entity_id = :id",
            {"id": entity_id}
        )
```

## Performance Optimization

### Streaming Large Results

```python
# Instead of loading everything into memory:
async with TrinoDAO() as dao:
    results = await dao.execute_query(
        "SELECT * FROM huge_table"  # Could be millions of rows!
    )

# Stream in chunks:
async with TrinoDAO() as dao:
    async for chunk in dao.stream_query(
        "SELECT * FROM huge_table",
        chunk_size=1000
    ):
        # Process each chunk
        await process_chunk(chunk)
```

### Batch Operations

```python
# Instead of individual inserts:
for item in items:
    await dao.execute_query(
        "INSERT INTO table VALUES (:id, :value)",
        {"id": item.id, "value": item.value}
    )

# Batch them:
params_list = [
    {"id": item.id, "value": item.value}
    for item in items
]
await dao.execute_many(
    "INSERT INTO table VALUES (:id, :value)",
    params_list,
    batch_size=1000
)
```

## Testing

### Unit Tests with Mocks

```python
import pytest
from unittest.mock import AsyncMock
from aurum.data.repositories import CurveRepository

@pytest.mark.asyncio
async def test_find_curves():
    repo = CurveRepository()
    repo._trino_dao = AsyncMock()
    repo._trino_dao.execute_query.return_value = [
        {"curve_key": "test", "value": 100}
    ]
    
    results = await repo.find_by_key("test")
    
    assert len(results) == 1
    assert results[0]["curve_key"] == "test"
    repo._trino_dao.execute_query.assert_called_once()
```

### Integration Tests

```python
import pytest
from aurum.data.repositories import CurveRepository

@pytest.mark.integration
@pytest.mark.asyncio
async def test_find_curves_integration():
    async with CurveRepository() as repo:
        # Test against real database
        results = await repo.find_by_filters(
            iso="PJM",
            market="DA",
            limit=10
        )
        assert isinstance(results, list)
        assert len(results) <= 10
```

## Common Pitfalls

### 1. Forgetting await

```python
# ❌ Wrong - missing await
results = repo.find_by_key("test")

# ✅ Correct
results = await repo.find_by_key("test")
```

### 2. Not Using Context Managers

```python
# ❌ Wrong - resource leak
repo = CurveRepository()
await repo.initialize()
results = await repo.find_by_key("test")
# Oops, forgot to close!

# ✅ Correct
async with CurveRepository() as repo:
    results = await repo.find_by_key("test")
```

### 3. Mixing Sync and Async

```python
# ❌ Wrong - can't call async from sync without event loop
def sync_function():
    repo = CurveRepository()
    results = await repo.find_by_key("test")  # SyntaxError!

# ✅ Option 1: Make function async
async def async_function():
    async with CurveRepository() as repo:
        results = await repo.find_by_key("test")

# ✅ Option 2: Use asyncio.run (for scripts/CLI)
import asyncio

def sync_function():
    async def inner():
        async with CurveRepository() as repo:
            return await repo.find_by_key("test")
    
    return asyncio.run(inner())
```

## Migration Checklist

For each service/module you migrate:

- [ ] Replace DAO imports with repository imports
- [ ] Convert functions to async (add `async def`)
- [ ] Add `await` to all async calls
- [ ] Use async context managers (`async with`)
- [ ] Update error handling for new exception types
- [ ] Update tests to be async
- [ ] Test with real databases (integration tests)
- [ ] Update documentation
- [ ] Remove old DAO imports

## Support

If you encounter issues during migration:

1. Check this guide for common patterns
2. Review the data layer README: `src/aurum/data/README.md`
3. Look at example repositories: `src/aurum/data/repositories/`
4. Check the legacy audit: `docs/refactoring/LEGACY_AUDIT.md`
5. Ask questions in team discussions

## Timeline

- **Phase 1 (Complete)**: New DAO and repository layer created
- **Phase 2 (Current)**: Migrate services one by one
- **Phase 3**: Remove legacy DAOs
- **Phase 4**: Performance optimization and refinement

