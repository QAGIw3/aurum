# Core Domain Services

Core business logic services for primary domains.

## Services

### CurveService

Market curve operations and analytics.

**Dependencies:**
- `CurveRepository` - Data access

**Operations:**
- `get_curves()` - Query curves with filters
- `get_curve_by_key()` - Get specific curve
- `get_latest_asof()` - Get latest data date
- `compare_curves()` - Curve comparison analytics

**Example:**
```python
from aurum.services.core import CurveService
from aurum.data.repositories import CurveRepository

async def query_curves():
    async with CurveRepository() as repo:
        service = CurveService(repo)
        
        result = await service.get_curves(
            iso="PJM",
            market="DA",
            limit=100
        )
        
        if result.success:
            curves = result.data
            print(f"Retrieved {len(curves)} curves")
        else:
            print(f"Error: {result.error}")
```

## Adding New Services

1. **Create service file** in appropriate category (core/, external/, ml/, platform/)
2. **Extend BaseService** for common functionality
3. **Inject repositories** via constructor (dependency injection)
4. **Implement business logic** - validation, orchestration, analytics
5. **Return ServiceResult** for consistent interface
6. **Write tests** - unit tests with mocked repositories

**Template:**
```python
from ..base import BaseService, ServiceContext, ServiceResult
from aurum.data.repositories import SomeRepository

class MyService(BaseService):
    def __init__(self, repo: SomeRepository):
        super().__init__()
        self.repo = repo
    
    async def do_something(
        self,
        param: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict]:
        self._log_operation("do_something", context=context)
        
        try:
            # Business logic
            result = await self.repo.some_operation(param)
            
            return ServiceResult.ok(data=result)
        except Exception as e:
            raise self._handle_error(e, "do_something", context)
```

## Testing

### Unit Tests

Mock repositories to test business logic in isolation:

```python
import pytest
from unittest.mock import AsyncMock
from aurum.services.core import CurveService

@pytest.mark.asyncio
async def test_get_curves():
    # Mock repository
    repo = AsyncMock()
    repo.find_by_filters.return_value = [
        {"curve_key": "test", "value": 100}
    ]
    
    # Test service
    service = CurveService(repo)
    result = await service.get_curves(iso="PJM")
    
    assert result.success
    assert len(result.data) == 1
    repo.find_by_filters.assert_called_once()
```

### Integration Tests

Test with real repositories and databases:

```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_curves_integration():
    async with CurveRepository() as repo:
        service = CurveService(repo)
        
        result = await service.get_curves(
            iso="PJM",
            market="DA",
            limit=10
        )
        
        assert result.success
        assert isinstance(result.data, list)
```

## Best Practices

1. **Single Responsibility** - Each service handles one domain
2. **Dependency Injection** - Inject repositories, don't create them
3. **Validation First** - Validate inputs before repository calls
4. **Logging** - Use `_log_operation()` for audit trail
5. **Error Handling** - Use `_handle_error()` for consistent errors
6. **Context** - Pass `ServiceContext` for tenant/user info
7. **Results** - Return `ServiceResult` for consistent interface
8. **Testing** - Mock repositories for unit tests

## Migration from Legacy

### Old Pattern (Direct DAO)
```python
class SomeService:
    def __init__(self):
        self.dao = SomeDao()  # Direct DAO
    
    def get_data(self):
        return self.dao.query()  # Sync, mixed concerns
```

### New Pattern (Repository)
```python
class SomeService(BaseService):
    def __init__(self, repo: SomeRepository):
        super().__init__()
        self.repo = repo  # Injected repository
    
    async def get_data(
        self,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult:
        self._log_operation("get_data", context)
        
        try:
            # Business logic
            data = await self.repo.find_all()
            return ServiceResult.ok(data)
        except Exception as e:
            raise self._handle_error(e, "get_data", context)
```

## Related Documentation

- [Service Layer Guide](../README.md)
- [Repository Layer](../../data/repositories/README.md)
- [Migration Guide](../../../docs/refactoring/MIGRATION_GUIDE.md)

