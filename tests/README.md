# Test Organization

This directory contains all tests for the Aurum platform, organized by test type.

## Structure

```
tests/
├── unit/                # Fast, isolated unit tests
│   ├── data/            # DAO and repository tests
│   ├── services/        # Service layer tests
│   ├── api/             # API route tests
│   └── external/        # External collector tests
├── integration/         # Integration tests with real systems
│   ├── data/            # Database integration tests
│   ├── api/             # API integration tests
│   └── kafka/           # Kafka integration tests
├── e2e/                 # End-to-end tests
├── contract/            # API contract tests
├── fixtures/            # Shared test fixtures
└── conftest.py          # Shared pytest configuration
```

## Test Types

### Unit Tests (`unit/`)

**Purpose:** Test individual components in isolation with mocked dependencies.

**Characteristics:**
- Fast (< 1 second per test)
- No external dependencies
- Use mocks/fakes for dependencies
- Test business logic only

**Run:**
```bash
pytest tests/unit/ -v
```

**Example:**
```python
from unittest.mock import AsyncMock
from aurum.services.core import CurveService

async def test_get_curves():
    repo = AsyncMock()
    repo.find_by_filters.return_value = [{"curve_key": "test"}]
    
    service = CurveService(repo)
    result = await service.get_curves(iso="PJM")
    
    assert result.success
    assert len(result.data) == 1
```

### Integration Tests (`integration/`)

**Purpose:** Test components working together with real external systems.

**Characteristics:**
- Slower (seconds to minutes)
- Uses real databases, Kafka, etc.
- Tests actual integration
- Requires test infrastructure

**Run:**
```bash
# Start test infrastructure
docker-compose -f compose/docker-compose.dev.yml up -d postgres timescale trino

# Run integration tests
pytest tests/integration/ -v -m integration
```

**Example:**
```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_trino_query():
    async with TrinoDAO() as dao:
        results = await dao.execute_query("SELECT 1 as test")
        assert results[0]["test"] == 1
```

### End-to-End Tests (`e2e/`)

**Purpose:** Test complete user workflows from API to database.

**Characteristics:**
- Slowest (minutes)
- Full stack integration
- Tests entire system
- Most realistic

**Run:**
```bash
pytest tests/e2e/ -v -m e2e
```

### Contract Tests (`contract/`)

**Purpose:** Verify API contracts and OpenAPI spec compliance.

**Characteristics:**
- Validates request/response schemas
- Tests API versioning
- Ensures backward compatibility
- Uses schema validation

**Run:**
```bash
pytest tests/contract/ -v
```

## Running Tests

### All Tests
```bash
pytest tests/
```

### By Type
```bash
pytest tests/unit/                  # Unit tests only
pytest tests/integration/           # Integration tests only
pytest tests/e2e/                   # E2E tests only
```

### By Module
```bash
pytest tests/unit/services/         # Service tests
pytest tests/unit/data/             # Data layer tests
pytest tests/integration/data/      # Database integration tests
```

### By Marker
```bash
pytest -m unit                      # All unit tests
pytest -m integration               # All integration tests
pytest -m "not integration"         # Skip integration tests
```

### With Coverage
```bash
pytest tests/ --cov=src/aurum --cov-report=html
```

### Specific Test
```bash
pytest tests/unit/services/test_curve_service.py::test_get_curves -v
```

## Test Markers

Tests use pytest markers for organization:

```python
@pytest.mark.unit              # Unit test
@pytest.mark.integration       # Integration test
@pytest.mark.e2e              # End-to-end test
@pytest.mark.asyncio          # Async test
@pytest.mark.slow             # Slow test (skip in quick runs)
@pytest.mark.database         # Requires database
@pytest.mark.kafka            # Requires Kafka
```

## Fixtures

Shared fixtures are in `fixtures/` and `conftest.py`:

- `mock_settings` - Mocked application settings
- `test_db` - Test database connection
- `test_kafka` - Test Kafka setup
- `mock_curve_repo` - Mocked curve repository
- `test_service_context` - Test service context

## Writing Tests

### Unit Test Template

```python
import pytest
from unittest.mock import AsyncMock
from aurum.services.core import MyService

@pytest.fixture
def mock_repo():
    return AsyncMock()

@pytest.mark.asyncio
async def test_my_operation(mock_repo):
    # Arrange
    mock_repo.some_method.return_value = {"data": "test"}
    service = MyService(mock_repo)
    
    # Act
    result = await service.do_something()
    
    # Assert
    assert result.success
    assert result.data == {"data": "test"}
    mock_repo.some_method.assert_called_once()
```

### Integration Test Template

```python
import pytest
from aurum.data.repositories import MyRepository

@pytest.mark.integration
@pytest.mark.asyncio
async def test_repository_operation():
    # Use real database
    async with MyRepository() as repo:
        # Act
        result = await repo.find_all()
        
        # Assert
        assert isinstance(result, list)
```

## Best Practices

1. **Test Isolation** - Each test is independent
2. **Clear Names** - Test names describe what is being tested
3. **AAA Pattern** - Arrange, Act, Assert
4. **One Assertion** - Test one thing per test (guideline)
5. **Fast Tests** - Keep unit tests < 1 second
6. **Mock External** - Mock external dependencies in unit tests
7. **Clean Up** - Use fixtures for setup/teardown
8. **Skip Slow** - Mark slow tests, skip in CI

## CI/CD Integration

### Pull Request Checks
```bash
# Fast feedback - unit tests only
pytest tests/unit/ --maxfail=5
```

### Merge to Main
```bash
# Comprehensive - all tests
pytest tests/ --cov=src/aurum --cov-fail-under=85
```

### Nightly
```bash
# Full suite including slow tests
pytest tests/ --run-slow
```

## Test Coverage

Current coverage targets:
- Overall: >85%
- New code: >90%
- Critical paths: 100%

View coverage report:
```bash
pytest --cov=src/aurum --cov-report=html
open htmlcov/index.html
```

## Troubleshooting

### Tests Hanging
- Check for missing `@pytest.mark.asyncio`
- Ensure async context managers are closed
- Check for infinite loops

### Import Errors
- Install package: `pip install -e .`
- Check PYTHONPATH
- Verify test file naming (`test_*.py`)

### Database Connection Errors
- Start test databases: `docker-compose up postgres timescale`
- Check connection strings in `.env`
- Verify network connectivity

### Flaky Tests
- Add retries for integration tests
- Use proper synchronization
- Check for race conditions
- Isolate test data

## Migration from Old Structure

Old test files scattered across:
- `tests/api/`
- `tests/services/`
- `tests/unit/`
- `tests/integration/`

New organization:
- Unit tests → `tests/unit/{module}/`
- Integration tests → `tests/integration/{module}/`
- Shared fixtures → `tests/fixtures/`

## Related Documentation

- [Testing Guide](../docs/testing.md)
- [CI/CD Pipeline](../.github/workflows/README.md)
- [Contributing](../CONTRIBUTING.md)

