# Integration Tests for Data Layer

Integration tests for DAOs and repositories with real database connections.

## Prerequisites

### Start Test Databases

```bash
# Start all required databases
docker-compose -f compose/docker-compose.dev.yml up -d postgres timescale trino clickhouse

# Verify services are running
docker-compose -f compose/docker-compose.dev.yml ps
```

### Environment Variables

Create `.env.test` file:

```bash
# PostgreSQL
AURUM_POSTGRES_HOST=localhost
AURUM_POSTGRES_PORT=5432
AURUM_POSTGRES_USER=aurum
AURUM_POSTGRES_PASSWORD=aurum
AURUM_POSTGRES_DATABASE=aurum_test

# TimescaleDB
AURUM_TIMESCALE_HOST=localhost
AURUM_TIMESCALE_PORT=5432
AURUM_TIMESCALE_USER=aurum
AURUM_TIMESCALE_PASSWORD=aurum
AURUM_TIMESCALE_DATABASE=timeseries_test

# Trino
AURUM_TRINO_HOST=localhost
AURUM_TRINO_PORT=8080
AURUM_TRINO_USER=aurum
AURUM_TRINO_CATALOG=iceberg
AURUM_TRINO_SCHEMA=market

# ClickHouse
AURUM_CLICKHOUSE_HOST=localhost
AURUM_CLICKHOUSE_PORT=9000
AURUM_CLICKHOUSE_USER=aurum
AURUM_CLICKHOUSE_DATABASE=aurum_test
```

## Running Tests

### All Integration Tests

```bash
pytest tests/integration/data/ -v -m integration
```

### Specific Database

```bash
# Trino tests only
pytest tests/integration/data/test_trino_dao_integration.py -v

# TimescaleDB tests only
pytest tests/integration/data/test_timescale_dao_integration.py -v

# PostgreSQL tests only
pytest tests/integration/data/test_postgres_dao_integration.py -v

# ClickHouse tests only
pytest tests/integration/data/test_clickhouse_dao_integration.py -v
```

### With Coverage

```bash
pytest tests/integration/data/ -v -m integration --cov=src/aurum/data/dao
```

## Test Categories

### DAO Integration Tests

Test database connectivity and operations:

- `test_dao_integration.py` - Basic DAO health checks
- `test_trino_dao_integration.py` - Trino-specific tests
- `test_timescale_dao_integration.py` - TimescaleDB-specific tests
- `test_postgres_dao_integration.py` - PostgreSQL-specific tests
- `test_clickhouse_dao_integration.py` - ClickHouse-specific tests

### Repository Integration Tests

Test domain logic with real databases:

- `test_curve_repository_integration.py` - Curve repository tests
- `test_scenario_repository_integration.py` - Scenario repository tests
- `test_metadata_repository_integration.py` - Metadata repository tests

### Service Integration Tests

Test complete service stack:

- `test_curve_service_integration.py` - End-to-end curve service tests
- `test_metadata_service_integration.py` - End-to-end metadata service tests

## Test Data Setup

### Database Schema

Create test schemas before running tests:

```sql
-- PostgreSQL
CREATE SCHEMA IF NOT EXISTS test_schema;

-- TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Trino
CREATE SCHEMA IF NOT EXISTS iceberg.test;
```

### Sample Data

Load sample data for testing:

```bash
# Load test fixtures
python3 scripts/load_test_data.py

# Or use SQL files
psql -h localhost -U aurum -d aurum_test -f tests/integration/data/fixtures/test_data.sql
```

## Writing Integration Tests

### Template

```python
import pytest
from aurum.data.dao import TrinoDAO

@pytest.mark.integration
@pytest.mark.database
@pytest.mark.asyncio
async def test_trino_connection():
    \"\"\"Test Trino DAO can connect and query.\"\"\"
    async with TrinoDAO() as dao:
        # Test connection
        healthy = await dao.health_check()
        assert healthy is True
        
        # Test simple query
        results = await dao.execute_query("SELECT 1 as test")
        assert len(results) == 1
        assert results[0]["test"] == 1
```

### Best Practices

1. **Use markers** - `@pytest.mark.integration`, `@pytest.mark.database`
2. **Clean up** - Use context managers for automatic cleanup
3. **Isolate tests** - Each test is independent
4. **Use fixtures** - Share database connections and test data
5. **Test real scenarios** - Query actual tables, not just SELECT 1

## Common Issues

### Connection Failures

```python
# Issue: Can't connect to database
# Fix: Check database is running and environment variables are correct

docker-compose ps
cat .env.test
```

### Timeout Errors

```python
# Issue: Query timeouts
# Fix: Increase timeout or optimize query

async with TrinoDAO() as dao:
    results = await dao.execute_query(query, timeout=60)  # 60 seconds
```

### Schema Not Found

```python
# Issue: Table or schema doesn't exist
# Fix: Create schema or update query

# Create schema first
await dao.execute_query("CREATE SCHEMA IF NOT EXISTS test_schema")
```

## Performance Benchmarks

Integration tests also serve as performance benchmarks:

```bash
# Benchmark DAO operations
pytest tests/integration/data/ -v --benchmark

# Compare old vs new implementation
pytest tests/integration/data/test_performance_comparison.py -v
```

## CI/CD Integration

### Local CI

```bash
# Quick smoke tests (fast)
pytest tests/integration/data/test_dao_integration.py::test_*_health_check -v

# Full integration suite (slow)
pytest tests/integration/data/ -v -m integration
```

### GitHub Actions

Integration tests run on pull requests:

```yaml
# .github/workflows/integration.yml
- name: Integration Tests
  run: |
    docker-compose up -d postgres timescale trino
    pytest tests/integration/ -v -m integration
```

## Troubleshooting

### Database Not Ready

```bash
# Wait for database to be ready
docker-compose up -d postgres
sleep 10  # Wait for startup

# Or use wait script
scripts/wait-for-it.sh localhost:5432 -- pytest tests/integration/
```

### Permission Errors

```bash
# Ensure test user has permissions
GRANT ALL ON SCHEMA test_schema TO aurum;
```

### Connection Pool Exhaustion

```bash
# Reduce test parallelism
pytest tests/integration/ -v -n 2  # Max 2 workers

# Or increase pool size
AURUM_DATABASE_POOL_SIZE=50 pytest tests/integration/
```

## Related Documentation

- [Data Layer README](../../../src/aurum/data/README.md)
- [Testing Guide](../../README.md)
- [DAO Implementation](../../../src/aurum/data/dao/)
- [Repository Implementation](../../../src/aurum/data/repositories/)

---

**For questions:** See the main testing guide or refactoring documentation.

