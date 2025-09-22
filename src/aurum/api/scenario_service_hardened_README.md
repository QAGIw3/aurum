# Hardened Postgres Scenario Store

This module provides a production-ready implementation of the scenario store with enhanced resilience, idempotency, and proper error handling.

## Features

### 🔒 Idempotency
- **Scenario Creation**: Unique constraints prevent duplicate scenarios per tenant/name
- **Run Creation**: Version hashes and idempotency keys ensure runs are not duplicated
- **Transaction Safety**: All operations are wrapped in proper database transactions

### 🛡️ Resilience
- **Circuit Breaker**: Protects against cascading failures during database outages
- **Retry Logic**: Exponential backoff for transient database errors
- **Connection Pooling**: Efficient database connection management
- **Graceful Degradation**: Clear error messages when services are unavailable

### 📊 Error Handling
- **RFC 7807 Compliance**: Structured error responses with proper HTTP status codes
- **Constraint Violations**: Meaningful error messages for unique constraint violations
- **Context Preservation**: Request IDs and detailed context in all error responses

### 🚀 Performance
- **Async Operations**: Full async support for better concurrency
- **Connection Pooling**: Reuses database connections efficiently
- **Batch Operations**: Optimized queries for bulk operations

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   API Layer     │───▶│ Hardened Store   │───▶│   Database      │
│ (scenarios.py)  │    │                  │    │ (PostgreSQL)    │
└─────────────────┘    │ - Circuit Breaker│    └─────────────────┘
                       │ - Retry Logic    │
                       │ - Transaction Mgmt│
                       │ - Error Mapping  │
                       └──────────────────┘
```

## Database Schema Requirements

The hardened store requires the following database constraints:

- `scenario_tenant_name_unique`: Ensures unique scenario names per tenant
- `model_run_scenario_hash_unique`: Ensures idempotent run creation by version hash
- `model_run_idempotency_key_unique`: Ensures unique idempotency keys

## Usage

```python
from aurum.api.scenario_service_hardened import PostgresScenarioStore

# Initialize store
store = PostgresScenarioStore("postgresql://user:pass@host:port/db")

# Create scenario with idempotency
try:
    scenario = await store.create_scenario(
        tenant_id="acme-corp",
        name="revenue-forecast-q4",
        description="Quarterly revenue forecasting",
        assumptions=[...]
    )
except ValidationException as e:
    # Handle constraint violations gracefully
    print(f"Scenario already exists: {e.detail}")

# Create run with idempotency
try:
    run = await store.create_run(
        scenario_id="scenario-123",
        code_version="v1.2.3",
        idempotency_key="forecast-q4-run-2025"
    )
except ValidationException as e:
    # Handle idempotent run creation
    print(f"Run already exists: {e.detail}")
```

## Error Handling

The hardened store provides structured error responses:

```json
{
  "error": {
    "type": "https://aurum.api/errors/scenario_already_exists",
    "title": "Scenario Already Exists",
    "detail": "A scenario with name 'revenue-forecast-q4' already exists for tenant 'acme-corp'",
    "instance": "req-12345678"
  }
}
```

## Configuration

Configure the store through environment variables:

```bash
# Database connection
DATABASE_URL="postgresql://user:pass@host:port/db"

# Circuit breaker settings
DB_FAILURE_THRESHOLD=5
DB_RESET_TIMEOUT=60

# Connection pool settings
DB_POOL_MIN_SIZE=5
DB_POOL_MAX_SIZE=20
```

## Migration

To migrate from the basic `PostgresScenarioStore`:

1. **Apply Database Migration**: Run `002_hardened_scenario_constraints.sql`
2. **Update Imports**: Replace `PostgresScenarioStore` with `PostgresScenarioStore`
3. **Update Error Handling**: Use new exception types and structured errors
4. **Test Thoroughly**: Verify idempotency and error handling work as expected

## Monitoring

The hardened store exposes metrics for monitoring:

- Circuit breaker state and failure count
- Database connection pool utilization
- Operation success/failure rates
- Retry attempt counts

## Testing

```python
import pytest
from unittest.mock import AsyncMock, patch

class TestHardenedScenarioStore:
    @patch('aurum.api.scenario_service_hardened.psycopg.AsyncConnectionPool')
    async def test_scenario_creation_idempotency(self, mock_pool):
        # Test that duplicate scenarios are handled gracefully
        pass

    async def test_circuit_breaker_behavior(self):
        # Test that circuit breaker opens after failures
        pass

    async def test_retry_logic(self):
        # Test exponential backoff for transient errors
        pass
```

## Security Considerations

- **SQL Injection Protection**: All queries use parameterized statements
- **Tenant Isolation**: Proper tenant scoping in all queries
- **Connection Security**: SSL/TLS support for database connections
- **Input Validation**: Comprehensive validation of all inputs

## Performance Considerations

- **Connection Pool Sizing**: Tune pool size based on expected load
- **Query Optimization**: Monitor and optimize slow queries
- **Index Usage**: Ensure proper indexes exist for all query patterns
- **Caching Strategy**: Consider caching for frequently accessed scenarios

## Troubleshooting

### Common Issues

1. **Circuit Breaker Open**: Check database connectivity and resource usage
2. **Constraint Violations**: Verify unique constraints are properly configured
3. **Connection Pool Exhaustion**: Monitor pool utilization and adjust sizes
4. **Slow Queries**: Check query plans and add missing indexes

### Debug Mode

Enable debug logging:

```python
import logging
logging.getLogger('aurum.api.scenario.hardened').setLevel(logging.DEBUG)
```
