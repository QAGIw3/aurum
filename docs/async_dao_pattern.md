# Async DAO Pattern with Connection Pooling

This document describes the async Data Access Object (DAO) pattern implementation for Aurum's database operations with automatic connection pooling.

## Overview

The async DAO pattern provides a clean separation between business logic and data access logic while leveraging asynchronous operations and connection pooling for optimal performance.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Service       │    │   Async DAO     │    │   Data Backend  │
│   Layer         │───▶│   Classes       │───▶│   with Pooling  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                │                       ▼
                                │               ┌─────────────────┐
                                │               │   Connection    │
                                └──────────────▶│   Pool          │
                                                └─────────────────┘
```

## Base Classes

### BaseAsyncDao

Abstract base class providing common functionality:

```python
from aurum.api.dao import BaseAsyncDao

class MyDao(BaseAsyncDao):
    @property
    def dao_name(self) -> str:
        return "my_dao"
    
    async def my_operation(self):
        return await self.execute_query("SELECT * FROM my_table")
```

**Key Methods:**
- `execute_query()` - Execute queries with automatic result transformation
- `execute_query_raw()` - Get raw columns and rows
- `execute_count_query()` - Execute count queries for pagination
- `close()` - Clean up resources

## Backend-Specific DAOs

### TrinoAsyncDao

For federated queries across multiple data sources:

```python
from aurum.api.dao import TrinoAsyncDao

dao = TrinoAsyncDao(settings)

# Federated query across catalogs
results = await dao.execute_federated_query(
    "SELECT * FROM iceberg.mart.energy_data JOIN clickhouse.raw.prices USING (id)",
    catalogs=["iceberg", "clickhouse"]
)

# Query Iceberg table metadata
tables = await dao.query_iceberg_tables(catalog="iceberg", schema="mart")

# Validate query syntax
validation = await dao.validate_query_syntax("SELECT invalid syntax")
```

### ClickHouseAsyncDao

For OLAP operations and analytical queries:

```python
from aurum.api.dao import ClickHouseAsyncDao

dao = ClickHouseAsyncDao(settings)

# Time series aggregation
data = await dao.query_time_series_data(
    table="energy_prices",
    start_time=datetime(2024, 1, 1),
    group_by_interval="1 DAY",
    aggregation="avg"
)

# Multi-dimensional metrics
metrics = await dao.query_aggregated_metrics(
    table="trades",
    metrics={"volume": "sum", "price": "avg"},
    dimensions=["region", "product"]
)

# Table optimization
await dao.optimize_table("energy_prices")
```

### TimescaleAsyncDao

For time-series operations on TimescaleDB:

```python
from aurum.api.dao import TimescaleAsyncDao

dao = TimescaleAsyncDao(settings)

# Time-bucketed queries
data = await dao.query_time_bucket_data(
    table="sensor_data",
    bucket_width="1 hour",
    aggregation="avg"
)

# Continuous aggregates
cagg_data = await dao.query_continuous_aggregate(
    cagg_name="hourly_summary",
    start_time=datetime.now() - timedelta(days=7)
)

# Hypertable management
info = await dao.get_hypertable_info("sensor_data")
await dao.compress_chunks("sensor_data", older_than=timedelta(days=7))
```

### EiaAsyncDao

For EIA-specific data operations:

```python
from aurum.api.dao import EiaAsyncDao

dao = EiaAsyncDao(settings)

# Query EIA series
series = await dao.query_eia_series(
    series_id="ELEC.GEN.ALL-US-99.M",
    start_date="2024-01-01",
    limit=1000
)

# Get available dimensions
dimensions = await dao.query_eia_series_dimensions(dataset="electricity")

# Count records for pagination
total = await dao.get_eia_series_count(frequency="monthly")
```

## Connection Pooling

All async DAOs leverage the existing connection pooling infrastructure:

- **Automatic Pool Management**: Pools are created and managed automatically
- **Resource Optimization**: Connections are reused across operations
- **Health Monitoring**: Unhealthy connections are automatically replaced
- **Configurable Limits**: Pool size and behavior can be configured

## Error Handling

Comprehensive error handling with structured logging:

```python
try:
    result = await dao.execute_query("SELECT * FROM table")
except Exception as e:
    # Detailed error information is logged automatically
    # including query hash, parameters, and backend info
    logger.error(f"Query failed: {e}")
```

## Best Practices

### 1. Resource Management

Always close DAOs when done:

```python
dao = EiaAsyncDao(settings)
try:
    result = await dao.query_eia_series()
finally:
    await dao.close()
```

Or use async context managers for automatic cleanup:

```python
async with EiaAsyncDao(settings) as dao:
    result = await dao.query_eia_series()
    # DAO is automatically closed when exiting the context
```

### 2. Concurrent Operations

Use `asyncio.gather()` for concurrent operations:

```python
results = await asyncio.gather(
    dao1.query_data(),
    dao2.query_data(),
    return_exceptions=True
)
```

### 3. Pagination

Use count queries for proper pagination:

```python
total = await dao.get_eia_series_count(filters)
pages = (total + limit - 1) // limit

for page in range(pages):
    offset = page * limit
    data = await dao.query_eia_series(offset=offset, limit=limit)
```

### 4. Backend Selection

Configure backend selection through settings:

```python
# settings.yaml
data_backend:
  backend_type: "trino"  # or "clickhouse", "timescale"
  connection_pool_min_size: 5
  connection_pool_max_size: 20
```

## Migration from Sync DAOs

### Before (Sync)
```python
dao = EiaDao(settings)
result = dao.query_eia_series()  # Blocking call
```

### After (Async)
```python
dao = EiaAsyncDao(settings)
result = await dao.query_eia_series()  # Non-blocking
await dao.close()  # Clean up
```

## Performance Benefits

1. **Non-blocking Operations**: Multiple database operations can run concurrently
2. **Connection Pooling**: Reduces connection overhead and improves throughput
3. **Resource Efficiency**: Better memory and CPU utilization
4. **Scalability**: Handles more concurrent requests with fewer resources

## Testing

Use async test patterns with mocking:

```python
@pytest.mark.asyncio
async def test_dao_operation():
    dao = EiaAsyncDao(mock_settings)
    dao._backend_adapter = mock_adapter
    
    result = await dao.query_eia_series()
    assert len(result) > 0
    
    await dao.close()
```

## Observability

All operations include comprehensive logging and metrics:

- Query execution times
- Connection pool metrics
- Error rates and types
- Backend performance statistics

This enables effective monitoring and troubleshooting of database operations.