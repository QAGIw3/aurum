# Refactored Maintenance System

This document describes the refactored maintenance system for Aurum, which provides a unified, extensible architecture for managing maintenance operations across different storage backends.

## Architecture Overview

The refactored system follows established Aurum patterns and provides clear separation of concerns:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Service Layer │───▶│   Operations    │───▶│    Backends     │
│                 │    │                 │    │                 │
│ MaintenanceService  │    │ CompactionOp    │    │   IcebergBackend│
│ Factory Pattern │    │ RetentionOp     │    │ TimescaleBackend│
│ Dependency Injection  │ PartitionEvolOp │    │ ClickHouseBackend
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Key Components

1. **Interfaces** (`interfaces.py`) - Core protocols and data structures
2. **Backends** (`backends/`) - Storage-specific implementations  
3. **Operations** (`operations/`) - Maintenance operation implementations
4. **Executor** (`executor.py`) - Async execution engine with resource management
5. **Factory** (`factory.py`) - Component creation with registry pattern
6. **Service** (`service.py`) - High-level API following Aurum patterns

## Usage Examples

### Basic Usage

```python
from aurum.maintenance import MaintenanceService, OperationType

# Create service (uses AurumSettings for configuration)
service = MaintenanceService()

# Execute a single maintenance operation
result = await service.execute_maintenance(
    backend_type="iceberg",
    operation_type=OperationType.COMPACTION,
    target_table="iceberg.market.curve_observation",
    dry_run=True,
    parameters={"target_file_size_mb": 256}
)

print(f"Operation {result.operation_id} status: {result.status}")
print(f"Files rewritten: {result.metrics.get('files_rewritten', 0)}")
```

### Batch Operations

```python
# Execute multiple operations
operations = [
    {
        "backend_type": "iceberg",
        "operation_type": "compaction", 
        "target_table": "iceberg.market.curve_observation",
        "parameters": {"target_file_size_mb": 256}
    },
    {
        "backend_type": "iceberg",
        "operation_type": "retention",
        "target_table": "iceberg.market.curve_observation", 
        "parameters": {"retention_days": 30}
    }
]

results = await service.execute_batch_maintenance(operations)
for result in results:
    print(f"Table: {result.config.target_table}, Status: {result.status}")
```

### Integration with Existing Code

The refactored system is designed to integrate seamlessly with existing maintenance functions:

```python
# The IcebergMaintenanceBackend bridges to existing functions
from aurum.maintenance.backends import IcebergMaintenanceBackend

backend = IcebergMaintenanceBackend({
    "host": "localhost",
    "port": 8080,
    "user": "aurum"
})

await backend.connect()

# This calls the existing aurum.iceberg.maintenance.rewrite_data_files
result = await backend.execute_iceberg_operation(
    "rewrite_data_files",
    "iceberg.market.curve_observation",
    target_file_size_mb=256,
    dry_run=True
)
```

## Migration Guide

### From Old Pattern

**Before:**
```python
from aurum.iceberg.maintenance import rewrite_data_files

result = rewrite_data_files(
    "iceberg.market.curve_observation",
    target_file_size_mb=256,
    dry_run=True
)
```

**After:**
```python
from aurum.maintenance import MaintenanceService, OperationType

service = MaintenanceService()
result = await service.execute_maintenance(
    backend_type="iceberg",
    operation_type=OperationType.COMPACTION,
    target_table="iceberg.market.curve_observation",
    dry_run=True,
    parameters={"target_file_size_mb": 256}
)
```

### Benefits of Migration

1. **Unified Interface** - Same pattern for all backends and operations
2. **Better Testing** - Clear interfaces enable easy mocking
3. **Async Support** - Native async/await throughout
4. **Resource Management** - Proper connection lifecycle management
5. **Extensibility** - Easy to add new backends and operations
6. **Observability** - Standardized metrics and logging

## Configuration

The service uses `AurumSettings` for configuration, following established patterns:

```python
# Configuration via environment variables
AURUM_DATA_BACKEND_TRINO_HOST=localhost
AURUM_DATA_BACKEND_TRINO_PORT=8080
AURUM_DATA_BACKEND_ICEBERG_CATALOG=iceberg

# Or programmatically
from aurum.core import AurumSettings

settings = AurumSettings(
    data_backend=DataBackendSettings(
        trino_host="localhost",
        trino_port=8080,
        iceberg_catalog="iceberg"
    )
)

service = MaintenanceService(settings=settings)
```

## Testing

The refactored system is designed for testability:

```python
import pytest
from unittest.mock import AsyncMock
from aurum.maintenance import MaintenanceFactory, OperationType

@pytest.mark.asyncio
async def test_compaction_operation():
    # Create components
    operation = MaintenanceFactory.create_operation(OperationType.COMPACTION)
    backend = AsyncMock()
    backend.backend_type = "iceberg"
    backend.health_check.return_value = True
    
    # Mock backend response
    backend.execute_iceberg_operation.return_value = {
        "rewritten_files": 5,
        "added_files": 3
    }
    
    # Execute and verify
    config = MaintenanceConfig(
        operation_type=OperationType.COMPACTION,
        target_table="test_table"
    )
    
    result = await operation.execute(backend, config)
    assert result.status == MaintenanceStatus.COMPLETED
```

## Extension Points

### Adding New Operations

```python
from aurum.maintenance.operations.base import BaseMaintenanceOperation
from aurum.maintenance.interfaces import OperationType

class CustomOperation(BaseMaintenanceOperation):
    def __init__(self):
        super().__init__(OperationType.CUSTOM)
    
    async def _execute_impl(self, backend, config):
        # Implementation here
        return {"custom_metric": 42}

# Register the operation
MaintenanceFactory.register_operation(OperationType.CUSTOM, CustomOperation)
```

### Adding New Backends

```python
from aurum.maintenance.backends.base import BaseMaintenanceBackend

class CustomBackend(BaseMaintenanceBackend):
    @property
    def backend_type(self):
        return "custom"
    
    @property 
    def supported_operations(self):
        return [OperationType.COMPACTION]
    
    async def _create_connection(self):
        # Implementation here
        return custom_connection
    
    # Implement other abstract methods...

# Register the backend
MaintenanceFactory.register_backend("custom", CustomBackend)
```

## Integration with Existing Infrastructure

The refactored system integrates with existing Aurum infrastructure:

- **Metrics**: Uses existing `aurum.logging.metrics` patterns
- **Settings**: Leverages `AurumSettings` configuration system  
- **Async Patterns**: Follows established async/await conventions
- **Error Handling**: Consistent with existing error handling patterns
- **Resource Management**: Proper cleanup following Aurum patterns

This ensures a smooth transition while providing the benefits of the improved architecture.