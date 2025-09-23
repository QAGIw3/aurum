"""Iceberg maintenance backend implementation.

This backend integrates with the existing Iceberg maintenance functions
while providing the new unified interface.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List

from .base import BaseMaintenanceBackend
from ..interfaces import OperationType

# Import existing maintenance functions
from aurum.iceberg.maintenance import _load_catalog_table

try:
    from aurum.api.database.trino_client import get_trino_client
except ImportError:
    # Fallback if trino client not available
    get_trino_client = None

LOGGER = logging.getLogger(__name__)


class IcebergMaintenanceBackend(BaseMaintenanceBackend):
    """Iceberg maintenance backend using Trino."""
    
    @property
    def backend_type(self) -> str:
        """Get the backend type identifier."""
        return "iceberg"
    
    @property
    def supported_operations(self) -> List[OperationType]:
        """Get list of supported operation types."""
        return [
            OperationType.COMPACTION,
            OperationType.RETENTION,
            OperationType.PARTITION_EVOLUTION,
            OperationType.ZORDER_OPTIMIZATION,
            OperationType.VACUUM,
            OperationType.REWRITE_MANIFESTS,
            OperationType.EXPIRE_SNAPSHOTS,
            OperationType.REMOVE_ORPHANS,
        ]
    
    async def _create_connection(self) -> Any:
        """Create a connection to the Iceberg backend via Trino."""
        if get_trino_client is None:
            raise RuntimeError("Trino client not available")
        
        # Use existing Trino client setup
        connection_kwargs = self._connection_config.copy()
        
        # Run in executor since trino client might be synchronous
        loop = asyncio.get_event_loop()
        client = await loop.run_in_executor(
            None, 
            lambda: get_trino_client(**connection_kwargs)
        )
        
        return client
    
    async def _close_connection(self, connection: Any) -> None:
        """Close connection to the Iceberg backend."""
        # Close the connection if it has a close method
        if hasattr(connection, 'close'):
            if asyncio.iscoroutinefunction(connection.close):
                await connection.close()
            else:
                # Run synchronous close in executor
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, connection.close)
    
    async def _health_check_impl(self, connection: Any) -> bool:
        """Implementation-specific health check for Iceberg."""
        try:
            # Try to execute a simple query to verify connection
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: connection.execute("SELECT 1")
            )
            # Check if we got a result
            return result is not None
        except Exception as e:
            LOGGER.warning("Iceberg health check failed: %s", e)
            return False
    
    async def _get_table_metadata_impl(
        self, 
        connection: Any, 
        table_name: str
    ) -> Dict[str, Any]:
        """Get metadata for an Iceberg table."""
        try:
            # Use existing table loading function in executor
            loop = asyncio.get_event_loop()
            table = await loop.run_in_executor(
                None,
                lambda: _load_catalog_table(table_name)
            )
            
            # Extract basic metadata
            metadata = {
                "table_name": table_name,
                "location": getattr(table, 'location', None),
                "metadata_location": getattr(table, 'metadata_location', None),
                "current_snapshot_id": None,
                "file_count": 0,
                "partition_spec": {},
                "schema": {},
            }
            
            # Get current snapshot info
            if hasattr(table, 'current_snapshot') and table.current_snapshot:
                metadata["current_snapshot_id"] = table.current_snapshot.snapshot_id
            
            # Get partition spec info
            if hasattr(table, 'spec'):
                spec = table.spec
                metadata["partition_spec"] = {
                    "spec_id": getattr(spec, 'spec_id', None),
                    "fields": [
                        {
                            "name": field.get('name', ''),
                            "transform": str(field.get('transform', '')),
                            "source_id": field.get('source_id', None),
                            "field_id": field.get('field_id', None),
                        }
                        for field in spec.get('fields', [])
                    ] if hasattr(spec, 'get') else []
                }
            
            # Get basic schema info
            if hasattr(table, 'schema'):
                schema = table.schema
                metadata["schema"] = {
                    "schema_id": getattr(schema, 'schema_id', None),
                    "fields": [
                        {
                            "id": field.get('id', None),
                            "name": field.get('name', ''),
                            "type": str(field.get('type', '')),
                            "required": field.get('required', False),
                        }
                        for field in schema.get('fields', [])
                    ] if hasattr(schema, 'get') else []
                }
            
            # Get file count from manifests
            try:
                manifests = list(getattr(table, 'manifests', lambda: [])())
                file_count = 0
                for manifest in manifests:
                    manifest_files = getattr(manifest, 'entries', [])
                    file_count += len(manifest_files)
                metadata["file_count"] = file_count
            except Exception as e:
                LOGGER.warning("Could not get file count for table %s: %s", table_name, e)
                metadata["file_count"] = 0
            
            return metadata
            
        except Exception as e:
            LOGGER.error("Failed to get metadata for Iceberg table %s: %s", table_name, e)
            raise
    
    async def execute_iceberg_operation(
        self,
        operation_name: str,
        table_name: str,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """Execute an Iceberg-specific operation using existing functions.
        
        This method bridges the new interface with existing maintenance functions.
        """
        connection = self._ensure_connected()
        
        # Import the specific operation function
        from aurum.iceberg import maintenance
        
        operation_func = getattr(maintenance, operation_name, None)
        if operation_func is None:
            raise ValueError(f"Unknown Iceberg operation: {operation_name}")
        
        # Execute the operation in an executor
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: operation_func(table_name, **kwargs)
        )
        
        return result