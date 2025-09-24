"""ClickHouse maintenance backend (placeholder)."""

from __future__ import annotations

from typing import Any, Dict, List

from .base import BaseMaintenanceBackend
from ..interfaces import OperationType


class ClickHouseMaintenanceBackend(BaseMaintenanceBackend):
    """ClickHouse maintenance backend."""
    
    @property
    def backend_type(self) -> str:
        return "clickhouse"
    
    @property
    def supported_operations(self) -> List[OperationType]:
        return [
            OperationType.COMPACTION,
            OperationType.RETENTION,
        ]
    
    async def _create_connection(self) -> Any:
        """Create connection to ClickHouse."""
        # Placeholder - would use clickhouse-driver
        raise NotImplementedError("ClickHouse backend not yet implemented")
    
    async def _close_connection(self, connection: Any) -> None:
        """Close ClickHouse connection."""
        pass
    
    async def _health_check_impl(self, connection: Any) -> bool:
        """Health check for ClickHouse."""
        return False
    
    async def _get_table_metadata_impl(
        self, connection: Any, table_name: str
    ) -> Dict[str, Any]:
        """Get ClickHouse table metadata."""
        return {"table_name": table_name, "backend": "clickhouse"}