"""Base classes for maintenance backends."""

from __future__ import annotations

import abc
import logging
from typing import Any, Dict, List, Optional
from contextlib import AsyncExitStack

from ..interfaces import MaintenanceBackend, OperationType

LOGGER = logging.getLogger(__name__)


class BaseMaintenanceBackend(MaintenanceBackend):
    """Base implementation for maintenance backends."""
    
    def __init__(self, connection_config: Dict[str, Any]) -> None:
        self._connection_config = connection_config
        self._connection: Optional[Any] = None
        self._is_connected = False
        self._exit_stack = AsyncExitStack()
    
    @property
    @abc.abstractmethod
    def backend_type(self) -> str:
        """Get the backend type identifier."""
        ...
    
    @property
    @abc.abstractmethod  
    def supported_operations(self) -> List[OperationType]:
        """Get list of supported operation types."""
        ...
    
    @abc.abstractmethod
    async def _create_connection(self) -> Any:
        """Create a connection to the backend."""
        ...
    
    @abc.abstractmethod
    async def _close_connection(self, connection: Any) -> None:
        """Close a connection to the backend.""" 
        ...
    
    @abc.abstractmethod
    async def _health_check_impl(self, connection: Any) -> bool:
        """Implementation-specific health check."""
        ...
    
    @abc.abstractmethod
    async def _get_table_metadata_impl(
        self, 
        connection: Any, 
        table_name: str
    ) -> Dict[str, Any]:
        """Implementation-specific table metadata retrieval."""
        ...
    
    async def connect(self) -> None:
        """Establish connection to the backend."""
        if self._is_connected:
            return
            
        try:
            LOGGER.debug("Connecting to %s backend", self.backend_type)
            self._connection = await self._create_connection()
            self._is_connected = True
            LOGGER.info("Successfully connected to %s backend", self.backend_type)
        except Exception as e:
            LOGGER.error("Failed to connect to %s backend: %s", self.backend_type, e)
            raise
    
    async def disconnect(self) -> None:
        """Close connection to the backend."""
        if not self._is_connected or self._connection is None:
            return
            
        try:
            LOGGER.debug("Disconnecting from %s backend", self.backend_type)
            await self._close_connection(self._connection)
            await self._exit_stack.aclose()
            self._connection = None
            self._is_connected = False
            LOGGER.info("Successfully disconnected from %s backend", self.backend_type)
        except Exception as e:
            LOGGER.warning("Error during disconnect from %s backend: %s", self.backend_type, e)
    
    async def health_check(self) -> bool:
        """Check if backend is healthy and accessible."""
        if not self._is_connected or self._connection is None:
            return False
            
        try:
            return await self._health_check_impl(self._connection)
        except Exception as e:
            LOGGER.warning("Health check failed for %s backend: %s", self.backend_type, e)
            return False
    
    async def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Get metadata for a table."""
        if not self._is_connected or self._connection is None:
            raise RuntimeError(f"Not connected to {self.backend_type} backend")
            
        try:
            return await self._get_table_metadata_impl(self._connection, table_name)
        except Exception as e:
            LOGGER.error(
                "Failed to get metadata for table %s from %s backend: %s", 
                table_name, self.backend_type, e
            )
            raise
    
    def _ensure_connected(self) -> Any:
        """Ensure connection is established and return it."""
        if not self._is_connected or self._connection is None:
            raise RuntimeError(f"Not connected to {self.backend_type} backend")
        return self._connection