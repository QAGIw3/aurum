"""Base service interface contracts for domain services."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class ServiceInterface(ABC):
    """Base interface for all domain services.
    
    This interface defines common operations that all domain services should support.
    It follows the principles outlined in Phase 1.3 of the development roadmap.
    """
    
    @abstractmethod
    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate domain-specific caches.
        
        Returns:
            Dict mapping cache patterns to number of keys deleted
        """
        pass


class QueryableServiceInterface(ServiceInterface):
    """Interface for services that support querying operations."""
    
    @abstractmethod
    async def query_data(
        self, 
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query domain data with pagination and filtering.
        
        Args:
            offset: Number of records to skip
            limit: Maximum number of records to return
            filters: Optional filtering parameters
            
        Returns:
            List of domain data records
        """
        pass


class DimensionalServiceInterface(ServiceInterface):
    """Interface for services that support dimensional queries."""
    
    @abstractmethod
    async def get_dimensions(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[str]]:
        """Get available dimensions for filtering.
        
        Args:
            filters: Optional filters to scope dimensions
            
        Returns:
            Dict mapping dimension names to available values
        """
        pass


class ExportableServiceInterface(ServiceInterface):
    """Interface for services that support data export."""
    
    @abstractmethod
    async def export_data(
        self,
        *,
        format: str = "json",
        filters: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ):
        """Export domain data in specified format.
        
        Args:
            format: Export format (json, csv, parquet)
            filters: Optional filtering parameters
            chunk_size: Size of export chunks for streaming
            
        Yields:
            Data chunks in specified format
        """
        pass