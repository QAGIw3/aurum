from __future__ import annotations

"""Metadata domain service with DAO pattern implementation.

Phase 1.3 Service Layer Decomposition: Handles metadata, dimensions, units, and calendar data.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple

from .base_service import DimensionalServiceInterface, QueryableServiceInterface
from ..dao.metadata_dao import MetadataDao


class MetadataService(DimensionalServiceInterface, QueryableServiceInterface):
    """Metadata domain service implementing business logic and data access through DAO.
    
    Handles dimensions, units, calendars, and ISO locations with comprehensive caching.
    """
    
    def __init__(self):
        self._dao = MetadataDao()

    async def list_dimensions(
        self, 
        *, 
        asof: str | None, 
        offset: int, 
        limit: int
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List dimensions with pagination (legacy method)."""
        metadata_dims = await self._dao.fetch_metadata_dimensions()
        
        # Apply offset and limit
        paginated = metadata_dims[offset:offset + limit]
        return paginated, len(metadata_dims)

    async def list_locations(
        self, 
        *, 
        iso: str, 
        offset: int, 
        limit: int
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List locations for ISO with pagination (legacy method)."""
        locations = await self._dao.fetch_iso_locations(iso=iso)
        
        # Apply offset and limit  
        paginated = locations[offset:offset + limit]
        return paginated, len(locations)

    async def list_units(
        self, 
        *, 
        offset: int, 
        limit: int
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List units with pagination (legacy method)."""
        units = await self._dao.fetch_units_canonical()
        
        # Apply offset and limit
        paginated = units[offset:offset + limit]
        return paginated, len(units)

    async def list_calendars(
        self, 
        *, 
        offset: int, 
        limit: int
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List calendars with pagination (legacy method)."""
        calendars = await self._dao.fetch_calendars()
        
        # Apply offset and limit
        paginated = calendars[offset:offset + limit]
        return paginated, len(calendars)

    async def query_data(
        self, 
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query metadata with pagination and filtering (QueryableServiceInterface)."""
        if filters is None:
            filters = {}
            
        # Default to dimensions query
        metadata_dims = await self._dao.fetch_metadata_dimensions(
            domain=filters.get("domain"),
            filters=filters,
        )
        
        return metadata_dims[offset:offset + limit]

    async def get_dimensions(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[str]]:
        """Get available dimensions for filtering (DimensionalServiceInterface)."""
        if filters is None:
            filters = {}
            
        return await self._dao.query_dimensions(
            dimension_type=filters.get("dimension_type"),
            filters=filters,
        )

    async def query_dimensions(
        self,
        *,
        dimension_type: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, List[str]]:
        """Query dimensions (legacy method name for compatibility)."""
        return await self._dao.query_dimensions(
            dimension_type=dimension_type,
            filters=filters,
        )

    async def fetch_metadata_dimensions(
        self,
        *,
        domain: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch metadata dimensions with detailed information."""
        return await self._dao.fetch_metadata_dimensions(
            domain=domain,
            filters=filters,
        )

    async def fetch_iso_locations(
        self, 
        *, 
        iso: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetch ISO locations with metadata."""
        return await self._dao.fetch_iso_locations(iso=iso)

    async def fetch_units_canonical(self) -> List[Dict[str, Any]]:
        """Fetch canonical units mapping."""
        return await self._dao.fetch_units_canonical()

    async def fetch_units_mapping(
        self, 
        *, 
        prefix: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetch units mapping with optional prefix filtering."""
        return await self._dao.fetch_units_mapping(prefix=prefix)

    async def fetch_calendars(self) -> List[Dict[str, Any]]:
        """Fetch available calendars."""
        return await self._dao.fetch_calendars()

    async def fetch_calendar_blocks(self, *, name: str) -> List[Dict[str, Any]]:
        """Fetch blocks for a specific calendar."""
        return await self._dao.fetch_calendar_blocks(name=name)

    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate metadata-related caches (ServiceInterface)."""
        return await self._dao.invalidate_metadata_cache()

    async def invalidate_metadata_cache(
        self, 
        *, 
        prefixes: Optional[Sequence[str]] = None
    ) -> Dict[str, int]:
        """Invalidate metadata caches with specific prefixes."""
        return await self._dao.invalidate_metadata_cache(prefixes=prefixes)

    async def invalidate_dimensions_cache(self) -> Dict[str, int]:
        """Invalidate dimensions-specific cache."""
        return await self._dao.invalidate_dimensions_cache()
