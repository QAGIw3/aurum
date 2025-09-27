from __future__ import annotations

"""Curves domain service with DAO pattern implementation.

Phase 1.3 Service Layer Decomposition: Extracted from monolithic service.py
Provides clean business logic layer with data access through DAO pattern.
"""

from typing import Any, Dict, List, Optional, AsyncGenerator

from .base_service import QueryableServiceInterface, ExportableServiceInterface
from ..dao.curves_dao import CurvesDao


class CurvesService(QueryableServiceInterface, ExportableServiceInterface):
    """Curves domain service implementing business logic and data access through DAO.
    
    Handles curve observations, diffs, and strips with caching and export capabilities.
    """
    
    def __init__(self):
        self._dao = CurvesDao()

    async def list_curves(
        self, 
        *, 
        offset: int, 
        limit: int, 
        name_filter: Optional[str] = None
    ) -> List[Any]:
        """List curves with optional name filtering."""
        # For backward compatibility, delegate to query_data
        filters = {"iso": name_filter} if name_filter else None
        return await self.query_data(offset=offset, limit=limit, filters=filters)

    async def get_curve_diff(
        self, 
        *, 
        curve_id: str, 
        from_timestamp: str, 
        to_timestamp: str
    ) -> Any:
        """Get curve diff between two timestamps."""
        # Parse curve_id for filtering (simplified implementation)
        filters = {"iso": curve_id.split(":")[0] if ":" in curve_id else None}
        
        results = await self._dao.query_curves_diff(
            iso=filters.get("iso"),
            from_asof=from_timestamp,
            to_asof=to_timestamp,
            limit=1000
        )
        
        return {"diff_data": results, "from_asof": from_timestamp, "to_asof": to_timestamp}

    async def query_data(
        self, 
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query curve data with pagination and filtering (QueryableServiceInterface)."""
        if filters is None:
            filters = {}
            
        return await self._dao.query_curves(
            iso=filters.get("iso"),
            market=filters.get("market"),
            location=filters.get("location"),
            product=filters.get("product"),
            block=filters.get("block"),
            asof=filters.get("asof"),
            strip=filters.get("strip"),
            offset=offset,
            limit=limit,
        )

    async def export_data(
        self,
        *,
        format: str = "json",
        filters: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Export curve data in specified format (ExportableServiceInterface)."""
        if filters is None:
            filters = {}
        
        offset = 0
        
        while True:
            chunk = await self._dao.query_curves(
                iso=filters.get("iso"),
                market=filters.get("market"),
                location=filters.get("location"),
                product=filters.get("product"),
                block=filters.get("block"),
                asof=filters.get("asof"),
                strip=filters.get("strip"),
                offset=offset,
                limit=chunk_size,
            )
            
            if not chunk:
                break
                
            if format == "json":
                for row in chunk:
                    yield row
            else:
                # For other formats, yield as batch
                yield {"format": format, "data": chunk, "offset": offset}
            
            offset += chunk_size
            
            # Safety break for large datasets
            if len(chunk) < chunk_size:
                break

    async def stream_curve_export(
        self,
        *,
        asof: Optional[str] = None,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        chunk_size: int = 1000,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Async generator yielding export rows for curves (legacy method)."""
        filters = {
            "asof": asof,
            "iso": iso,
            "market": market,
            "location": location,
            "product": product,
            "block": block,
        }
        
        async for item in self.export_data(filters=filters, chunk_size=chunk_size):
            yield item

    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate curve-related caches (ServiceInterface)."""
        return await self._dao.invalidate_curve_cache()
