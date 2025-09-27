from __future__ import annotations

"""Drought domain service with DAO pattern implementation."""

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from .base_service import QueryableServiceInterface, DimensionalServiceInterface
from ..dao.drought_dao import DroughtDao


class DroughtService(QueryableServiceInterface, DimensionalServiceInterface):
    """Drought domain service implementing business logic and data access through DAO.
    
    Implements Phase 1.3 service layer decomposition with clear domain boundaries.
    """
    
    def __init__(self):
        self._dao = DroughtDao()
    
    # Legacy v2 service compatibility methods
    async def list_indices(
        self,
        *,
        dataset: Optional[str],
        index: Optional[str],
        timescale: Optional[str],
        region: Optional[str],
        region_type: Optional[str],
        region_id: Optional[str],
        start: Optional[str],
        end: Optional[str],
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        from aurum.api.drought_v2_service import DroughtV2Service

        return await DroughtV2Service().list_indices(
            dataset=dataset,
            index=index,
            timescale=timescale,
            region=region,
            region_type=region_type,
            region_id=region_id,
            start=start,
            end=end,
            offset=offset,
            limit=limit,
        )

    async def list_usdm(
        self,
        *,
        region_type: Optional[str],
        region_id: Optional[str],
        start: Optional[str],
        end: Optional[str],
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        from aurum.api.drought_v2_service import DroughtV2Service

        return await DroughtV2Service().list_usdm(
            region_type=region_type,
            region_id=region_id,
            start=start,
            end=end,
            offset=offset,
            limit=limit,
        )
    
    # New DAO-based methods that replace service.py functions
    def query_drought_indices(
        self,
        *,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        dataset: Optional[str] = None,
        index_id: Optional[str] = None,
        timescale: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query drought indices data."""
        return self._dao.query_drought_indices(
            region_type=region_type,
            region_id=region_id,
            dataset=dataset,
            index_id=index_id,
            timescale=timescale,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    
    def query_drought_usdm(
        self,
        *,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query USDM (US Drought Monitor) data."""
        return self._dao.query_drought_usdm(
            region_type=region_type,
            region_id=region_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    
    def query_drought_vector_events(
        self,
        *,
        layer: Optional[str] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query drought vector events data."""
        return self._dao.query_drought_vector_events(
            layer=layer,
            region_type=region_type,
            region_id=region_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
    
    # ServiceInterface implementation
    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate drought-specific caches."""
        # Placeholder - implement based on specific caching patterns
        return {"drought": 0}
    
    # QueryableServiceInterface implementation
    async def query_data(
        self, 
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query drought data with pagination and filtering."""
        filters = filters or {}
        data, _ = self.query_drought_indices(
            region_type=filters.get("region_type"),
            region_id=filters.get("region_id"),
            dataset=filters.get("dataset"),
            index_id=filters.get("index_id"),
            timescale=filters.get("timescale"),
            limit=limit,
        )
        return data[offset:offset + limit]
    
    # DimensionalServiceInterface implementation
    async def get_dimensions(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[str]]:
        """Get available dimensions for drought data filtering."""
        # Placeholder - would query available datasets, indices, timescales
        return {
            "dataset": ["USDM", "SPI", "SPEI", "PDSI"],
            "index": ["SPI", "SPEI", "PDSI", "SCPDSI"],
            "timescale": ["1", "3", "6", "9", "12", "24"],
            "region_type": ["state", "county", "climate_division"],
        }
