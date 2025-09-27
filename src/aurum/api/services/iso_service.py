from __future__ import annotations

"""ISO domain service with DAO pattern implementation."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .base_service import QueryableServiceInterface, DimensionalServiceInterface
from ..dao.iso_dao import IsoDao


class IsoService(QueryableServiceInterface, DimensionalServiceInterface):
    """ISO domain service implementing business logic and data access through DAO.
    
    Implements Phase 1.3 service layer decomposition with clear domain boundaries.
    """
    
    def __init__(self):
        self._dao = IsoDao()
    
    # Legacy v2 service compatibility methods
    async def lmp_last_24h(self, *, iso: str, offset: int, limit: int) -> List[Dict[str, Any]]:
        from aurum.api.iso_v2_service import IsoV2Service
        return await IsoV2Service().lmp_last_24h(iso=iso, offset=offset, limit=limit)

    async def lmp_hourly(self, *, iso: str, date: str, offset: int, limit: int) -> List[Dict[str, Any]]:
        from aurum.api.iso_v2_service import IsoV2Service
        return await IsoV2Service().lmp_hourly(iso=iso, date=date, offset=offset, limit=limit)

    async def lmp_daily(self, *, iso: str, date: str, offset: int, limit: int) -> List[Dict[str, Any]]:
        from aurum.api.iso_v2_service import IsoV2Service
        return await IsoV2Service().lmp_daily(iso=iso, date=date, offset=offset, limit=limit)
    
    # New DAO-based methods that replace service.py functions
    def query_lmp_last_24h(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query LMP data for the last 24 hours."""
        return self._dao.query_lmp_last_24h(
            iso_code=iso_code,
            market=market,
            location_id=location_id,
            limit=limit,
        )
    
    def query_lmp_hourly(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query hourly LMP data."""
        return self._dao.query_lmp_hourly(
            iso_code=iso_code,
            market=market,
            location_id=location_id,
            start=start,
            end=end,
            limit=limit,
        )
    
    def query_lmp_daily(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query daily LMP data."""
        return self._dao.query_lmp_daily(
            iso_code=iso_code,
            market=market,
            location_id=location_id,
            start=start,
            end=end,
            limit=limit,
        )
    
    def query_lmp_negative(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        limit: int = 200,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query negative LMP data from the last 7 days."""
        return self._dao.query_lmp_negative(
            iso_code=iso_code,
            market=market,
            limit=limit,
        )
    
    # ServiceInterface implementation
    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate ISO-specific caches."""
        # Placeholder - implement based on specific caching patterns
        return {"iso_lmp": 0}
    
    # QueryableServiceInterface implementation
    async def query_data(
        self, 
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query ISO data with pagination and filtering."""
        filters = filters or {}
        data, _ = self.query_lmp_last_24h(
            iso_code=filters.get("iso_code"),
            market=filters.get("market"),
            location_id=filters.get("location_id"),
            limit=limit,
        )
        return data[offset:offset + limit]
    
    # DimensionalServiceInterface implementation
    async def get_dimensions(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[str]]:
        """Get available dimensions for ISO data filtering."""
        # Placeholder - would query available ISOs, markets, locations
        return {
            "iso_code": ["CAISO", "ERCOT", "NYISO", "PJM", "NEISO", "MISO", "SPP"],
            "market": ["DAM", "RTM", "HAM"],
            "location_type": ["NODE", "ZONE", "HUB"],
        }

