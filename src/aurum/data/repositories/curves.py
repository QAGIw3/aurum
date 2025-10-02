"""Curve repository for market curve operations.

Provides domain-specific operations for curve data.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import date, datetime

from .base import BaseRepository
from ..dao import TrinoDAO

logger = logging.getLogger(__name__)


class CurveRepository(BaseRepository):
    """Repository for curve data operations.
    
    Curves represent market data points (prices, forecasts, etc.)
    across time intervals and locations.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._trino_dao: Optional[TrinoDAO] = None
    
    async def initialize(self) -> None:
        """Initialize repository and its DAOs."""
        self._trino_dao = TrinoDAO(self.settings)
        await self._trino_dao.initialize()
    
    async def close(self) -> None:
        """Close repository and its DAOs."""
        if self._trino_dao:
            await self._trino_dao.close()
    
    async def __aenter__(self) -> CurveRepository:
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
    
    async def find_by_key(
        self,
        curve_key: str,
        asof: Optional[date] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Find curves by curve key.
        
        Args:
            curve_key: Unique curve identifier
            asof: As-of date for point-in-time query
            limit: Maximum number of results
            
        Returns:
            List of curve data points
        """
        query = """
            SELECT *
            FROM iceberg.market.curve_observation
            WHERE curve_key = :curve_key
        """
        
        params = {"curve_key": curve_key, "limit": limit}
        
        if asof:
            query += " AND asof_date = :asof"
            params["asof"] = asof.isoformat()
        
        query += " ORDER BY interval_start LIMIT :limit"
        
        return await self._trino_dao.execute_query(query, params)
    
    async def find_by_filters(
        self,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        asof: Optional[date] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Find curves by various filters.
        
        Args:
            iso: ISO/RTO identifier (e.g., "PJM", "ERCOT")
            market: Market type (e.g., "DA", "RT")
            location: Location/node identifier
            product: Product type
            asof: As-of date
            limit: Maximum number of results
            offset: Pagination offset
            
        Returns:
            List of curve data points
        """
        query = "SELECT * FROM iceberg.market.curve_observation WHERE 1=1"
        params: Dict[str, Any] = {}
        
        if iso:
            query += " AND iso = :iso"
            params["iso"] = iso
        
        if market:
            query += " AND market = :market"
            params["market"] = market
        
        if location:
            query += " AND location = :location"
            params["location"] = location
        
        if product:
            query += " AND product = :product"
            params["product"] = product
        
        if asof:
            query += " AND asof_date = :asof"
            params["asof"] = asof.isoformat()
        
        query += " ORDER BY asof_date DESC, interval_start LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        
        return await self._trino_dao.execute_query(query, params)
    
    async def get_latest_asof(
        self,
        curve_key: Optional[str] = None,
        iso: Optional[str] = None
    ) -> Optional[date]:
        """Get the latest as-of date for curves.
        
        Args:
            curve_key: Filter by specific curve
            iso: Filter by ISO
            
        Returns:
            Latest as-of date or None
        """
        query = "SELECT MAX(asof_date) as latest FROM iceberg.market.curve_observation WHERE 1=1"
        params: Dict[str, Any] = {}
        
        if curve_key:
            query += " AND curve_key = :curve_key"
            params["curve_key"] = curve_key
        
        if iso:
            query += " AND iso = :iso"
            params["iso"] = iso
        
        result = await self._trino_dao.execute_query_single(query, params)
        
        if result and result.get("latest"):
            return result["latest"]
        
        return None

