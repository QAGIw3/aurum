from __future__ import annotations

"""ISO domain service façade -> forwards to old service implementation for now."""

from typing import Any, Dict, List, Optional, Tuple


class IsoService:
    async def lmp_last_24h(
        self, 
        *, 
        iso_code: Optional[str] = None,
        market: Optional[str] = None, 
        location_id: Optional[str] = None,
        limit: int = 500
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Get last 24h LMP data - delegates to old service for now."""
        # Import here to avoid circular import
        from ..service import query_iso_lmp_last_24h
        from ..config import CacheConfig
        from ..state import get_settings
        
        cache_cfg = CacheConfig.from_settings(get_settings())
        return query_iso_lmp_last_24h(
            iso_code=iso_code,
            market=market,
            location_id=location_id,
            limit=limit,
            cache_cfg=cache_cfg,
        )

    async def lmp_hourly(
        self, 
        *, 
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        date: Optional[str] = None,
        limit: int = 500
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Get hourly LMP data - delegates to old service for now."""
        from ..service import query_iso_lmp_hourly
        from ..config import CacheConfig
        from ..state import get_settings
        
        cache_cfg = CacheConfig.from_settings(get_settings())
        return query_iso_lmp_hourly(
            iso_code=iso_code,
            market=market,
            location_id=location_id,
            date=date,
            limit=limit,
            cache_cfg=cache_cfg,
        )

    async def lmp_daily(
        self, 
        *, 
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        date: Optional[str] = None,
        limit: int = 500
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Get daily LMP data - delegates to old service for now."""
        from ..service import query_iso_lmp_daily
        from ..config import CacheConfig
        from ..state import get_settings
        
        cache_cfg = CacheConfig.from_settings(get_settings())
        return query_iso_lmp_daily(
            iso_code=iso_code,
            market=market,
            location_id=location_id,
            date=date,
            limit=limit,
            cache_cfg=cache_cfg,
        )

    async def lmp_negative(
        self, 
        *, 
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        limit: int = 200
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Get negative LMP data - delegates to old service for now."""
        from ..service import query_iso_lmp_negative
        from ..config import CacheConfig
        from ..state import get_settings
        
        cache_cfg = CacheConfig.from_settings(get_settings())
        return query_iso_lmp_negative(
            iso_code=iso_code,
            market=market,
            limit=limit,
            cache_cfg=cache_cfg,
        )

