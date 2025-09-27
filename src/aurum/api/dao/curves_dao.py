"""Curves Data Access Object - handles curve data operations."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from aurum.core import AurumSettings
from ..config import CacheConfig, TrinoConfig
from ..state import get_settings

LOGGER = logging.getLogger(__name__)


class CurvesDao:
    """Data Access Object for curves operations."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        self._settings = settings or get_settings()
        self._trino_config = TrinoConfig.from_settings(self._settings)
        self._cache_config = CacheConfig.from_settings(self._settings)
    
    def _build_curve_query(
        self,
        *,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        asof: Optional[str] = None,
        strip: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[str, Dict[str, Any]]:
        """Build SQL query for curve data."""
        
        query = """
        SELECT 
            iso,
            market,
            location,
            product,
            block,
            strip,
            asof,
            forward_date,
            forward_value,
            created_at,
            updated_at
        FROM iceberg.market.curve_observation
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        # Add filters
        if iso:
            query += " AND iso = %(iso)s"
            params["iso"] = iso
            
        if market:
            query += " AND market = %(market)s"
            params["market"] = market
            
        if location:
            query += " AND location = %(location)s"
            params["location"] = location
            
        if product:
            query += " AND product = %(product)s"
            params["product"] = product
            
        if block:
            query += " AND block = %(block)s"
            params["block"] = block
            
        if asof:
            query += " AND asof = %(asof)s"
            params["asof"] = asof
            
        if strip:
            query += " AND strip = %(strip)s"
            params["strip"] = strip
        
        # Add ordering and pagination
        query += " ORDER BY iso, market, location, product, asof, forward_date"
        query += " OFFSET %(offset)s LIMIT %(limit)s"
        params["offset"] = offset
        params["limit"] = limit
        
        return query, params
    
    def _build_curve_diff_query(
        self,
        *,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        from_asof: str,
        to_asof: str,
        strip: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[str, Dict[str, Any]]:
        """Build SQL query for curve diff data."""
        
        query = """
        WITH from_curves AS (
            SELECT 
                iso, market, location, product, block, strip,
                forward_date, forward_value as from_value
            FROM iceberg.market.curve_observation
            WHERE asof = %(from_asof)s
        ),
        to_curves AS (
            SELECT 
                iso, market, location, product, block, strip,
                forward_date, forward_value as to_value
            FROM iceberg.market.curve_observation
            WHERE asof = %(to_asof)s
        )
        SELECT 
            COALESCE(f.iso, t.iso) as iso,
            COALESCE(f.market, t.market) as market,
            COALESCE(f.location, t.location) as location,
            COALESCE(f.product, t.product) as product,
            COALESCE(f.block, t.block) as block,
            COALESCE(f.strip, t.strip) as strip,
            COALESCE(f.forward_date, t.forward_date) as forward_date,
            f.from_value,
            t.to_value,
            (t.to_value - f.from_value) as diff_value,
            CASE 
                WHEN f.from_value IS NULL OR f.from_value = 0 THEN NULL
                ELSE ((t.to_value - f.from_value) / f.from_value) * 100
            END as pct_change
        FROM from_curves f
        FULL OUTER JOIN to_curves t ON (
            f.iso = t.iso AND
            f.market = t.market AND
            f.location = t.location AND
            f.product = t.product AND
            f.block = t.block AND
            f.strip = t.strip AND
            f.forward_date = t.forward_date
        )
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {
            "from_asof": from_asof,
            "to_asof": to_asof,
        }
        
        # Add filters
        if iso:
            query += " AND COALESCE(f.iso, t.iso) = %(iso)s"
            params["iso"] = iso
            
        if market:
            query += " AND COALESCE(f.market, t.market) = %(market)s"
            params["market"] = market
            
        if location:
            query += " AND COALESCE(f.location, t.location) = %(location)s"
            params["location"] = location
            
        if product:
            query += " AND COALESCE(f.product, t.product) = %(product)s"
            params["product"] = product
            
        if block:
            query += " AND COALESCE(f.block, t.block) = %(block)s"
            params["block"] = block
            
        if strip:
            query += " AND COALESCE(f.strip, t.strip) = %(strip)s"
            params["strip"] = strip
        
        # Add ordering and pagination
        query += " ORDER BY iso, market, location, product, forward_date"
        query += " OFFSET %(offset)s LIMIT %(limit)s"
        params["offset"] = offset
        params["limit"] = limit
        
        return query, params
    
    async def query_curves(
        self,
        *,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        asof: Optional[str] = None,
        strip: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query curve data."""
        from ..database import get_trino_client
        
        query, params = self._build_curve_query(
            iso=iso,
            market=market,
            location=location,
            product=product,
            block=block,
            asof=asof,
            strip=strip,
            offset=offset,
            limit=limit,
        )
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
        return [dict(row) for row in rows]
    
    async def query_curves_diff(
        self,
        *,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        from_asof: str,
        to_asof: str,
        strip: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query curve diff data between two asof dates."""
        from ..database import get_trino_client
        
        query, params = self._build_curve_diff_query(
            iso=iso,
            market=market,
            location=location,
            product=product,
            block=block,
            from_asof=from_asof,
            to_asof=to_asof,
            strip=strip,
            offset=offset,
            limit=limit,
        )
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
        return [dict(row) for row in rows]
    
    async def query_curve_strips(
        self,
        *,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        asof: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query curve strips data (aggregated by strip)."""
        from ..database import get_trino_client
        
        query = """
        SELECT 
            iso,
            market,
            location,
            product,
            block,
            strip,
            asof,
            AVG(forward_value) as avg_value,
            MIN(forward_value) as min_value,
            MAX(forward_value) as max_value,
            COUNT(*) as point_count,
            MIN(forward_date) as first_date,
            MAX(forward_date) as last_date
        FROM iceberg.market.curve_observation
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        # Add filters
        if iso:
            query += " AND iso = %(iso)s"
            params["iso"] = iso
            
        if market:
            query += " AND market = %(market)s"
            params["market"] = market
            
        if location:
            query += " AND location = %(location)s"
            params["location"] = location
            
        if product:
            query += " AND product = %(product)s"
            params["product"] = product
            
        if block:
            query += " AND block = %(block)s"
            params["block"] = block
            
        if asof:
            query += " AND asof = %(asof)s"
            params["asof"] = asof
        
        # Group by strip and add ordering/pagination
        query += """
        GROUP BY iso, market, location, product, block, strip, asof
        ORDER BY iso, market, location, product, strip
        OFFSET %(offset)s LIMIT %(limit)s
        """
        params["offset"] = offset
        params["limit"] = limit
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
        return [dict(row) for row in rows]
    
    async def invalidate_curve_cache(
        self,
        *,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
    ) -> Dict[str, int]:
        """Invalidate curve cache."""
        from ..cache.cache import CacheManager
        from ..container import get_service
        
        cache_manager = get_service(CacheManager)
        
        # Build cache key pattern based on parameters
        key_pattern = "curves:"
        
        if iso:
            key_pattern += f"{hash(iso)}:"
        else:
            key_pattern += "*:"
        
        if market:
            key_pattern += f"{hash(market)}:"
        else:
            key_pattern += "*:"
        
        if location:
            key_pattern += f"{hash(location)}"
        else:
            key_pattern += "*"
        
        # Delete matching cache keys
        deleted_count = await cache_manager.delete_pattern(key_pattern)
        
        LOGGER.info(
            "Invalidated curve cache",
            extra={
                "iso": iso,
                "market": market,
                "location": location,
                "pattern": key_pattern,
                "deleted_keys": deleted_count,
            }
        )
        
        return {"curves": deleted_count}