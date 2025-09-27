"""ISO Data Access Object - handles ISO market data operations."""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from aurum.core import AurumSettings
from ..config import CacheConfig, TrinoConfig
from ..state import get_settings

LOGGER = logging.getLogger(__name__)


class IsoDao:
    """Data Access Object for ISO market data operations."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        self._settings = settings or get_settings()
        self._trino_config = TrinoConfig.from_settings(self._settings)
        self._cache_config = CacheConfig.from_settings(self._settings)
    
    def _fetch_timescale_rows(self, sql: str, params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], float]:
        """Execute a Timescale query and return results with timing."""
        from ..service import _fetch_timescale_rows
        return _fetch_timescale_rows(sql, params)
    
    def _cached_iso_lmp_query(
        self, 
        operation: str, 
        sql: str, 
        params: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Execute cached ISO LMP query."""
        from ..service import _cached_iso_lmp_query
        return _cached_iso_lmp_query(operation, sql, params, cache_cfg=self._cache_config)
    
    def query_lmp_last_24h(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        limit: int = 500,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query LMP data for the last 24 hours."""
        sql = (
            "SELECT iso_code, market, delivery_date, interval_start, interval_end, interval_minutes, "
            "location_id, location_name, location_type, price_total, price_energy, price_congestion, "
            "price_loss, currency, uom, settlement_point, source_run_id, ingest_ts, record_hash, metadata "
            "FROM public.iso_lmp_last_24h WHERE 1 = 1"
        )
        params: Dict[str, Any] = {"limit": min(limit, 2000)}
        
        if iso_code:
            sql += " AND iso_code = %(iso_code)s"
            params["iso_code"] = iso_code.upper()
        if market:
            sql += " AND market = %(market)s"
            params["market"] = market.upper()
        if location_id:
            sql += " AND upper(location_id) = upper(%(location_id)s)"
            params["location_id"] = location_id
            
        sql += " ORDER BY interval_start DESC LIMIT %(limit)s"
        return self._cached_iso_lmp_query("last-24h", sql, params)
    
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
        sql = (
            "SELECT iso_code, market, interval_start, location_id, currency, uom, price_avg, price_min, "
            "price_max, price_stddev, sample_count FROM public.iso_lmp_hourly WHERE 1 = 1"
        )
        params: Dict[str, Any] = {"limit": min(limit, 2000)}
        
        if iso_code:
            sql += " AND iso_code = %(iso_code)s"
            params["iso_code"] = iso_code.upper()
        if market:
            sql += " AND market = %(market)s"
            params["market"] = market.upper()
        if location_id:
            sql += " AND upper(location_id) = upper(%(location_id)s)"
            params["location_id"] = location_id
        if start:
            sql += " AND interval_start >= %(start)s"
            params["start"] = start
        if end:
            sql += " AND interval_start <= %(end)s"
            params["end"] = end
            
        sql += " ORDER BY interval_start DESC LIMIT %(limit)s"
        return self._cached_iso_lmp_query("hourly", sql, params)
    
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
        sql = (
            "SELECT iso_code, market, interval_start, location_id, currency, uom, price_avg, price_min, "
            "price_max, price_stddev, sample_count FROM public.iso_lmp_daily WHERE 1 = 1"
        )
        params: Dict[str, Any] = {"limit": min(limit, 2000)}
        
        if iso_code:
            sql += " AND iso_code = %(iso_code)s"
            params["iso_code"] = iso_code.upper()
        if market:
            sql += " AND market = %(market)s"
            params["market"] = market.upper()
        if location_id:
            sql += " AND upper(location_id) = upper(%(location_id)s)"
            params["location_id"] = location_id
        if start:
            sql += " AND interval_start >= %(start)s"
            params["start"] = start
        if end:
            sql += " AND interval_start <= %(end)s"
            params["end"] = end
            
        sql += " ORDER BY interval_start DESC LIMIT %(limit)s"
        return self._cached_iso_lmp_query("daily", sql, params)
    
    def query_lmp_negative(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        limit: int = 200,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query negative LMP data from the last 7 days."""
        sql = (
            "SELECT iso_code, market, delivery_date, interval_start, interval_end, interval_minutes, "
            "location_id, location_name, location_type, price_total, price_energy, price_congestion, "
            "price_loss, currency, uom, settlement_point, source_run_id, ingest_ts, record_hash, metadata "
            "FROM public.iso_lmp_negative_7d WHERE 1 = 1"
        )
        params: Dict[str, Any] = {"limit": min(limit, 1000)}
        
        if iso_code:
            sql += " AND iso_code = %(iso_code)s"
            params["iso_code"] = iso_code.upper()
        if market:
            sql += " AND market = %(market)s"
            params["market"] = market.upper()
            
        sql += " ORDER BY price_total ASC LIMIT %(limit)s"
        return self._cached_iso_lmp_query("negative", sql, params)
    
    async def fetch_locations(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Fetch ISO locations."""
        # This is a placeholder - the actual implementation would depend on
        # the specific table structure for ISO locations
        return []