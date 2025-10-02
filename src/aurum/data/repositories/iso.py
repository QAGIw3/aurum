"""ISO repository for Independent System Operator data operations.

Provides domain-specific operations for ISO market and pricing data.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime

from .base import BaseRepository
from ..dao import TimescaleDAO

logger = logging.getLogger(__name__)


class IsoRepository(BaseRepository):
    """Repository for ISO (Independent System Operator) data operations.
    
    ISO data includes:
    - LMP (Locational Marginal Pricing) data
    - Market summaries (hourly, daily)
    - Negative price events
    - Node/location metadata
    - Market operations data
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._timescale_dao: Optional[TimescaleDAO] = None
    
    async def initialize(self) -> None:
        """Initialize repository and its DAOs."""
        self._timescale_dao = TimescaleDAO(self.settings)
        await self._timescale_dao.initialize()
    
    async def close(self) -> None:
        """Close repository and its DAOs."""
        if self._timescale_dao:
            await self._timescale_dao.close()
    
    async def __aenter__(self) -> IsoRepository:
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
    
    async def get_lmp_last_24h(
        self,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        """Get last 24 hours of LMP data.
        
        Args:
            iso_code: ISO identifier (e.g., "PJM", "ERCOT")
            market: Market type (e.g., "DA", "RT")
            location_id: Specific location/node identifier
            limit: Maximum number of results
            
        Returns:
            List of LMP data points from the last 24 hours
        """
        query = """
            SELECT iso_code, market, delivery_date, interval_start, interval_end, interval_minutes,
                   location_id, location_name, location_type, price_total, price_energy, price_congestion,
                   price_loss, currency, uom, settlement_point, source_run_id, ingest_ts, record_hash, metadata
            FROM public.iso_lmp_last_24h
            WHERE 1=1
        """
        params: Dict[str, Any] = {}
        
        if iso_code:
            query += " AND iso_code = :iso_code"
            params["iso_code"] = iso_code.upper()
        
        if market:
            query += " AND market = :market"
            params["market"] = market.upper()
        
        if location_id:
            query += " AND UPPER(location_id) = UPPER(:location_id)"
            params["location_id"] = location_id
        
        query += " ORDER BY interval_start DESC LIMIT :limit"
        params["limit"] = min(limit, 2000)
        
        return await self._timescale_dao.execute_query(query, params)
    
    async def get_lmp_hourly(
        self,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        date_str: Optional[str] = None,
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        """Get hourly aggregated LMP data.
        
        Args:
            iso_code: ISO identifier
            market: Market type
            location_id: Location identifier
            date_str: Specific date (ISO format)
            limit: Maximum number of results
            
        Returns:
            List of hourly aggregated LMP data
        """
        query = """
            SELECT iso_code, market, interval_start, location_id, currency, uom,
                   price_avg, price_min, price_max, price_stddev, sample_count
            FROM public.iso_lmp_hourly
            WHERE 1=1
        """
        params: Dict[str, Any] = {}
        
        if iso_code:
            query += " AND iso_code = :iso_code"
            params["iso_code"] = iso_code.upper()
        
        if market:
            query += " AND market = :market"
            params["market"] = market.upper()
        
        if location_id:
            query += " AND UPPER(location_id) = UPPER(:location_id)"
            params["location_id"] = location_id
        
        if date_str:
            query += " AND interval_start::date = DATE :date_str"
            params["date_str"] = date_str
        
        query += " ORDER BY interval_start DESC LIMIT :limit"
        params["limit"] = min(limit, 2000)
        
        return await self._timescale_dao.execute_query(query, params)
    
    async def get_lmp_daily(
        self,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        """Get daily aggregated LMP data.
        
        Args:
            iso_code: ISO identifier
            market: Market type
            location_id: Location identifier
            start_date: Start date for range
            end_date: End date for range
            limit: Maximum number of results
            
        Returns:
            List of daily aggregated LMP data
        """
        query = """
            SELECT iso_code, market, interval_start, location_id, currency, uom,
                   price_avg, price_min, price_max, price_stddev, sample_count
            FROM public.iso_lmp_daily
            WHERE 1=1
        """
        params: Dict[str, Any] = {}
        
        if iso_code:
            query += " AND iso_code = :iso_code"
            params["iso_code"] = iso_code.upper()
        
        if market:
            query += " AND market = :market"
            params["market"] = market.upper()
        
        if location_id:
            query += " AND UPPER(location_id) = UPPER(:location_id)"
            params["location_id"] = location_id
        
        if start_date:
            query += " AND interval_start >= :start_date"
            params["start_date"] = start_date
        
        if end_date:
            query += " AND interval_start <= :end_date"
            params["end_date"] = end_date
        
        query += " ORDER BY interval_start DESC LIMIT :limit"
        params["limit"] = min(limit, 2000)
        
        return await self._timescale_dao.execute_query(query, params)
    
    async def get_lmp_negative(
        self,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        """Get negative LMP price events.
        
        Args:
            iso_code: ISO identifier
            market: Market type
            location_id: Location identifier
            start_date: Start date for range
            end_date: End date for range
            limit: Maximum number of results
            
        Returns:
            List of negative LMP events sorted by price (most negative first)
        """
        query = """
            SELECT iso_code, market, delivery_date, interval_start, interval_end, interval_minutes,
                   location_id, location_name, location_type, price_total, price_energy, price_congestion,
                   price_loss, currency, uom, settlement_point, source_run_id, ingest_ts, record_hash, metadata
            FROM public.iso_lmp_negative_7d
            WHERE 1=1
        """
        params: Dict[str, Any] = {}
        
        if iso_code:
            query += " AND iso_code = :iso_code"
            params["iso_code"] = iso_code.upper()
        
        if market:
            query += " AND market = :market"
            params["market"] = market.upper()
        
        if location_id:
            query += " AND UPPER(location_id) = UPPER(:location_id)"
            params["location_id"] = location_id
        
        if start_date:
            query += " AND delivery_date >= :start_date"
            params["start_date"] = start_date
        
        if end_date:
            query += " AND delivery_date <= :end_date"
            params["end_date"] = end_date
        
        query += " ORDER BY price_total ASC LIMIT :limit"
        params["limit"] = min(limit, 1000)
        
        return await self._timescale_dao.execute_query(query, params)
    
    async def get_iso_nodes(
        self,
        iso_code: str,
        market: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get available nodes/locations for an ISO.
        
        Args:
            iso_code: ISO identifier
            market: Optional market type filter
            limit: Maximum number of results
            
        Returns:
            List of node/location information
        """
        query = """
            SELECT DISTINCT location_id, location_name, location_type
            FROM public.iso_lmp_hourly
            WHERE iso_code = :iso_code
        """
        params: Dict[str, Any] = {"iso_code": iso_code.upper()}
        
        if market:
            query += " AND market = :market"
            params["market"] = market.upper()
        
        query += " ORDER BY location_id LIMIT :limit"
        params["limit"] = limit
        
        return await self._timescale_dao.execute_query(query, params)
    
    async def get_market_summary(
        self,
        iso_code: str,
        market: Optional[str] = None,
        date_obj: Optional[date] = None
    ) -> Dict[str, Any]:
        """Get market summary statistics for an ISO.
        
        Args:
            iso_code: ISO identifier
            market: Optional market type filter
            date_obj: Date for summary (None = latest)
            
        Returns:
            Dictionary with market summary statistics
        """
        # If no date provided, use the latest available date
        if not date_obj:
            latest_query = """
                SELECT MAX(interval_start::date) as latest_date
                FROM public.iso_lmp_daily
                WHERE iso_code = :iso_code
            """
            result = await self._timescale_dao.execute_query_single(
                latest_query,
                {"iso_code": iso_code.upper()}
            )
            if result and result.get("latest_date"):
                date_obj = result["latest_date"]
            else:
                # No data available
                return {
                    "iso": iso_code,
                    "date": None,
                    "market": market,
                    "data_available": False
                }
        
        # Get summary statistics
        query = """
            SELECT
                COUNT(*) as location_count,
                AVG(price_avg) as avg_price,
                MIN(price_min) as min_price,
                MAX(price_max) as max_price,
                AVG(price_stddev) as avg_volatility,
                SUM(sample_count) as total_samples
            FROM public.iso_lmp_daily
            WHERE iso_code = :iso_code
              AND interval_start::date = :date_obj
        """
        params: Dict[str, Any] = {
            "iso_code": iso_code.upper(),
            "date_obj": date_obj
        }
        
        if market:
            query += " AND market = :market"
            params["market"] = market.upper()
        
        result = await self._timescale_dao.execute_query_single(query, params)
        
        if result:
            return {
                "iso": iso_code,
                "date": date_obj.isoformat(),
                "market": market,
                "location_count": result.get("location_count", 0),
                "avg_price_dollar_per_mwh": float(result.get("avg_price", 0)),
                "min_price_dollar_per_mwh": float(result.get("min_price", 0)),
                "max_price_dollar_per_mwh": float(result.get("max_price", 0)),
                "price_volatility": float(result.get("avg_volatility", 0)),
                "total_samples": result.get("total_samples", 0),
                "data_available": True
            }
        else:
            return {
                "iso": iso_code,
                "date": date_obj.isoformat(),
                "market": market,
                "data_available": False
            }
    
    async def get_available_isos(self) -> List[str]:
        """Get list of available ISOs in the system.
        
        Returns:
            List of ISO codes
        """
        query = """
            SELECT DISTINCT iso_code
            FROM public.iso_lmp_hourly
            ORDER BY iso_code
        """
        
        results = await self._timescale_dao.execute_query(query, {})
        return [r["iso_code"] for r in results]
    
    async def get_available_markets(self, iso_code: str) -> List[str]:
        """Get available markets for an ISO.
        
        Args:
            iso_code: ISO identifier
            
        Returns:
            List of market types
        """
        query = """
            SELECT DISTINCT market
            FROM public.iso_lmp_hourly
            WHERE iso_code = :iso_code
            ORDER BY market
        """
        
        results = await self._timescale_dao.execute_query(
            query,
            {"iso_code": iso_code.upper()}
        )
        return [r["market"] for r in results]
