"""Async EIA Data Access Object - handles EIA series data operations with connection pooling."""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Optional, Tuple

from .base_async_dao import BaseAsyncDao

LOGGER = logging.getLogger(__name__)


class EiaAsyncDao(BaseAsyncDao):
    """Async Data Access Object for EIA series operations with connection pooling."""
    
    @property
    def dao_name(self) -> str:
        return "eia"
    
    def _eia_series_base_table(self) -> str:
        """Get EIA series base table name."""
        return "timescale.eia.series"
    
    def _build_sql_eia_series(
        self,
        series_id: Optional[str] = None,
        frequency: Optional[str] = None,
        area: Optional[str] = None,
        sector: Optional[str] = None,
        dataset: Optional[str] = None,
        unit: Optional[str] = None,
        canonical_unit: Optional[str] = None,
        canonical_currency: Optional[str] = None,
        source: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[str, Dict[str, Any]]:
        """Build SQL query for EIA series data."""
        base_table = self._eia_series_base_table()
        
        # Build base query
        query = f"""
        SELECT 
            series_id,
            timestamp_utc,
            value,
            frequency,
            area,
            sector,
            dataset,
            unit,
            canonical_unit,
            canonical_currency,
            source,
            created_at,
            updated_at
        FROM {base_table}
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        # Add filters
        if series_id:
            query += " AND series_id = %(series_id)s"
            params["series_id"] = series_id
            
        if frequency:
            query += " AND frequency = %(frequency)s"
            params["frequency"] = frequency
            
        if area:
            query += " AND area = %(area)s"
            params["area"] = area
            
        if sector:
            query += " AND sector = %(sector)s"
            params["sector"] = sector
            
        if dataset:
            query += " AND dataset = %(dataset)s"
            params["dataset"] = dataset
            
        if unit:
            query += " AND unit = %(unit)s"
            params["unit"] = unit
            
        if canonical_unit:
            query += " AND canonical_unit = %(canonical_unit)s"
            params["canonical_unit"] = canonical_unit
            
        if canonical_currency:
            query += " AND canonical_currency = %(canonical_currency)s"
            params["canonical_currency"] = canonical_currency
            
        if source:
            query += " AND source = %(source)s"
            params["source"] = source
            
        if start_date:
            query += " AND timestamp_utc >= %(start_date)s::timestamp"
            params["start_date"] = start_date
            
        if end_date:
            query += " AND timestamp_utc <= %(end_date)s::timestamp"
            params["end_date"] = end_date
        
        # Add ordering and pagination
        query += " ORDER BY series_id, timestamp_utc"
        query += " LIMIT %(limit)s OFFSET %(offset)s"
        params["limit"] = limit
        params["offset"] = offset
        
        return query, params
    
    def _build_sql_eia_series_dimensions(
        self,
        series_id: Optional[str] = None,
        frequency: Optional[str] = None,
        area: Optional[str] = None,
        sector: Optional[str] = None,
        dataset: Optional[str] = None,
        unit: Optional[str] = None,
        canonical_unit: Optional[str] = None,
        canonical_currency: Optional[str] = None,
        source: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Build SQL query for EIA series dimensions."""
        base_table = self._eia_series_base_table()
        
        query = f"""
        SELECT DISTINCT
            frequency,
            area,
            sector,
            dataset,
            unit,
            canonical_unit,
            canonical_currency,
            source
        FROM {base_table}
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        # Add filters
        if series_id:
            query += " AND series_id = %(series_id)s"
            params["series_id"] = series_id
            
        if frequency:
            query += " AND frequency = %(frequency)s"
            params["frequency"] = frequency
            
        if area:
            query += " AND area = %(area)s"
            params["area"] = area
            
        if sector:
            query += " AND sector = %(sector)s"
            params["sector"] = sector
            
        if dataset:
            query += " AND dataset = %(dataset)s"
            params["dataset"] = dataset
            
        if unit:
            query += " AND unit = %(unit)s"
            params["unit"] = unit
            
        if canonical_unit:
            query += " AND canonical_unit = %(canonical_unit)s"
            params["canonical_unit"] = canonical_unit
            
        if canonical_currency:
            query += " AND canonical_currency = %(canonical_currency)s"
            params["canonical_currency"] = canonical_currency
            
        if source:
            query += " AND source = %(source)s"
            params["source"] = source
        
        query += " ORDER BY frequency, area, sector, dataset, unit"
        
        return query, params
    
    async def query_eia_series(
        self,
        series_id: Optional[str] = None,
        frequency: Optional[str] = None,
        area: Optional[str] = None,
        sector: Optional[str] = None,
        dataset: Optional[str] = None,
        unit: Optional[str] = None,
        canonical_unit: Optional[str] = None,
        canonical_currency: Optional[str] = None,
        source: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query EIA series data asynchronously."""
        query, params = self._build_sql_eia_series(
            series_id=series_id,
            frequency=frequency,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=limit,
        )
        
        try:
            result = await self.execute_query(query, params, use_cache=True)
            
            LOGGER.info(
                "EIA series query executed successfully",
                extra={
                    "series_id": series_id,
                    "dataset": dataset,
                    "rows_returned": len(result),
                    "offset": offset,
                    "limit": limit
                }
            )
            
            return result
            
        except Exception as e:
            LOGGER.error(
                "EIA series query failed",
                extra={
                    "series_id": series_id,
                    "dataset": dataset,
                    "error": str(e)
                }
            )
            raise
    
    async def query_eia_series_dimensions(
        self,
        series_id: Optional[str] = None,
        frequency: Optional[str] = None,
        area: Optional[str] = None,
        sector: Optional[str] = None,
        dataset: Optional[str] = None,
        unit: Optional[str] = None,
        canonical_unit: Optional[str] = None,
        canonical_currency: Optional[str] = None,
        source: Optional[str] = None,
    ) -> Dict[str, List[str]]:
        """Query EIA series dimensions asynchronously."""
        query, params = self._build_sql_eia_series_dimensions(
            series_id=series_id,
            frequency=frequency,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source,
        )
        
        try:
            rows = await self.execute_query(query, params, use_cache=True)
            
            # Group dimensions
            dimensions: Dict[str, set] = {
                "frequency": set(),
                "area": set(),
                "sector": set(),
                "dataset": set(),
                "unit": set(),
                "canonical_unit": set(),
                "canonical_currency": set(),
                "source": set(),
            }
            
            for row in rows:
                for key in dimensions:
                    value = row.get(key)
                    if value:
                        dimensions[key].add(str(value))
            
            # Convert to sorted lists
            result = {key: sorted(list(values)) for key, values in dimensions.items()}
            
            LOGGER.info(
                "EIA series dimensions query executed successfully",
                extra={
                    "dimensions_count": {k: len(v) for k, v in result.items()},
                    "total_rows": len(rows)
                }
            )
            
            return result
            
        except Exception as e:
            LOGGER.error(
                "EIA series dimensions query failed",
                extra={"error": str(e)}
            )
            raise
    
    async def get_eia_series_count(
        self,
        series_id: Optional[str] = None,
        frequency: Optional[str] = None,
        area: Optional[str] = None,
        sector: Optional[str] = None,
        dataset: Optional[str] = None,
        unit: Optional[str] = None,
        canonical_unit: Optional[str] = None,
        canonical_currency: Optional[str] = None,
        source: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        """Get total count of EIA series matching criteria."""
        base_query, params = self._build_sql_eia_series(
            series_id=series_id,
            frequency=frequency,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source,
            start_date=start_date,
            end_date=end_date,
            offset=0,
            limit=1000000  # Large limit for count
        )
        
        # Remove LIMIT and OFFSET for count query
        base_query = base_query.replace(" LIMIT %(limit)s OFFSET %(offset)s", "")
        params.pop("limit", None)
        params.pop("offset", None)
        
        return await self.execute_count_query(base_query, params)
    
    async def invalidate_eia_series_cache(self) -> Dict[str, int]:
        """Invalidate EIA series cache."""
        try:
            # This would integrate with the cache system when available
            # For now, return a placeholder result
            LOGGER.info("EIA series cache invalidation requested")
            
            return {"eia_series": 0}  # Placeholder - would integrate with actual cache
            
        except Exception as e:
            LOGGER.error(
                "EIA series cache invalidation failed",
                extra={"error": str(e)}
            )
            raise