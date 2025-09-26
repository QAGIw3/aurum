"""EIA Data Access Object - handles EIA series data operations."""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from aurum.core import AurumSettings
from ..config import CacheConfig, TrinoConfig
from ..state import get_settings

LOGGER = logging.getLogger(__name__)


class EiaDao:
    """Data Access Object for EIA series operations."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        self._settings = settings or get_settings()
        self._trino_config = TrinoConfig.from_settings(self._settings)
        self._cache_config = CacheConfig.from_settings(self._settings)
    
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
        query += " OFFSET %(offset)s LIMIT %(limit)s"
        params["offset"] = offset
        params["limit"] = limit
        
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
        """Query EIA series data."""
        from ..database import get_trino_client
        
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
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
        return [dict(row) for row in rows]
    
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
        """Query EIA series dimensions."""
        from ..database import get_trino_client
        
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
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
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
        return {key: sorted(list(values)) for key, values in dimensions.items()}
    
    async def invalidate_eia_series_cache(self) -> Dict[str, int]:
        """Invalidate EIA series cache."""
        from ..cache.cache import CacheManager
        from ..container import get_service
        
        cache_manager = get_service(CacheManager)
        
        # Pattern to match EIA series cache keys
        pattern = "eia_series:*"
        deleted_count = await cache_manager.delete_pattern(pattern)
        
        LOGGER.info(
            "Invalidated EIA series cache",
            extra={"pattern": pattern, "deleted_keys": deleted_count}
        )
        
        return {"eia_series": deleted_count}