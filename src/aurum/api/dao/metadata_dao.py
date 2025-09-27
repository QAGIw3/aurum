"""Metadata Data Access Object - handles metadata and dimension operations."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from aurum.core import AurumSettings
from ..config import CacheConfig, TrinoConfig
from ..state import get_settings

LOGGER = logging.getLogger(__name__)


class MetadataDao:
    """Data Access Object for metadata and dimensions operations."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        self._settings = settings or get_settings()
        self._trino_config = TrinoConfig.from_settings(self._settings)
        self._cache_config = CacheConfig.from_settings(self._settings)
    
    async def query_dimensions(
        self,
        *,
        dimension_type: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, List[str]]:
        """Query available dimensions for filtering across domains."""
        from ..database import get_trino_client
        
        # Base query for common dimensions across all domains
        query = """
        WITH combined_dimensions AS (
            -- Curve dimensions
            SELECT DISTINCT
                'iso' as dimension_type,
                iso as dimension_value
            FROM iceberg.market.curve_observation
            WHERE iso IS NOT NULL
            
            UNION ALL
            
            SELECT DISTINCT
                'market' as dimension_type,
                market as dimension_value
            FROM iceberg.market.curve_observation
            WHERE market IS NOT NULL
            
            UNION ALL
            
            SELECT DISTINCT
                'location' as dimension_type,
                location as dimension_value
            FROM iceberg.market.curve_observation
            WHERE location IS NOT NULL
            
            UNION ALL
            
            SELECT DISTINCT
                'product' as dimension_type,
                product as dimension_value
            FROM iceberg.market.curve_observation
            WHERE product IS NOT NULL
        )
        SELECT 
            dimension_type,
            dimension_value
        FROM combined_dimensions
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        if dimension_type:
            query += " AND dimension_type = %(dimension_type)s"
            params["dimension_type"] = dimension_type
        
        query += " ORDER BY dimension_type, dimension_value"
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
        # Group by dimension type
        dimensions: Dict[str, set] = {}
        for row in rows:
            dim_type = row.get("dimension_type")
            dim_value = row.get("dimension_value")
            
            if dim_type and dim_value:
                if dim_type not in dimensions:
                    dimensions[dim_type] = set()
                dimensions[dim_type].add(str(dim_value))
        
        # Convert to sorted lists
        return {key: sorted(list(values)) for key, values in dimensions.items()}
    
    async def fetch_metadata_dimensions(
        self,
        *,
        domain: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch metadata dimensions with detailed information."""
        from ..database import get_trino_client
        
        # Query for metadata with counts and additional info
        query = """
        SELECT 
            'iso' as dimension_name,
            iso as dimension_value,
            COUNT(*) as record_count,
            MAX(asof) as latest_asof,
            MIN(asof) as earliest_asof
        FROM iceberg.market.curve_observation
        WHERE iso IS NOT NULL
        GROUP BY iso
        
        UNION ALL
        
        SELECT 
            'market' as dimension_name,
            market as dimension_value,
            COUNT(*) as record_count,
            MAX(asof) as latest_asof,
            MIN(asof) as earliest_asof
        FROM iceberg.market.curve_observation
        WHERE market IS NOT NULL
        GROUP BY market
        
        UNION ALL
        
        SELECT 
            'location' as dimension_name,
            location as dimension_value,
            COUNT(*) as record_count,
            MAX(asof) as latest_asof,
            MIN(asof) as earliest_asof
        FROM iceberg.market.curve_observation
        WHERE location IS NOT NULL
        GROUP BY location
        """
        
        params: Dict[str, Any] = {}
        
        if domain:
            # Add domain-specific filtering if needed
            query += " WHERE dimension_name LIKE %(domain_pattern)s"
            params["domain_pattern"] = f"%{domain}%"
        
        query += " ORDER BY dimension_name, record_count DESC"
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
        return [dict(row) for row in rows]
    
    async def fetch_iso_locations(self, *, iso: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch ISO locations with metadata."""
        from ..database import get_trino_client
        
        query = """
        SELECT DISTINCT
            iso,
            location,
            COUNT(*) as curve_count,
            MAX(asof) as latest_data,
            MIN(asof) as earliest_data
        FROM iceberg.market.curve_observation
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        if iso:
            query += " AND iso = %(iso)s"
            params["iso"] = iso
        
        query += """
        GROUP BY iso, location
        ORDER BY iso, curve_count DESC, location
        """
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
        return [dict(row) for row in rows]
    
    async def fetch_units_canonical(self) -> List[Dict[str, Any]]:
        """Fetch canonical units mapping."""
        from ..database import get_trino_client
        
        # This is a simplified implementation - in practice, this might come from a dedicated units table
        query = """
        SELECT DISTINCT
            'energy' as unit_type,
            'MWh' as canonical_unit,
            'Megawatt-hour' as description,
            'MW' as base_unit
            
        UNION ALL
        
        SELECT DISTINCT
            'price' as unit_type,
            'USD/MWh' as canonical_unit,
            'US Dollars per Megawatt-hour' as description,
            'USD' as base_unit
            
        UNION ALL
        
        SELECT DISTINCT
            'volume' as unit_type,
            'MCF' as canonical_unit,
            'Million Cubic Feet' as description,
            'CF' as base_unit
        
        ORDER BY unit_type, canonical_unit
        """
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, {}, use_cache=True)
        
        return [dict(row) for row in rows]
    
    async def fetch_units_mapping(self, *, prefix: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch units mapping with optional prefix filtering."""
        from ..database import get_trino_client
        
        query = """
        SELECT DISTINCT
            'MWh' as source_unit,
            'MWh' as target_unit,
            1.0 as conversion_factor,
            'energy' as unit_category
            
        UNION ALL
        
        SELECT DISTINCT
            'kWh' as source_unit,
            'MWh' as target_unit,
            0.001 as conversion_factor,
            'energy' as unit_category
            
        UNION ALL
        
        SELECT DISTINCT
            'USD/MWh' as source_unit,
            'USD/MWh' as target_unit,
            1.0 as conversion_factor,
            'price' as unit_category
        
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        if prefix:
            query += " AND source_unit LIKE %(prefix_pattern)s"
            params["prefix_pattern"] = f"{prefix}%"
        
        query += " ORDER BY unit_category, source_unit"
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
        return [dict(row) for row in rows]
    
    async def fetch_calendars(self) -> List[Dict[str, Any]]:
        """Fetch available calendars."""
        from ..database import get_trino_client
        
        query = """
        SELECT DISTINCT
            calendar_name,
            COUNT(DISTINCT block_name) as block_count,
            MIN(hour_date) as start_date,
            MAX(hour_date) as end_date
        FROM iceberg.market.dim_calendar_hour
        GROUP BY calendar_name
        ORDER BY calendar_name
        """
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, {}, use_cache=True)
        
        return [dict(row) for row in rows]
    
    async def fetch_calendar_blocks(self, *, name: str) -> List[Dict[str, Any]]:
        """Fetch blocks for a specific calendar."""
        from ..database import get_trino_client
        
        query = """
        SELECT DISTINCT
            calendar_name,
            block_name,
            COUNT(*) as hour_count,
            MIN(hour_date) as start_date,
            MAX(hour_date) as end_date,
            COUNT(DISTINCT CASE WHEN is_business_hour THEN hour_date END) as business_days
        FROM iceberg.market.dim_calendar_hour
        WHERE calendar_name = %(calendar_name)s
        GROUP BY calendar_name, block_name
        ORDER BY block_name
        """
        
        params = {"calendar_name": name}
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params, use_cache=True)
        
        return [dict(row) for row in rows]
    
    async def invalidate_metadata_cache(
        self, 
        *, 
        prefixes: Optional[Sequence[str]] = None
    ) -> Dict[str, int]:
        """Invalidate metadata caches."""
        from ..cache.cache import CacheManager
        from ..container import get_service
        
        cache_manager = get_service(CacheManager)
        
        if prefixes is None:
            prefixes = ["metadata:", "dimensions:", "units:", "calendars:"]
        
        total_deleted = 0
        results = {}
        
        for prefix in prefixes:
            pattern = f"{prefix}*"
            deleted_count = await cache_manager.delete_pattern(pattern)
            results[prefix.rstrip(":")] = deleted_count
            total_deleted += deleted_count
        
        LOGGER.info(
            "Invalidated metadata cache",
            extra={
                "prefixes": list(prefixes),
                "total_deleted": total_deleted,
                "breakdown": results,
            }
        )
        
        return results
    
    async def invalidate_dimensions_cache(self) -> Dict[str, int]:
        """Invalidate dimensions-specific cache."""
        return await self.invalidate_metadata_cache(prefixes=["dimensions:"])