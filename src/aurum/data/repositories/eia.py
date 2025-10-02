"""EIA repository for Energy Information Administration data operations.

Provides domain-specific operations for EIA energy data.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime

from .base import BaseRepository
from ..dao import TimescaleDAO

logger = logging.getLogger(__name__)


class EiaRepository(BaseRepository):
    """Repository for EIA (Energy Information Administration) data operations.
    
    EIA data includes:
    - Energy production and consumption series
    - Price data for various energy commodities
    - Supply and demand metrics
    - Regional energy statistics
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
    
    async def __aenter__(self) -> EiaRepository:
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
    
    async def query_series(
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
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query EIA series data with flexible filtering.
        
        Args:
            series_id: EIA series identifier
            frequency: Data frequency (e.g., "M" for monthly, "A" for annual)
            area: Geographic area
            sector: Energy sector
            dataset: EIA dataset name
            unit: Original unit of measurement
            canonical_unit: Standardized unit
            canonical_currency: Currency for price data
            source: Data source
            start_date: Start date for time range
            end_date: End date for time range
            offset: Pagination offset
            limit: Maximum results
            
        Returns:
            List of EIA series data points
        """
        query = """
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
            FROM timescale.eia.series
            WHERE 1=1
        """
        params: Dict[str, Any] = {}
        
        # Add filters
        if series_id:
            query += " AND series_id = :series_id"
            params["series_id"] = series_id
        
        if frequency:
            query += " AND frequency = :frequency"
            params["frequency"] = frequency
        
        if area:
            query += " AND area = :area"
            params["area"] = area
        
        if sector:
            query += " AND sector = :sector"
            params["sector"] = sector
        
        if dataset:
            query += " AND dataset = :dataset"
            params["dataset"] = dataset
        
        if unit:
            query += " AND unit = :unit"
            params["unit"] = unit
        
        if canonical_unit:
            query += " AND canonical_unit = :canonical_unit"
            params["canonical_unit"] = canonical_unit
        
        if canonical_currency:
            query += " AND canonical_currency = :canonical_currency"
            params["canonical_currency"] = canonical_currency
        
        if source:
            query += " AND source = :source"
            params["source"] = source
        
        if start_date:
            query += " AND timestamp_utc >= :start_date"
            params["start_date"] = start_date
        
        if end_date:
            query += " AND timestamp_utc <= :end_date"
            params["end_date"] = end_date
        
        # Add ordering and pagination
        query += " ORDER BY timestamp_utc DESC, series_id LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        
        return await self._timescale_dao.execute_query(query, params)
    
    async def get_series_dimensions(
        self,
        series_id: Optional[str] = None,
        frequency: Optional[str] = None,
        area: Optional[str] = None,
        sector: Optional[str] = None,
        dataset: Optional[str] = None,
        unit: Optional[str] = None,
        canonical_unit: Optional[str] = None,
        canonical_currency: Optional[str] = None,
        source: Optional[str] = None
    ) -> Dict[str, List[str]]:
        """Get available dimension values for EIA series.
        
        Returns distinct values for each dimension that match the filters.
        
        Args:
            Various filter parameters to constrain dimension values
            
        Returns:
            Dictionary mapping dimension names to lists of distinct values
        """
        dimensions = {}
        base_where = "WHERE 1=1"
        base_params: Dict[str, Any] = {}
        
        # Build base filter conditions
        if series_id:
            base_where += " AND series_id = :series_id"
            base_params["series_id"] = series_id
        
        if frequency:
            base_where += " AND frequency = :frequency"
            base_params["frequency"] = frequency
        
        if area:
            base_where += " AND area = :area"
            base_params["area"] = area
        
        if sector:
            base_where += " AND sector = :sector"
            base_params["sector"] = sector
        
        if dataset:
            base_where += " AND dataset = :dataset"
            base_params["dataset"] = dataset
        
        if unit:
            base_where += " AND unit = :unit"
            base_params["unit"] = unit
        
        if canonical_unit:
            base_where += " AND canonical_unit = :canonical_unit"
            base_params["canonical_unit"] = canonical_unit
        
        if canonical_currency:
            base_where += " AND canonical_currency = :canonical_currency"
            base_params["canonical_currency"] = canonical_currency
        
        if source:
            base_where += " AND source = :source"
            base_params["source"] = source
        
        # Query each dimension
        dimension_columns = [
            "frequency", "area", "sector", "dataset", 
            "unit", "canonical_unit", "canonical_currency", "source"
        ]
        
        for dim in dimension_columns:
            query = f"""
                SELECT DISTINCT {dim}
                FROM timescale.eia.series
                {base_where}
                AND {dim} IS NOT NULL
                ORDER BY {dim}
            """
            
            results = await self._timescale_dao.execute_query(query, base_params)
            dimensions[dim] = [r[dim] for r in results if r.get(dim)]
        
        return dimensions
    
    async def get_series_metadata(
        self,
        series_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific EIA series.
        
        Args:
            series_id: EIA series identifier
            
        Returns:
            Series metadata or None if not found
        """
        query = """
            SELECT DISTINCT
                series_id,
                frequency,
                area,
                sector,
                dataset,
                unit,
                canonical_unit,
                canonical_currency,
                source,
                MIN(timestamp_utc) as start_date,
                MAX(timestamp_utc) as end_date,
                COUNT(*) as data_points
            FROM timescale.eia.series
            WHERE series_id = :series_id
            GROUP BY 
                series_id, frequency, area, sector, dataset,
                unit, canonical_unit, canonical_currency, source
        """
        
        result = await self._timescale_dao.execute_query_single(
            query,
            {"series_id": series_id}
        )
        
        return result
    
    async def get_latest_values(
        self,
        series_ids: List[str],
        as_of_date: Optional[date] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Get latest values for multiple series.
        
        Args:
            series_ids: List of EIA series identifiers
            as_of_date: Get latest values as of this date (None = current)
            
        Returns:
            Dictionary mapping series_id to latest value data
        """
        if not series_ids:
            return {}
        
        params: Dict[str, Any] = {"series_ids": series_ids}
        
        if as_of_date:
            date_filter = "AND timestamp_utc <= :as_of_date"
            params["as_of_date"] = as_of_date.isoformat()
        else:
            date_filter = ""
        
        query = f"""
            WITH latest_dates AS (
                SELECT 
                    series_id,
                    MAX(timestamp_utc) as latest_timestamp
                FROM timescale.eia.series
                WHERE series_id = ANY(:series_ids)
                {date_filter}
                GROUP BY series_id
            )
            SELECT 
                s.series_id,
                s.timestamp_utc,
                s.value,
                s.frequency,
                s.area,
                s.sector,
                s.dataset,
                s.unit,
                s.canonical_unit,
                s.canonical_currency
            FROM timescale.eia.series s
            INNER JOIN latest_dates ld
                ON s.series_id = ld.series_id
                AND s.timestamp_utc = ld.latest_timestamp
        """
        
        results = await self._timescale_dao.execute_query(query, params)
        
        # Convert to dictionary keyed by series_id
        return {
            row["series_id"]: row
            for row in results
        }
    
    async def get_series_statistics(
        self,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get statistical summary for a series.
        
        Args:
            series_id: EIA series identifier
            start_date: Start date for statistics
            end_date: End date for statistics
            
        Returns:
            Dictionary with statistical measures
        """
        query = """
            SELECT
                COUNT(*) as count,
                AVG(value) as mean,
                MIN(value) as min,
                MAX(value) as max,
                STDDEV(value) as stddev,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) as q1,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) as q3
            FROM timescale.eia.series
            WHERE series_id = :series_id
            AND value IS NOT NULL
        """
        params: Dict[str, Any] = {"series_id": series_id}
        
        if start_date:
            query += " AND timestamp_utc >= :start_date"
            params["start_date"] = start_date
        
        if end_date:
            query += " AND timestamp_utc <= :end_date"
            params["end_date"] = end_date
        
        result = await self._timescale_dao.execute_query_single(query, params)
        
        if result:
            # Convert to float and handle None values
            for key in ["mean", "min", "max", "stddev", "q1", "median", "q3"]:
                if result.get(key) is not None:
                    result[key] = float(result[key])
        
        return result or {
            "count": 0,
            "mean": None,
            "min": None,
            "max": None,
            "stddev": None,
            "q1": None,
            "median": None,
            "q3": None
        }
    
    async def get_datasets(
        self,
        offset: int = 0,
        limit: int = 100
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get list of available EIA datasets.
        
        Args:
            offset: Pagination offset
            limit: Maximum results
            
        Returns:
            Tuple of (dataset list, total count)
        """
        # Get total count
        count_query = """
            SELECT COUNT(DISTINCT dataset) as total
            FROM timescale.eia.series
            WHERE dataset IS NOT NULL
        """
        count_result = await self._timescale_dao.execute_query_single(count_query, {})
        total = count_result["total"] if count_result else 0
        
        # Get paginated datasets
        query = """
            SELECT 
                dataset,
                COUNT(DISTINCT series_id) as series_count,
                COUNT(*) as data_points,
                MIN(timestamp_utc) as earliest_date,
                MAX(timestamp_utc) as latest_date
            FROM timescale.eia.series
            WHERE dataset IS NOT NULL
            GROUP BY dataset
            ORDER BY dataset
            LIMIT :limit OFFSET :offset
        """
        
        results = await self._timescale_dao.execute_query(
            query,
            {"limit": limit, "offset": offset}
        )
        
        # Format results
        datasets = []
        for row in results:
            datasets.append({
                "dataset": row["dataset"],
                "description": f"EIA dataset {row['dataset']}",
                "series_count": row["series_count"],
                "data_points": row["data_points"],
                "earliest_date": row["earliest_date"].isoformat() if row["earliest_date"] else None,
                "latest_date": row["latest_date"].isoformat() if row["latest_date"] else None
            })
        
        return datasets, total
