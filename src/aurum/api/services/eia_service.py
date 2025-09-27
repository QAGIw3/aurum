from __future__ import annotations

"""EIA domain service with DAO pattern implementation."""

from typing import Any, Dict, List, Optional, Tuple

from .base_service import QueryableServiceInterface, DimensionalServiceInterface
from ..dao.eia_dao import EiaDao


class EiaService(QueryableServiceInterface, DimensionalServiceInterface):
    """EIA domain service implementing business logic and data access through DAO.
    
    Implements Phase 1.3 service layer decomposition with clear domain boundaries.
    """
    
    def __init__(self):
        self._dao = EiaDao()
    
    async def list_datasets(self, *, offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int]:
        """List EIA datasets with pagination."""
        # For now, delegate to DAO series query with dataset grouping
        series_data = await self._dao.query_eia_series(offset=offset, limit=limit)
        
        # Group by dataset to get unique datasets
        datasets_seen = set()
        datasets = []
        
        for series in series_data:
            dataset = series.get("dataset")
            if dataset and dataset not in datasets_seen:
                datasets_seen.add(dataset)
                datasets.append({
                    "dataset": dataset,
                    "description": f"EIA dataset {dataset}",
                    "series_count": 1  # Simplified for now
                })
        
        return datasets, len(datasets)

    async def get_series(
        self, 
        *, 
        series_id: str, 
        start_date: Optional[str], 
        end_date: Optional[str], 
        offset: int, 
        limit: int
    ) -> List[Dict[str, Any]]:
        """Get EIA series data with optional date filtering."""
        return await self._dao.query_eia_series(
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=limit,
        )

    async def query_data(
        self, 
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query EIA data with pagination and filtering (QueryableServiceInterface)."""
        if filters is None:
            filters = {}
            
        return await self._dao.query_eia_series(
            series_id=filters.get("series_id"),
            frequency=filters.get("frequency"),
            area=filters.get("area"),
            sector=filters.get("sector"),
            dataset=filters.get("dataset"),
            unit=filters.get("unit"),
            canonical_unit=filters.get("canonical_unit"),
            canonical_currency=filters.get("canonical_currency"),
            source=filters.get("source"),
            start_date=filters.get("start_date"),
            end_date=filters.get("end_date"),
            offset=offset,
            limit=limit,
        )

    async def get_dimensions(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[str]]:
        """Get available dimensions for EIA series filtering (DimensionalServiceInterface)."""
        if filters is None:
            filters = {}
            
        return await self._dao.query_eia_series_dimensions(
            series_id=filters.get("series_id"),
            frequency=filters.get("frequency"),
            area=filters.get("area"),
            sector=filters.get("sector"),
            dataset=filters.get("dataset"),
            unit=filters.get("unit"),
            canonical_unit=filters.get("canonical_unit"),
            canonical_currency=filters.get("canonical_currency"),
            source=filters.get("source"),
        )

    async def get_series_dimensions(
        self,
        *,
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
        """Get available dimensions for EIA series filtering (legacy method)."""
        return await self._dao.query_eia_series_dimensions(
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
    
    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate EIA-related caches (ServiceInterface)."""
        return await self._dao.invalidate_eia_series_cache()

