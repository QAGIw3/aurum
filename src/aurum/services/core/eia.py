"""EIA service for energy data operations.

Provides business logic for Energy Information Administration data.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime

from .base import BaseService
from ...core.protocols.cache import CacheProtocol
from ...data.repositories.eia import EiaRepository

logger = logging.getLogger(__name__)


class EiaService(BaseService):
    """Service for EIA energy data analytics with caching.
    
    Handles:
    - Energy production and consumption series
    - Price data for various energy commodities
    - Supply and demand metrics
    - Regional energy statistics
    - Dataset discovery and metadata
    
    This service:
    - Validates business rules
    - Orchestrates repository operations
    - Implements energy analytics
    - Provides optional caching for performance
    - Supports streaming exports for large datasets
    - Enforces access control
    - Tracks performance metrics
    """
    
    def __init__(
        self,
        eia_repository: EiaRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 3600  # 1 hour default for energy data
    ):
        """Initialize service with dependencies.
        
        Args:
            eia_repository: Repository for EIA data access
            cache: Optional cache implementation for performance
            cache_ttl: Cache time-to-live in seconds (default 3600)
        """
        super().__init__()
        self.eia_repo = eia_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "eia:v1"
    
    # Data Access Methods
    
    async def list_datasets(
        self,
        *,
        offset: int = 0,
        limit: int = 100
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List available EIA datasets with pagination.
        
        Args:
            offset: Pagination offset
            limit: Maximum results
            
        Returns:
            Tuple of (dataset list, total count)
        """
        cache_key = f"{self._cache_namespace}:datasets:{offset}:{limit}"
        
        # Check cache
        if self.cache:
            cached = await self._get_cached(cache_key)
            if cached is not None:
                return cached
        
        # Get from repository
        datasets, total = await self.eia_repo.get_datasets(
            offset=offset,
            limit=limit
        )
        
        result = (datasets, total)
        
        # Cache result
        if self.cache and datasets:
            await self._set_cached(cache_key, result, ttl=self.cache_ttl)
        
        return result
    
    async def get_series(
        self,
        *,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get EIA series data with optional date filtering.
        
        Args:
            series_id: EIA series identifier
            start_date: Start date for time range
            end_date: End date for time range
            offset: Pagination offset
            limit: Maximum results
            
        Returns:
            List of series data points
        """
        # For specific series queries, use shorter cache
        cache_key = f"{self._cache_namespace}:series:{series_id}:{start_date}:{end_date}:{offset}:{limit}"
        
        if self.cache:
            cached = await self._get_cached(cache_key)
            if cached is not None:
                return cached
        
        result = await self.eia_repo.query_series(
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=limit
        )
        
        if self.cache and result:
            # Shorter cache for time-series data
            await self._set_cached(cache_key, result, ttl=self.cache_ttl // 2)
        
        return result
    
    async def query_data(
        self,
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query EIA data with pagination and filtering.
        
        Args:
            offset: Pagination offset
            limit: Maximum results
            filters: Query filters
            
        Returns:
            List of matching data points
        """
        if filters is None:
            filters = {}
        
        # Extract filters
        return await self.eia_repo.query_series(
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
            limit=limit
        )
    
    async def get_dimensions(
        self,
        *,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[str]]:
        """Get available dimensions for EIA series filtering.
        
        Args:
            filters: Optional filters to constrain dimensions
            
        Returns:
            Dictionary mapping dimension names to lists of values
        """
        if filters is None:
            filters = {}
        
        # Create cache key from filters
        cache_key = f"{self._cache_namespace}:dimensions:{self._hash_dict(filters)}"
        
        if self.cache:
            cached = await self._get_cached(cache_key)
            if cached is not None:
                return cached
        
        result = await self.eia_repo.get_series_dimensions(
            series_id=filters.get("series_id"),
            frequency=filters.get("frequency"),
            area=filters.get("area"),
            sector=filters.get("sector"),
            dataset=filters.get("dataset"),
            unit=filters.get("unit"),
            canonical_unit=filters.get("canonical_unit"),
            canonical_currency=filters.get("canonical_currency"),
            source=filters.get("source")
        )
        
        if self.cache:
            await self._set_cached(cache_key, result, ttl=self.cache_ttl)
        
        return result
    
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
        source: Optional[str] = None
    ) -> Dict[str, List[str]]:
        """Get available dimensions for EIA series filtering (legacy method).
        
        Args:
            Various filter parameters
            
        Returns:
            Dictionary mapping dimension names to lists of values
        """
        return await self.eia_repo.get_series_dimensions(
            series_id=series_id,
            frequency=frequency,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source
        )
    
    # Analytics Methods
    
    async def get_series_metadata(
        self,
        series_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get comprehensive metadata for a series.
        
        Args:
            series_id: EIA series identifier
            
        Returns:
            Series metadata or None if not found
        """
        cache_key = f"{self._cache_namespace}:metadata:{series_id}"
        
        if self.cache:
            cached = await self._get_cached(cache_key)
            if cached is not None:
                return cached
        
        result = await self.eia_repo.get_series_metadata(series_id)
        
        if self.cache and result:
            await self._set_cached(cache_key, result, ttl=self.cache_ttl * 2)
        
        return result
    
    async def get_latest_values(
        self,
        series_ids: List[str],
        as_of_date: Optional[date] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Get latest values for multiple series.
        
        Args:
            series_ids: List of EIA series identifiers
            as_of_date: Get latest values as of this date
            
        Returns:
            Dictionary mapping series_id to latest value data
        """
        if not series_ids:
            return {}
        
        # Use short cache for latest values
        cache_key = f"{self._cache_namespace}:latest:{self._hash_list(series_ids)}:{as_of_date}"
        
        if self.cache:
            cached = await self._get_cached(cache_key)
            if cached is not None:
                return cached
        
        result = await self.eia_repo.get_latest_values(
            series_ids=series_ids,
            as_of_date=as_of_date
        )
        
        if self.cache and result:
            # Very short cache for latest values
            await self._set_cached(cache_key, result, ttl=300)  # 5 minutes
        
        return result
    
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
        cache_key = f"{self._cache_namespace}:stats:{series_id}:{start_date}:{end_date}"
        
        if self.cache:
            cached = await self._get_cached(cache_key)
            if cached is not None:
                return cached
        
        result = await self.eia_repo.get_series_statistics(
            series_id=series_id,
            start_date=start_date,
            end_date=end_date
        )
        
        if self.cache:
            await self._set_cached(cache_key, result, ttl=self.cache_ttl)
        
        return result
    
    # Cache Management
    
    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate EIA-related caches.
        
        Returns:
            Dictionary with invalidation statistics
        """
        if not self.cache:
            return {"invalidated": 0}
        
        # Invalidate all EIA cache entries
        pattern = f"{self._cache_namespace}:*"
        invalidated = await self.cache.delete_pattern(pattern)
        
        logger.info(f"Invalidated {invalidated} EIA cache entries")
        
        return {"invalidated": invalidated}
    
    # Helper Methods
    
    async def _get_cached(self, key: str) -> Any:
        """Get value from cache."""
        if not self.cache:
            return None
        try:
            return await self.cache.get(key)
        except Exception as e:
            logger.warning(f"Cache get error: {e}")
            return None
    
    async def _set_cached(self, key: str, value: Any, ttl: int) -> None:
        """Set value in cache."""
        if not self.cache:
            return
        try:
            await self.cache.set(key, value, ttl=ttl)
        except Exception as e:
            logger.warning(f"Cache set error: {e}")
    
    def _hash_dict(self, d: Dict[str, Any]) -> str:
        """Create hash from dictionary for cache keys."""
        import hashlib
        import json
        content = json.dumps(d, sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()
    
    def _hash_list(self, lst: List[Any]) -> str:
        """Create hash from list for cache keys."""
        import hashlib
        import json
        content = json.dumps(sorted(lst))
        return hashlib.md5(content.encode()).hexdigest()
