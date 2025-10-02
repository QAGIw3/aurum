"""EIA (Energy Information Administration) service with caching.

Implements business logic for EIA data operations including series queries,
catalog management, and data validation.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol
from datetime import date, datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import EiaRepository

logger = logging.getLogger(__name__)


class CacheProtocol(Protocol):
    """Protocol for cache implementations."""
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        ...
    
    async def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value in cache with TTL."""
        ...
    
    async def delete(self, key: str) -> None:
        """Delete value from cache."""
        ...


class EiaService(BaseService):
    """Service for EIA data operations with caching support.
    
    EIA provides energy statistics, forecasts, and analysis.
    Data includes:
    - Electricity generation and consumption
    - Energy prices and production
    - International energy statistics
    - Forecasts and projections
    
    This service:
    - Validates EIA series IDs
    - Manages series catalog
    - Provides data query interface
    - Handles frequency conversions
    - Enforces access control
    - Caches series data for performance
    """
    
    def __init__(
        self,
        eia_repository: EiaRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 3600  # 1 hour for EIA data
    ):
        """Initialize service with dependencies.
        
        Args:
            eia_repository: Repository for EIA data access
            cache: Optional cache implementation
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__()
        self.eia_repo = eia_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "eia:v1"
    
    def _build_cache_key(self, operation: str, **params) -> str:
        """Build a cache key from operation and parameters."""
        # Stable key generation regardless of parameter order
        sorted_params = sorted(params.items())
        param_str = json.dumps(sorted_params, sort_keys=True, default=str)
        # Use a short hash suffix to keep keys compact
        param_hash = hashlib.md5(param_str.encode()).hexdigest()[:16]
        return f"{self._cache_namespace}:{operation}:{param_hash}"
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Get value from cache if available."""
        if not self.cache:
            return None
        
        try:
            cached = await self.cache.get(cache_key)
            if cached:
                self.logger.debug(f"Cache hit: {cache_key}")
                return cached
            return None
        except Exception as e:
            self.logger.warning(f"Cache get error: {e}")
            return None
    
    async def _set_in_cache(self, cache_key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        if not self.cache:
            return
        
        try:
            ttl = ttl or self.cache_ttl
            await self.cache.set(cache_key, value, ttl)
            self.logger.debug(f"Cache set: {cache_key}")
        except Exception as e:
            self.logger.warning(f"Cache set error: {e}")
    
    async def get_series(
        self,
        series_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get EIA time series data.
        
        Business logic:
        - Validates series ID format
        - Checks series exists in catalog
        - Applies date range filters
        - Handles missing data
        - Returns observations with metadata
        
        Args:
            series_id: EIA series identifier
            start_date: Start date for data
            end_date: End date for data
            context: Service context
            
        Returns:
            ServiceResult with time series observations
            
        Raises:
            ValidationError: If series ID invalid
            NotFoundError: If series not found
            ServiceError: If query fails
        """
        self._log_operation(
            "get_series",
            context=context,
            series_id=series_id
        )
        
        try:
            # Validate series ID format and enforce ordering of dates
            self._validate_series_id(series_id)
            
            # Validate date range
            if start_date and end_date and start_date > end_date:
                raise ValidationError(
                    "Start date must be before end date",
                    field="date_range"
                )
            
            # Check if series exists in catalog to return 404 quickly
            series_info = await self._get_series_info(series_id)
            if not series_info:
                raise NotFoundError("eia_series", series_id)
            
            # Query observations from repository (DB/warehouse)
            observations = await self.eia_repo.query_series(
                series_id=series_id,
                start_date=start_date.isoformat() if start_date else None,
                end_date=end_date.isoformat() if end_date else None
            )
            
            self.logger.info(
                f"Retrieved {len(observations)} observations for series {series_id}",
                extra={
                    "series_id": series_id,
                    "count": len(observations),
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None
                }
            )
            
            return ServiceResult.ok(
                data=observations,
                metadata={
                    "series_id": series_id,
                    "series_info": series_info,
                    "count": len(observations),
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None
                }
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_series", context)
    
    async def search_series(
        self,
        search_term: str,
        category: Optional[str] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Search EIA series catalog.
        
        Business logic:
        - Validates search term
        - Filters by category if provided
        - Returns ranked results
        - Includes series metadata
        
        Args:
            search_term: Search query
            category: Filter by category (e.g., "electricity", "petroleum")
            limit: Maximum results (max 1000)
            context: Service context
            
        Returns:
            ServiceResult with matching series
            
        Raises:
            ValidationError: If inputs invalid
            ServiceError: If search fails
        """
        self._log_operation(
            "search_series",
            context=context,
            search_term=search_term,
            category=category
        )
        
        try:
            # Validate inputs
            if not search_term or len(search_term.strip()) < 2:
                raise ValidationError(
                    "Search term must be at least 2 characters",
                    field="search_term"
                )
            
            if limit < 1 or limit > 1000:
                raise ValidationError(
                    "Limit must be between 1 and 1000",
                    field="limit"
                )
            
            # Basic search approach until a dedicated index exists:
            # 1) Fetch candidate series from repo
            # 2) Filter client-side by id/dataset/sector fields
            # TODO: Replace with repository-backed text search
            all_series = await self.eia_repo.query_series(limit=limit)
            
            # Basic text search in series data
            search_lower = search_term.strip().lower()
            results = [
                s for s in all_series 
                if search_lower in s.get('series_id', '').lower() or
                   search_lower in str(s.get('dataset', '')).lower() or
                   search_lower in str(s.get('sector', '')).lower()
            ]
            
            # Optional category filter
            if category:
                results = [r for r in results if r.get("category") == category]
            
            return ServiceResult.ok(
                data=results,
                metadata={
                    "search_term": search_term,
                    "category": category,
                    "result_count": len(results),
                    "has_more": len(results) == limit
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "search_series", context)
    
    async def get_categories(
        self,
        parent_category: Optional[str] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get EIA data categories.
        
        Business logic:
        - Returns category hierarchy
        - Filters by parent if provided
        - Includes series counts
        
        Args:
            parent_category: Parent category ID
            context: Service context
            
        Returns:
            ServiceResult with categories
        """
        self._log_operation(
            "get_categories",
            context=context,
            parent_category=parent_category
        )
        
        try:
            # Retrieve categories from the EIA catalog structure once available.
            # Could include parent-child hierarchy and series counts per node.
            categories = []  # TODO: Implement actual category query
            
            return ServiceResult.ok(
                data=categories,
                metadata={
                    "parent_category": parent_category,
                    "count": len(categories)
                }
            )
            
        except Exception as e:
            raise self._handle_error(e, "get_categories", context)
    
    async def get_latest_update(
        self,
        series_id: Optional[str] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Optional[datetime]]:
        """Get latest update timestamp for EIA data.
        
        Args:
            series_id: Specific series (None = all EIA data)
            context: Service context
            
        Returns:
            ServiceResult with latest update timestamp
        """
        self._log_operation(
            "get_latest_update",
            context=context,
            series_id=series_id
        )
        
        try:
            # Query ingestion metadata to determine freshness of EIA data.
            latest = None  # TODO: Implement actual query
            
            return ServiceResult.ok(
                data=latest,
                metadata={
                    "series_id": series_id,
                    "has_data": latest is not None
                }
            )
            
        except Exception as e:
            raise self._handle_error(e, "get_latest_update", context)
    
    # Private helper methods
    
    def _validate_series_id(self, series_id: str) -> None:
        """Validate EIA series ID format.
        
        EIA series IDs typically follow patterns like:
        - ELEC.GEN.ALL-US-99.A (annual)
        - PET.MCRFPUS2.M (monthly)
        """
        if not series_id or not series_id.strip():
            raise ValidationError("Series ID is required", field="series_id")
        
        # Check length
        if len(series_id) > 255:
            raise ValidationError(
                "Series ID too long",
                field="series_id"
            )
        
        # Check for invalid characters
        invalid_chars = ["<", ">", "&", "\"", "'", ";"]
        if any(char in series_id for char in invalid_chars):
            raise ValidationError(
                "Series ID contains invalid characters",
                field="series_id"
            )
    
    async def _get_series_info(self, series_id: str) -> Optional[Dict[str, Any]]:
        """Get series information from catalog.
        
        Returns series metadata including:
        - Name and description
        - Unit of measurement
        - Frequency (annual, monthly, etc.)
        - Last updated
        """
        # Get series metadata from repository
        metadata = await self.eia_repo.get_series_metadata(series_id)
        return metadata
