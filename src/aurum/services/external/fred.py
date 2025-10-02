"""FRED (Federal Reserve Economic Data) service with caching.

Implements business logic for FRED data operations including series queries,
economic indicators, and historical data retrieval.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol
from datetime import date, datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import BaseRepository

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


class FredRepository(BaseRepository):
    """Repository for FRED data operations.
    
    Temporary implementation until proper FredRepository is created.
    """
    
    async def get_series(
        self,
        series_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """Get FRED series data."""
        # Stub implementation - would query from iceberg.external.fred_observations
        return []
    
    async def search_series(
        self,
        search_text: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search FRED series catalog."""
        # Stub implementation - would query from iceberg.external.fred_catalog
        return []
    
    async def get_series_metadata(
        self,
        series_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get metadata for a FRED series."""
        # Stub implementation
        return None


class FredService(BaseService):
    """Service for FRED data operations with caching support.
    
    FRED provides economic data from the Federal Reserve including:
    - Interest rates (Federal Funds Rate, Treasury yields)
    - Economic indicators (GDP, CPI, unemployment)
    - Money supply metrics
    - Exchange rates
    - Industrial production indices
    
    This service:
    - Validates FRED series IDs
    - Manages series catalog
    - Provides data query interface
    - Handles frequency conversions
    - Caches series data for performance
    - Enforces access control
    """
    
    def __init__(
        self,
        fred_repository: FredRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 3600  # 1 hour for FRED data
    ):
        """Initialize service with dependencies.
        
        Args:
            fred_repository: Repository for FRED data access
            cache: Optional cache implementation
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__()
        self.fred_repo = fred_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "fred:v1"
    
    def _build_cache_key(self, operation: str, **params) -> str:
        """Build a cache key from operation and parameters."""
        sorted_params = sorted(params.items())
        param_str = json.dumps(sorted_params, sort_keys=True, default=str)
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
            self.logger.debug(f"Cache set: {cache_key} (TTL: {ttl}s)")
        except Exception as e:
            self.logger.warning(f"Cache set error: {e}")
    
    async def get_series(
        self,
        series_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get FRED series data with optional date range.
        
        Args:
            series_id: FRED series identifier (e.g., "DFF", "UNRATE", "GDPC1")
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with series data
        """
        self._log_operation("get_series", context=context, series_id=series_id)
        
        try:
            # Validate series ID
            if not series_id:
                raise ValidationError("Series ID is required", field="series_id")
            
            # Normalize series ID
            series_id = series_id.upper()
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "series",
                    series_id=series_id,
                    start_date=start_date.isoformat() if start_date else None,
                    end_date=end_date.isoformat() if end_date else None
                )
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={
                            "source": "cache",
                            "series_id": series_id,
                            "cached_at": datetime.utcnow().isoformat()
                        }
                    )
            
            # Get from repository
            data = await self.fred_repo.get_series(
                series_id=series_id,
                start_date=start_date,
                end_date=end_date
            )
            
            if not data:
                raise NotFoundError(f"FRED series not found: {series_id}")
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, data)
            
            return ServiceResult.ok(
                data=data,
                metadata={
                    "source": "database",
                    "series_id": series_id,
                    "count": len(data),
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
        search_text: str,
        limit: int = 100,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Search FRED series catalog.
        
        Args:
            search_text: Text to search for in series names/descriptions
            limit: Maximum number of results
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with matching series
        """
        self._log_operation("search_series", context=context, search_text=search_text)
        
        try:
            # Validate
            if not search_text:
                raise ValidationError("Search text is required", field="search_text")
            
            if limit < 1 or limit > 1000:
                raise ValidationError("Limit must be between 1 and 1000", field="limit")
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "search",
                    search_text=search_text.lower(),
                    limit=limit
                )
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={"source": "cache", "search_text": search_text}
                    )
            
            # Search in repository
            results = await self.fred_repo.search_series(
                search_text=search_text,
                limit=limit
            )
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, results)
            
            return ServiceResult.ok(
                data=results,
                metadata={
                    "source": "database",
                    "search_text": search_text,
                    "count": len(results)
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "search_series", context)
    
    async def get_economic_indicators(
        self,
        indicators: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, List[Dict[str, Any]]]]:
        """Get multiple economic indicators at once.
        
        Common indicators:
        - DFF: Federal Funds Rate
        - UNRATE: Unemployment Rate
        - GDPC1: Real GDP
        - CPIAUCSL: Consumer Price Index
        - DGS10: 10-Year Treasury Rate
        
        Args:
            indicators: List of FRED series IDs
            start_date: Start date for data
            end_date: End date for data
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with dict mapping series_id to data
        """
        self._log_operation(
            "get_economic_indicators",
            context=context,
            indicators=indicators,
            count=len(indicators)
        )
        
        try:
            # Validate
            if not indicators:
                raise ValidationError("At least one indicator is required", field="indicators")
            
            if len(indicators) > 20:
                raise ValidationError("Maximum 20 indicators per request", field="indicators")
            
            # Get each series
            results = {}
            errors = []
            
            for series_id in indicators:
                try:
                    result = await self.get_series(
                        series_id=series_id,
                        start_date=start_date,
                        end_date=end_date,
                        use_cache=use_cache,
                        context=context
                    )
                    if result.success:
                        results[series_id] = result.data
                except NotFoundError:
                    errors.append(f"Series not found: {series_id}")
                except Exception as e:
                    errors.append(f"Error getting {series_id}: {str(e)}")
            
            if not results and errors:
                raise ServiceError(f"Failed to get any indicators: {'; '.join(errors)}")
            
            return ServiceResult.ok(
                data=results,
                metadata={
                    "requested": len(indicators),
                    "retrieved": len(results),
                    "errors": errors if errors else None
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_economic_indicators", context)
    
    async def get_series_metadata(
        self,
        series_id: str,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get metadata for a FRED series.
        
        Args:
            series_id: FRED series identifier
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with series metadata
        """
        self._log_operation("get_series_metadata", context=context, series_id=series_id)
        
        try:
            # Validate
            if not series_id:
                raise ValidationError("Series ID is required", field="series_id")
            
            series_id = series_id.upper()
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key("metadata", series_id=series_id)
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={"source": "cache", "series_id": series_id}
                    )
            
            # Get from repository
            metadata = await self.fred_repo.get_series_metadata(series_id)
            
            if not metadata:
                raise NotFoundError(f"FRED series metadata not found: {series_id}")
            
            # Cache results (longer TTL for metadata)
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, metadata, ttl=86400)  # 24 hours
            
            return ServiceResult.ok(
                data=metadata,
                metadata={"source": "database", "series_id": series_id}
            )
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            raise self._handle_error(e, "get_series_metadata", context)
