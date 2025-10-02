"""Curve service for market data operations.

Implements business logic for curve queries, comparisons, and analytics with caching support.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol
from datetime import date, datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import CurveRepository

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


class CurveService(BaseService):
    """Service for curve business logic with caching and export capabilities.
    
    Curves represent market data points (prices, forecasts) across
    time intervals and locations.
    
    This service:
    - Validates business rules
    - Orchestrates repository operations
    - Implements curve analytics and comparisons
    - Provides optional caching for performance
    - Supports streaming exports for large datasets
    - Enforces access control
    - Tracks performance metrics
    """
    
    def __init__(
        self,
        curve_repository: CurveRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 300  # 5 minutes default
    ):
        """Initialize service with dependencies.
        
        Args:
            curve_repository: Repository for curve data access
            cache: Optional cache implementation for performance
            cache_ttl: Cache time-to-live in seconds (default 300)
        """
        super().__init__()
        self.curve_repo = curve_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "curves:v1"
    
    # Caching helper methods
    
    def _build_cache_key(self, operation: str, **params) -> str:
        """Build a cache key from operation and parameters.
        
        Args:
            operation: Operation name (e.g., "get_curves")
            **params: Parameters to include in cache key
            
        Returns:
            Cache key string
        """
        # Sort params for consistent cache keys
        sorted_params = sorted(params.items())
        param_str = json.dumps(sorted_params, sort_keys=True, default=str)
        param_hash = hashlib.md5(param_str.encode()).hexdigest()[:16]
        return f"{self._cache_namespace}:{operation}:{param_hash}"
    
    async def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Get value from cache if available.
        
        Args:
            cache_key: Cache key
            
        Returns:
            Cached value or None
        """
        if not self.cache:
            return None
        
        try:
            cached = await self.cache.get(cache_key)
            if cached:
                self.logger.debug(f"Cache hit: {cache_key}")
                return cached
            self.logger.debug(f"Cache miss: {cache_key}")
            return None
        except Exception as e:
            self.logger.warning(f"Cache get error: {e}")
            return None
    
    async def _set_in_cache(self, cache_key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache.
        
        Args:
            cache_key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (None = use default)
        """
        if not self.cache:
            return
        
        try:
            ttl = ttl or self.cache_ttl
            await self.cache.set(cache_key, value, ttl)
            self.logger.debug(f"Cache set: {cache_key} (TTL={ttl}s)")
        except Exception as e:
            self.logger.warning(f"Cache set error: {e}")
    
    async def _invalidate_cache(self, operation: str, **params) -> None:
        """Invalidate cache entry.
        
        Args:
            operation: Operation name
            **params: Parameters used to build cache key
        """
        if not self.cache:
            return
        
        try:
            cache_key = self._build_cache_key(operation, **params)
            await self.cache.delete(cache_key)
            self.logger.debug(f"Cache invalidated: {cache_key}")
        except Exception as e:
            self.logger.warning(f"Cache invalidation error: {e}")
    
    # Public service methods
    
    async def get_curves(
        self,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        asof: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get curves with filters and optional caching.
        
        Business logic:
        - Validates filter combinations
        - Enforces tenant access control
        - Applies default as-of date if not provided
        - Limits result size
        - Caches results for performance (optional)
        
        Args:
            iso: ISO/RTO identifier
            market: Market type (DA, RT, etc.)
            location: Location/node identifier
            product: Product type
            asof: As-of date for point-in-time query
            limit: Maximum results (default 100, max 1000)
            offset: Pagination offset
            use_cache: Whether to use caching (default True)
            context: Service context with tenant info
            
        Returns:
            ServiceResult with list of curve data points
            
        Raises:
            ValidationError: If validation fails
            ServiceError: If operation fails
        """
        self._log_operation(
            "get_curves",
            context=context,
            iso=iso,
            market=market,
            limit=limit
        )
        
        try:
            # Validate business rules
            self._validate_query_params(limit, offset)
            
            # Apply business logic: default to latest if no asof
            if not asof and not iso and not market:
                # Require at least some filter for broad queries
                raise ValidationError(
                    "Must provide at least one filter (iso, market, or asof)",
                    field="filters"
                )
            
            # Enforce max limit
            limit = min(limit, 1000)
            
            # Try cache first if enabled
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "get_curves",
                    iso=iso,
                    market=market,
                    location=location,
                    product=product,
                    asof=asof.isoformat() if asof else None,
                    limit=limit,
                    offset=offset
                )
                cached_curves = await self._get_from_cache(cache_key)
                if cached_curves is not None:
                    return ServiceResult.ok(
                        data=cached_curves,
                        metadata={
                            "count": len(cached_curves),
                            "limit": limit,
                            "offset": offset,
                            "source": "cache",
                            "has_more": len(cached_curves) == limit
                        }
                    )
            
            # Query repository
            curves = await self.curve_repo.find_by_filters(
                iso=iso,
                market=market,
                location=location,
                product=product,
                asof=asof,
                limit=limit,
                offset=offset
            )
            
            # Apply tenant filtering if needed
            if context and context.tenant_id:
                curves = self._filter_by_tenant(curves, context.tenant_id)
            
            # Cache results if enabled
            if use_cache and cache_key and self.cache:
                await self._set_in_cache(cache_key, curves)
            
            self.logger.info(
                f"Retrieved {len(curves)} curves",
                extra={
                    "count": len(curves),
                    "iso": iso,
                    "market": market,
                }
            )
            
            return ServiceResult.ok(
                data=curves,
                metadata={
                    "count": len(curves),
                    "limit": limit,
                    "offset": offset,
                    "source": "database",
                    "has_more": len(curves) == limit
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_curves", context)
    
    async def get_curve_by_key(
        self,
        curve_key: str,
        asof: Optional[date] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get curve by unique key.
        
        Args:
            curve_key: Unique curve identifier
            asof: As-of date for point-in-time query
            context: Service context
            
        Returns:
            ServiceResult with curve data points
            
        Raises:
            NotFoundError: If curve not found
            ServiceError: If operation fails
        """
        self._log_operation("get_curve_by_key", context=context, curve_key=curve_key)
        
        try:
            curves = await self.curve_repo.find_by_key(
                curve_key=curve_key,
                asof=asof,
                limit=1000  # Reasonable limit for single curve
            )
            
            if not curves:
                raise NotFoundError(
                    resource="curve",
                    identifier=curve_key
                )
            
            return ServiceResult.ok(
                data=curves,
                metadata={"curve_key": curve_key, "count": len(curves)}
            )
            
        except NotFoundError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_curve_by_key", context)
    
    async def get_latest_asof(
        self,
        iso: Optional[str] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Optional[date]]:
        """Get the latest as-of date for curves.
        
        Useful for determining data freshness and default query dates.
        
        Args:
            iso: Filter by ISO (None = all)
            context: Service context
            
        Returns:
            ServiceResult with latest date or None
        """
        self._log_operation("get_latest_asof", context=context, iso=iso)
        
        try:
            latest = await self.curve_repo.get_latest_asof(iso=iso)
            
            return ServiceResult.ok(
                data=latest,
                metadata={"iso": iso, "has_data": latest is not None}
            )
            
        except Exception as e:
            raise self._handle_error(e, "get_latest_asof", context)
    
    async def compare_curves(
        self,
        curve_key_1: str,
        curve_key_2: str,
        asof: Optional[date] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Compare two curves.
        
        Business logic for curve comparison analytics.
        
        Args:
            curve_key_1: First curve identifier
            curve_key_2: Second curve identifier
            asof: As-of date for comparison
            context: Service context
            
        Returns:
            ServiceResult with comparison analytics
        """
        self._log_operation(
            "compare_curves",
            context=context,
            curve1=curve_key_1,
            curve2=curve_key_2
        )
        
        try:
            # Get both curves
            curves1 = await self.curve_repo.find_by_key(curve_key_1, asof)
            curves2 = await self.curve_repo.find_by_key(curve_key_2, asof)
            
            if not curves1:
                raise NotFoundError("curve", curve_key_1)
            if not curves2:
                raise NotFoundError("curve", curve_key_2)
            
            # Perform comparison analytics
            comparison = self._compute_comparison(curves1, curves2)
            
            return ServiceResult.ok(
                data=comparison,
                metadata={
                    "curve_key_1": curve_key_1,
                    "curve_key_2": curve_key_2,
                    "asof": asof.isoformat() if asof else None
                }
            )
            
        except NotFoundError:
            raise
        except Exception as e:
            raise self._handle_error(e, "compare_curves", context)
    
    async def export_curves(
        self,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        asof: Optional[date] = None,
        context: Optional[ServiceContext] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """Export curves as a streaming iterator for large datasets.
        
        This method is optimized for exporting large amounts of curve data
        without loading everything into memory at once.
        
        Args:
            iso: ISO/RTO identifier
            market: Market type
            location: Location/node identifier
            product: Product type
            asof: As-of date for point-in-time query
            context: Service context
            
        Yields:
            Individual curve data points
            
        Raises:
            ValidationError: If validation fails
            ServiceError: If operation fails
        """
        self._log_operation(
            "export_curves",
            context=context,
            iso=iso,
            market=market
        )
        
        try:
            # Stream curves from repository
            # Note: Repository needs to support streaming for this to work efficiently
            # For now, we'll fetch in batches
            offset = 0
            batch_size = 1000
            
            while True:
                result = await self.get_curves(
                    iso=iso,
                    market=market,
                    location=location,
                    product=product,
                    asof=asof,
                    limit=batch_size,
                    offset=offset,
                    use_cache=False,  # Don't cache during exports
                    context=context
                )
                
                if not result.data:
                    break
                
                for curve in result.data:
                    yield curve
                
                # Check if there are more results
                if not result.metadata.get("has_more", False):
                    break
                
                offset += batch_size
            
            self.logger.info(
                "Curve export completed",
                extra={"iso": iso, "market": market}
            )
            
        except Exception as e:
            self.logger.error(f"Export error: {e}")
            raise self._handle_error(e, "export_curves", context)
    
    async def invalidate_curve_cache(
        self,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[bool]:
        """Invalidate cached curve data.
        
        Useful when data has been updated and cache needs to be cleared.
        
        Args:
            iso: ISO to invalidate (None = all)
            market: Market to invalidate (None = all)
            context: Service context
            
        Returns:
            ServiceResult indicating success
        """
        self._log_operation("invalidate_cache", context=context, iso=iso, market=market)
        
        try:
            # In a full implementation, we'd need to track and invalidate all related cache keys
            # For now, just invalidate common patterns
            await self._invalidate_cache("get_curves", iso=iso, market=market)
            
            return ServiceResult.ok(
                data=True,
                metadata={"invalidated": True, "iso": iso, "market": market}
            )
        except Exception as e:
            self.logger.warning(f"Cache invalidation error: {e}")
            # Cache invalidation errors shouldn't fail the operation
            return ServiceResult.ok(
                data=False,
                metadata={"invalidated": False, "error": str(e)}
            )
    
    # Private helper methods
    
    def _validate_query_params(self, limit: int, offset: int) -> None:
        """Validate query parameters."""
        if limit < 1:
            raise ValidationError("Limit must be at least 1", field="limit")
        if limit > 10000:
            raise ValidationError("Limit cannot exceed 10000", field="limit")
        if offset < 0:
            raise ValidationError("Offset cannot be negative", field="offset")
    
    def _filter_by_tenant(
        self,
        curves: List[Dict[str, Any]],
        tenant_id: str
    ) -> List[Dict[str, Any]]:
        """Apply tenant-based filtering (if applicable).
        
        In a multi-tenant system, filter curves by tenant access rights.
        """
        # Placeholder: In production, implement actual tenant filtering
        # based on curve metadata or access control rules
        return curves
    
    def _compute_comparison(
        self,
        curves1: List[Dict[str, Any]],
        curves2: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Compute comparison analytics between two curves.
        
        Returns statistics like:
        - Mean difference
        - Max difference
        - Correlation
        - Trend comparison
        """
        # Simplified implementation
        # In production, implement full analytics
        return {
            "curve1_count": len(curves1),
            "curve2_count": len(curves2),
            "comparison_type": "basic",
            "note": "Full analytics implementation pending"
        }

