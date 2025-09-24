"""Enhanced async curve service with performance optimizations."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from ....core import (
    get_current_context,
    MultiLevelCache,
    CacheLevel,
    BackpressureManager,
    AsyncBatch,
)
from ...exceptions import ValidationException, NotFoundException, ServiceUnavailableException
from ...models import CurveResponse, CurvePoint, Meta

logger = logging.getLogger(__name__)


@dataclass
class CurveQuery:
    """Structured curve query parameters."""
    iso: str
    market: Optional[str] = None
    location: Optional[str] = None
    commodity: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    limit: Optional[int] = None
    filters: Optional[Dict[str, Any]] = None
    
    def to_cache_key(self) -> str:
        """Generate cache key for this query."""
        key_parts = [
            f"iso:{self.iso}",
            f"market:{self.market or 'all'}",
            f"location:{self.location or 'all'}",
            f"commodity:{self.commodity or 'all'}",
            f"dates:{self.start_date}-{self.end_date}",
            f"limit:{self.limit or 'all'}",
        ]
        if self.filters:
            sorted_filters = sorted(self.filters.items())
            key_parts.append(f"filters:{sorted_filters}")
        
        return ":".join(key_parts)


class AsyncCurveService:
    """Enhanced async curve service with intelligent caching and batching."""
    
    def __init__(
        self,
        cache: MultiLevelCache,
        data_source,  # Abstract data source
        backpressure_manager: Optional[BackpressureManager] = None,
        batch_size: int = 100,
        batch_timeout: float = 0.1
    ):
        self.cache = cache
        self.data_source = data_source
        self.backpressure_manager = backpressure_manager or BackpressureManager()
        self._query_batch = AsyncBatch(batch_size, batch_timeout)
        self._cache_warming_tasks: set = set()
    
    async def fetch_curves(
        self,
        iso: str,
        market: Optional[str] = None,
        location: Optional[str] = None,
        commodity: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: Optional[int] = None,
        use_cache: bool = True,
        warm_cache: bool = False
    ) -> Tuple[List[CurvePoint], Meta]:
        """Fetch curves with intelligent caching and performance optimization."""
        
        context = get_current_context()
        logger.info(
            "Fetching curves",
            extra={
                "request_id": context.request_id,
                "iso": iso,
                "market": market,
                "location": location,
                "start_date": start_date,
                "end_date": end_date
            }
        )
        
        # Validate input parameters
        self._validate_curve_query(iso, market, location, start_date, end_date)
        
        # Create structured query
        query = CurveQuery(
            iso=iso,
            market=market,
            location=location,
            commodity=commodity,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
        
        # Try cache first if enabled
        if use_cache:
            cached_result = await self._get_from_cache(query)
            if cached_result is not None:
                curves, meta = cached_result
                logger.debug("Cache hit for curve query", extra={"cache_key": query.to_cache_key()})
                return curves, meta
        
        # Use backpressure management for data source queries
        async with self.backpressure_manager.acquire():
            try:
                # Fetch from data source
                curves, meta = await self._fetch_from_source(query)
                
                # Cache the result if caching is enabled
                if use_cache:
                    await self._store_in_cache(query, curves, meta)
                
                # Optionally warm related cache entries
                if warm_cache:
                    self._schedule_cache_warming(query)
                
                return curves, meta
                
            except Exception as e:
                logger.error(
                    "Failed to fetch curves from data source",
                    extra={
                        "request_id": context.request_id,
                        "error": str(e),
                        "query": query
                    }
                )
                raise ServiceUnavailableException(f"Unable to fetch curve data: {str(e)}")
    
    async def fetch_curve_diff(
        self,
        iso: str,
        market: str,
        location: str,
        base_date: date,
        compare_date: date,
        use_cache: bool = True
    ) -> Tuple[List[Dict[str, Any]], Meta]:
        """Calculate curve differences between two dates."""
        
        # Fetch both curves concurrently
        base_curves_task = asyncio.create_task(
            self.fetch_curves(iso, market, location, start_date=base_date, end_date=base_date, use_cache=use_cache)
        )
        compare_curves_task = asyncio.create_task(
            self.fetch_curves(iso, market, location, start_date=compare_date, end_date=compare_date, use_cache=use_cache)
        )
        
        base_curves, base_meta = await base_curves_task
        compare_curves, compare_meta = await compare_curves_task
        
        # Calculate differences
        diff_points = self._calculate_curve_diff(base_curves, compare_curves)
        
        # Create metadata
        context = get_current_context()
        meta = Meta(
            request_id=context.request_id,
            query_time_ms=max(base_meta.query_time_ms, compare_meta.query_time_ms)
        )
        
        return diff_points, meta
    
    async def batch_fetch_curves(
        self,
        queries: List[CurveQuery],
        use_cache: bool = True
    ) -> List[Tuple[List[CurvePoint], Meta]]:
        """Batch fetch multiple curve queries for efficiency."""
        
        # Separate cached and uncached queries
        cached_results = {}
        uncached_queries = []
        
        if use_cache:
            for i, query in enumerate(queries):
                cached_result = await self._get_from_cache(query)
                if cached_result is not None:
                    cached_results[i] = cached_result
                else:
                    uncached_queries.append((i, query))
        else:
            uncached_queries = list(enumerate(queries))
        
        # Fetch uncached queries in batch
        uncached_results = {}
        if uncached_queries:
            fetch_tasks = [
                asyncio.create_task(self._fetch_from_source_with_index(i, query))
                for i, query in uncached_queries
            ]
            
            completed_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            
            for result in completed_results:
                if isinstance(result, Exception):
                    logger.warning(f"Batch fetch failed for query: {result}")
                    continue
                
                index, curves, meta, query = result
                uncached_results[index] = (curves, meta)
                
                # Cache the result
                if use_cache:
                    await self._store_in_cache(query, curves, meta)
        
        # Combine results in original order
        results = []
        for i, query in enumerate(queries):
            if i in cached_results:
                results.append(cached_results[i])
            elif i in uncached_results:
                results.append(uncached_results[i])
            else:
                # Handle failed queries
                context = get_current_context()
                empty_meta = Meta(request_id=context.request_id, query_time_ms=0)
                results.append(([], empty_meta))
        
        return results
    
    def _validate_curve_query(
        self,
        iso: str,
        market: Optional[str],
        location: Optional[str],
        start_date: Optional[date],
        end_date: Optional[date]
    ) -> None:
        """Validate curve query parameters."""
        if not iso or not isinstance(iso, str):
            raise ValidationException("ISO parameter is required and must be a string")
        
        if start_date and end_date and start_date > end_date:
            raise ValidationException("Start date must be before or equal to end date")
        
        # Add more validation as needed
        valid_isos = {"PJM", "CAISO", "ERCOT", "ISONE", "NYISO", "MISO", "SPP"}
        if iso.upper() not in valid_isos:
            raise ValidationException(f"Invalid ISO: {iso}. Must be one of {valid_isos}")
    
    async def _get_from_cache(self, query: CurveQuery) -> Optional[Tuple[List[CurvePoint], Meta]]:
        """Get curve data from cache."""
        try:
            cache_key = query.to_cache_key()
            cached_data = await self.cache.get(cache_key)
            
            if cached_data is not None:
                return cached_data
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {e}")
        
        return None
    
    async def _store_in_cache(
        self,
        query: CurveQuery,
        curves: List[CurvePoint],
        meta: Meta,
        ttl: int = 3600  # 1 hour default
    ) -> None:
        """Store curve data in cache."""
        try:
            cache_key = query.to_cache_key()
            cache_value = (curves, meta)
            
            # Generate cache tags for invalidation
            tags = {
                f"iso:{query.iso}",
                f"market:{query.market or 'all'}",
                f"location:{query.location or 'all'}"
            }
            
            await self.cache.set(cache_key, cache_value, ttl=ttl, tags=tags)
        except Exception as e:
            logger.warning(f"Cache storage failed: {e}")
    
    async def _fetch_from_source(self, query: CurveQuery) -> Tuple[List[CurvePoint], Meta]:
        """Fetch curve data from the underlying data source."""
        # This would integrate with the actual data source (Trino, TimescaleDB, etc.)
        # For now, this is a placeholder that would be implemented based on the actual data source
        
        context = get_current_context()
        
        # Simulate async data source call
        await asyncio.sleep(0.1)  # Simulate network/DB latency
        
        # Return empty results for now - in real implementation this would query the data source
        curves = []
        meta = Meta(request_id=context.request_id, query_time_ms=100)
        
        return curves, meta
    
    async def _fetch_from_source_with_index(
        self,
        index: int,
        query: CurveQuery
    ) -> Tuple[int, List[CurvePoint], Meta, CurveQuery]:
        """Fetch from source with index for batch operations."""
        curves, meta = await self._fetch_from_source(query)
        return index, curves, meta, query
    
    def _calculate_curve_diff(
        self,
        base_curves: List[CurvePoint],
        compare_curves: List[CurvePoint]
    ) -> List[Dict[str, Any]]:
        """Calculate differences between two curve datasets."""
        # Create lookup for base curves
        base_lookup = {
            (point.delivery_date, point.delivery_hour): point.price
            for point in base_curves
        }
        
        diff_points = []
        for point in compare_curves:
            key = (point.delivery_date, point.delivery_hour)
            base_price = base_lookup.get(key)
            
            if base_price is not None:
                diff = point.price - base_price
                diff_points.append({
                    "delivery_date": point.delivery_date,
                    "delivery_hour": point.delivery_hour,
                    "base_price": base_price,
                    "compare_price": point.price,
                    "difference": diff,
                    "percent_change": (diff / base_price * 100) if base_price != 0 else 0
                })
        
        return diff_points
    
    def _schedule_cache_warming(self, query: CurveQuery) -> None:
        """Schedule related cache warming operations."""
        # This could warm related queries (different time ranges, markets, etc.)
        # Implementation would depend on usage patterns and data relationships
        pass
    
    async def invalidate_cache(self, iso: Optional[str] = None, market: Optional[str] = None) -> int:
        """Invalidate cache entries for specific ISO/market."""
        tags = set()
        
        if iso:
            tags.add(f"iso:{iso}")
        if market:
            tags.add(f"market:{market}")
        
        if not tags:
            # Clear all curve cache if no specific tags
            await self.cache.clear()
            return -1  # Unknown count
        
        return await self.cache.invalidate_by_tags(tags)