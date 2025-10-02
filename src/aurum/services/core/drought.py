"""Drought service for climate risk operations.

Implements business logic for drought data queries and analytics with caching support.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol
from datetime import date, datetime

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import DroughtRepository

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


class DroughtService(BaseService):
    """Service for drought monitoring and analytics with caching.
    
    Handles:
    - Drought indices (SPI, SPEI, PDSI)
    - USDM (U.S. Drought Monitor) classifications
    - Regional drought conditions
    - Historical drought patterns
    - Vector event data (drought boundaries)
    
    This service:
    - Validates business rules
    - Orchestrates repository operations
    - Implements drought analytics
    - Provides optional caching for performance
    - Supports streaming exports for large datasets
    - Enforces access control
    - Tracks performance metrics
    """
    
    def __init__(
        self,
        drought_repository: DroughtRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 3600  # 1 hour default for climate data
    ):
        """Initialize service with dependencies.
        
        Args:
            drought_repository: Repository for drought data access
            cache: Optional cache implementation for performance
            cache_ttl: Cache time-to-live in seconds (default 3600)
        """
        super().__init__()
        self.drought_repo = drought_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "drought:v1"
    
    # Caching helper methods
    
    def _build_cache_key(self, operation: str, **params) -> str:
        """Build a cache key from operation and parameters.
        
        Args:
            operation: Operation name
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
        """Get value from cache if available."""
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
        """Set value in cache."""
        if not self.cache:
            return
        
        try:
            ttl = ttl or self.cache_ttl
            await self.cache.set(cache_key, value, ttl)
            self.logger.debug(f"Cache set: {cache_key} (TTL={ttl}s)")
        except Exception as e:
            self.logger.warning(f"Cache set error: {e}")
    
    # Public service methods
    
    async def query_indices(
        self,
        dataset: Optional[str] = None,
        index_id: Optional[str] = None,
        timescale: Optional[str] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Query drought indices with filters and optional caching.
        
        Business logic:
        - Validates filter combinations
        - Enforces reasonable time ranges
        - Applies default limit
        - Caches results for performance (optional)
        
        Args:
            dataset: Dataset name (e.g., "spi", "spei", "pdsi")
            index_id: Specific index identifier
            timescale: Time scale (e.g., "1-month", "3-month", "12-month")
            region_type: Type of region (e.g., "state", "county", "basin")
            region_id: Specific region identifier
            start_date: Start date for data
            end_date: End date for data
            limit: Maximum results (default 500, max 10000)
            use_cache: Whether to use caching (default True)
            context: Service context with tenant info
            
        Returns:
            ServiceResult with list of drought index data points
            
        Raises:
            ValidationError: If validation fails
            ServiceError: If operation fails
        """
        self._log_operation(
            "query_indices",
            context=context,
            dataset=dataset,
            region_type=region_type,
            region_id=region_id
        )
        
        try:
            # Validate business rules
            self._validate_query_params(limit, start_date, end_date)
            
            # Validate dataset if provided
            if dataset:
                self._validate_dataset(dataset)
            
            # Validate timescale if provided
            if timescale:
                self._validate_timescale(timescale)
            
            # Enforce max limit
            limit = min(limit, 10000)
            
            # Try cache first if enabled
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "query_indices",
                    dataset=dataset,
                    index_id=index_id,
                    timescale=timescale,
                    region_type=region_type,
                    region_id=region_id,
                    start_date=start_date.isoformat() if start_date else None,
                    end_date=end_date.isoformat() if end_date else None,
                    limit=limit
                )
                cached_data = await self._get_from_cache(cache_key)
                if cached_data is not None:
                    return ServiceResult.ok(
                        data=cached_data,
                        metadata={
                            "count": len(cached_data),
                            "limit": limit,
                            "source": "cache",
                            "has_more": len(cached_data) == limit
                        }
                    )
            
            # Query repository
            indices = await self.drought_repo.query_drought_indices(
                region_type=region_type,
                region_id=region_id,
                dataset=dataset,
                index_id=index_id,
                timescale=timescale,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )
            
            # Apply tenant filtering if needed
            if context and context.tenant_id:
                indices = self._filter_by_tenant(indices, context.tenant_id)
            
            # Cache results if enabled
            if use_cache and cache_key and self.cache:
                await self._set_in_cache(cache_key, indices)
            
            self.logger.info(
                f"Retrieved {len(indices)} drought indices",
                extra={
                    "count": len(indices),
                    "dataset": dataset,
                    "region_type": region_type,
                }
            )
            
            return ServiceResult.ok(
                data=indices,
                metadata={
                    "count": len(indices),
                    "limit": limit,
                    "source": "database",
                    "has_more": len(indices) == limit,
                    "dataset": dataset,
                    "timescale": timescale
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "query_indices", context)
    
    async def query_usdm(
        self,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Query USDM (U.S. Drought Monitor) data.
        
        Business logic:
        - Validates region parameters
        - Enforces reasonable time ranges
        - Returns drought classifications by region
        
        Args:
            region_type: Type of region
            region_id: Specific region identifier
            start_date: Start date for data
            end_date: End date for data
            limit: Maximum results (default 500, max 10000)
            use_cache: Whether to use caching (default True)
            context: Service context
            
        Returns:
            ServiceResult with USDM classification data
        """
        self._log_operation(
            "query_usdm",
            context=context,
            region_type=region_type,
            region_id=region_id
        )
        
        try:
            # Validate business rules
            self._validate_query_params(limit, start_date, end_date)
            
            # Validate region type if provided
            if region_type:
                self._validate_region_type(region_type)
            
            # Enforce max limit
            limit = min(limit, 10000)
            
            # Try cache first if enabled
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key(
                    "query_usdm",
                    region_type=region_type,
                    region_id=region_id,
                    start_date=start_date.isoformat() if start_date else None,
                    end_date=end_date.isoformat() if end_date else None,
                    limit=limit
                )
                cached_data = await self._get_from_cache(cache_key)
                if cached_data is not None:
                    return ServiceResult.ok(
                        data=cached_data,
                        metadata={
                            "count": len(cached_data),
                            "limit": limit,
                            "source": "cache",
                            "has_more": len(cached_data) == limit
                        }
                    )
            
            # Query repository
            usdm_data = await self.drought_repo.query_usdm_data(
                region_type=region_type,
                region_id=region_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit
            )
            
            # Cache results if enabled
            if use_cache and cache_key and self.cache:
                await self._set_in_cache(cache_key, usdm_data)
            
            self.logger.info(
                f"Retrieved {len(usdm_data)} USDM records",
                extra={
                    "count": len(usdm_data),
                    "region_type": region_type,
                }
            )
            
            return ServiceResult.ok(
                data=usdm_data,
                metadata={
                    "count": len(usdm_data),
                    "limit": limit,
                    "source": "database",
                    "has_more": len(usdm_data) == limit,
                    "data_type": "usdm"
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "query_usdm", context)
    
    async def query_vector_events(
        self,
        layer: Optional[str] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 500,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Query drought vector events (spatial drought boundaries).
        
        Business logic:
        - Validates layer types
        - Handles temporal queries for drought boundaries
        - Returns GeoJSON-compatible vector data
        
        Args:
            layer: Vector layer type
            region_type: Type of region
            region_id: Specific region identifier
            start_time: Start time for events
            end_time: End time for events
            limit: Maximum results (default 500, max 10000)
            use_cache: Whether to use caching (default True)
            context: Service context
            
        Returns:
            ServiceResult with vector event data
        """
        self._log_operation(
            "query_vector_events",
            context=context,
            layer=layer,
            region_type=region_type
        )
        
        try:
            # Validate business rules
            self._validate_query_params(limit, None, None)
            self._validate_time_range(start_time, end_time)
            
            # Validate layer if provided
            if layer:
                self._validate_layer(layer)
            
            # Enforce max limit
            limit = min(limit, 10000)
            
            # Note: Vector event queries are not yet implemented in the repository
            # This is a placeholder for future implementation
            self.logger.warning("Vector event queries not yet implemented")
            
            return ServiceResult.ok(
                data=[],
                metadata={
                    "count": 0,
                    "limit": limit,
                    "source": "not_implemented",
                    "message": "Vector event queries coming soon"
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "query_vector_events", context)
    
    async def get_drought_statistics(
        self,
        region_type: str,
        region_id: str,
        start_date: date,
        end_date: date,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get comprehensive drought statistics for a region.
        
        Business logic:
        - Computes aggregate drought metrics
        - Identifies drought episodes and severity
        - Provides trend analysis
        
        Args:
            region_type: Type of region
            region_id: Region identifier
            start_date: Start date for analysis
            end_date: End date for analysis
            context: Service context
            
        Returns:
            ServiceResult with drought statistics
        """
        self._log_operation(
            "get_drought_statistics",
            context=context,
            region_type=region_type,
            region_id=region_id
        )
        
        try:
            # Validate inputs
            if not region_type or not region_id:
                raise ValidationError(
                    "Both region_type and region_id are required",
                    field="region"
                )
            
            self._validate_region_type(region_type)
            self._validate_date_range(start_date, end_date)
            
            # Get statistics from repository
            stats = await self.drought_repo.get_drought_statistics(
                region_type=region_type,
                region_id=region_id,
                start_date=start_date,
                end_date=end_date
            )
            
            # Enhance with business logic calculations
            stats = self._enhance_statistics(stats)
            
            return ServiceResult.ok(
                data=stats,
                metadata={
                    "region_type": region_type,
                    "region_id": region_id,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_drought_statistics", context)
    
    async def get_latest_drought_data(
        self,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get the latest drought data for regions.
        
        Business logic:
        - Returns most recent drought indices and classifications
        - Useful for current condition monitoring
        
        Args:
            region_type: Type of region (None = all)
            region_id: Specific region (None = all)
            limit: Maximum number of results
            context: Service context
            
        Returns:
            ServiceResult with latest drought data
        """
        self._log_operation(
            "get_latest_drought_data",
            context=context,
            region_type=region_type
        )
        
        try:
            # Validate inputs
            if region_type:
                self._validate_region_type(region_type)
            
            limit = min(limit, 1000)
            
            # Get latest data from repository
            latest_data = await self.drought_repo.get_latest_drought_data(
                region_type=region_type,
                region_id=region_id,
                limit=limit
            )
            
            return ServiceResult.ok(
                data=latest_data,
                metadata={
                    "count": len(latest_data),
                    "limit": limit,
                    "data_type": "latest"
                }
            )
            
        except Exception as e:
            raise self._handle_error(e, "get_latest_drought_data", context)
    
    async def export_drought_data(
        self,
        dataset: Optional[str] = None,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        context: Optional[ServiceContext] = None
    ) -> AsyncIterator[Dict[str, Any]]:
        """Export drought data as a streaming iterator for large datasets.
        
        This method is optimized for exporting large amounts of drought data
        without loading everything into memory at once.
        
        Args:
            dataset: Dataset to export
            region_type: Type of region
            region_id: Specific region identifier
            start_date: Start date for data
            end_date: End date for data
            context: Service context
            
        Yields:
            Individual drought data points
        """
        self._log_operation(
            "export_drought_data",
            context=context,
            dataset=dataset,
            region_type=region_type
        )
        
        try:
            # Stream data in batches
            offset = 0
            batch_size = 1000
            
            while True:
                result = await self.query_indices(
                    dataset=dataset,
                    region_type=region_type,
                    region_id=region_id,
                    start_date=start_date,
                    end_date=end_date,
                    limit=batch_size,
                    use_cache=False,  # Don't cache during exports
                    context=context
                )
                
                if not result.data:
                    break
                
                for item in result.data:
                    yield item
                
                # Check if there are more results
                if not result.metadata.get("has_more", False):
                    break
                
                offset += batch_size
            
            self.logger.info(
                "Drought data export completed",
                extra={"dataset": dataset, "region_type": region_type}
            )
            
        except Exception as e:
            self.logger.error(f"Export error: {e}")
            raise self._handle_error(e, "export_drought_data", context)
    
    # Private helper methods
    
    def _validate_query_params(
        self,
        limit: int,
        start_date: Optional[date],
        end_date: Optional[date]
    ) -> None:
        """Validate common query parameters."""
        if limit < 1:
            raise ValidationError("Limit must be at least 1", field="limit")
        if limit > 10000:
            raise ValidationError("Limit cannot exceed 10000", field="limit")
        
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
    
    def _validate_date_range(self, start_date: date, end_date: date) -> None:
        """Validate date range."""
        if start_date > end_date:
            raise ValidationError(
                "Start date must be before or equal to end date",
                field="date_range"
            )
        
        # Prevent unreasonably large date ranges
        days_diff = (end_date - start_date).days
        if days_diff > 3650:  # 10 years
            raise ValidationError(
                "Date range cannot exceed 10 years",
                field="date_range"
            )
    
    def _validate_time_range(
        self,
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> None:
        """Validate time range for vector events."""
        if start_time and end_time and start_time > end_time:
            raise ValidationError(
                "Start time must be before or equal to end time",
                field="time_range"
            )
    
    def _validate_dataset(self, dataset: str) -> None:
        """Validate dataset name."""
        valid_datasets = {"spi", "spei", "pdsi", "palmer", "standardized"}
        if dataset.lower() not in valid_datasets:
            raise ValidationError(
                f"Invalid dataset: {dataset}. Must be one of: {', '.join(valid_datasets)}",
                field="dataset"
            )
    
    def _validate_timescale(self, timescale: str) -> None:
        """Validate timescale value."""
        valid_timescales = {
            "1-month", "3-month", "6-month", "9-month", "12-month",
            "24-month", "48-month", "60-month"
        }
        if timescale not in valid_timescales:
            raise ValidationError(
                f"Invalid timescale: {timescale}. Must be one of: {', '.join(valid_timescales)}",
                field="timescale"
            )
    
    def _validate_region_type(self, region_type: str) -> None:
        """Validate region type."""
        valid_types = {"state", "county", "basin", "huc2", "huc4", "huc8", "climate_division"}
        if region_type.lower() not in valid_types:
            raise ValidationError(
                f"Invalid region type: {region_type}. Must be one of: {', '.join(valid_types)}",
                field="region_type"
            )
    
    def _validate_layer(self, layer: str) -> None:
        """Validate vector layer type."""
        valid_layers = {"drought_boundaries", "fire_risk", "water_stress", "agricultural_impact"}
        if layer.lower() not in valid_layers:
            raise ValidationError(
                f"Invalid layer: {layer}. Must be one of: {', '.join(valid_layers)}",
                field="layer"
            )
    
    def _filter_by_tenant(
        self,
        data: List[Dict[str, Any]],
        tenant_id: str
    ) -> List[Dict[str, Any]]:
        """Apply tenant-based filtering (if applicable).
        
        In a multi-tenant system, filter data by tenant access rights.
        """
        # Placeholder: In production, implement actual tenant filtering
        return data
    
    def _enhance_statistics(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance statistics with business logic calculations."""
        # Add drought severity classification
        if stats.get("avg_index_value"):
            avg_value = stats["avg_index_value"]
            if avg_value >= 2.0:
                stats["overall_condition"] = "extremely_wet"
            elif avg_value >= 1.5:
                stats["overall_condition"] = "very_wet"
            elif avg_value >= 1.0:
                stats["overall_condition"] = "moderately_wet"
            elif avg_value >= -0.99:
                stats["overall_condition"] = "normal"
            elif avg_value >= -1.49:
                stats["overall_condition"] = "moderate_drought"
            elif avg_value >= -1.99:
                stats["overall_condition"] = "severe_drought"
            else:
                stats["overall_condition"] = "extreme_drought"
        
        # Calculate drought frequency
        total_obs = stats.get("total_observations", 0)
        if total_obs > 0:
            drought_episodes = stats.get("drought_episodes", 0)
            severe_episodes = stats.get("severe_drought_episodes", 0)
            
            stats["drought_frequency"] = round(drought_episodes / total_obs * 100, 2)
            stats["severe_drought_frequency"] = round(severe_episodes / total_obs * 100, 2)
        
        return stats
