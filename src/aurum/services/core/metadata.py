"""Metadata service for dimension and catalog operations with caching.

Implements business logic for metadata queries, dimension discovery,
catalog search, and reference data management.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Protocol, Tuple

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import MetadataRepository

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


class MetadataService(BaseService):
    """Service for metadata operations with caching and reference data support.
    
    Metadata includes:
    - Dimension tables (ISOs, markets, locations, products)
    - Data catalogs
    - Reference data (units, calendars)
    - ISO locations
    
    This service:
    - Validates metadata queries
    - Provides dimension discovery
    - Implements catalog search
    - Caches frequently accessed metadata
    - Manages reference data (units, calendars, locations)
    """
    
    def __init__(
        self,
        metadata_repository: MetadataRepository,
        cache: Optional[CacheProtocol] = None,
        cache_ttl: int = 600  # 10 minutes default for metadata
    ):
        """Initialize service with dependencies.
        
        Args:
            metadata_repository: Repository for metadata access
            cache: Optional cache implementation
            cache_ttl: Cache time-to-live in seconds (default 600)
        """
        super().__init__()
        self.metadata_repo = metadata_repository
        self.cache = cache
        self.cache_ttl = cache_ttl
        self._cache_namespace = "metadata:v1"
    
    # Caching helper methods
    
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
    
    async def get_dimensions(
        self,
        dataset: str,
        dimension: str,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[str]]:
        """Get unique values for a dimension with optional caching.
        
        Business logic:
        - Validates dataset and dimension names
        - Returns sorted unique values
        - Caches results for performance
        
        Args:
            dataset: Dataset name (e.g., "curves", "iso_metrics")
            dimension: Dimension name (e.g., "iso", "market")
            use_cache: Whether to use caching (default True)
            context: Service context
            
        Returns:
            ServiceResult with list of dimension values
            
        Raises:
            ValidationError: If dataset or dimension invalid
            ServiceError: If operation fails
        """
        self._log_operation(
            "get_dimensions",
            context=context,
            dataset=dataset,
            dimension=dimension
        )
        
        try:
            # Validate inputs
            self._validate_dataset(dataset)
            self._validate_dimension_name(dimension)
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key("dimensions", dataset=dataset, dimension=dimension)
                cached_values = await self._get_from_cache(cache_key)
                if cached_values is not None:
                    return ServiceResult.ok(
                        data=cached_values,
                        metadata={
                            "dataset": dataset,
                            "dimension": dimension,
                            "count": len(cached_values),
                            "source": "cache"
                        }
                    )
            
            # Get dimensions from repository
            values = await self.metadata_repo.get_dimensions(dataset, dimension)
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, values)
            
            self.logger.info(
                f"Retrieved {len(values)} values for {dimension} in {dataset}",
                extra={
                    "dataset": dataset,
                    "dimension": dimension,
                    "count": len(values)
                }
            )
            
            return ServiceResult.ok(
                data=values,
                metadata={
                    "dataset": dataset,
                    "dimension": dimension,
                    "count": len(values),
                    "source": "database"
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_dimensions", context)
    
    async def get_all_dimensions(
        self,
        dataset: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, List[str]]]:
        """Get all dimensions for a dataset.
        
        Args:
            dataset: Dataset name
            context: Service context
            
        Returns:
            ServiceResult with dictionary of dimension name -> values
            
        Raises:
            ValidationError: If dataset invalid
            ServiceError: If operation fails
        """
        self._log_operation("get_all_dimensions", context=context, dataset=dataset)
        
        try:
            self._validate_dataset(dataset)
            
            dimensions = await self.metadata_repo.get_all_dimensions(dataset)
            
            total_values = sum(len(values) for values in dimensions.values())
            
            return ServiceResult.ok(
                data=dimensions,
                metadata={
                    "dataset": dataset,
                    "dimension_count": len(dimensions),
                    "total_values": total_values
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_all_dimensions", context)
    
    async def search_metadata(
        self,
        search_term: str,
        datasets: Optional[List[str]] = None,
        limit: int = 100,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Search metadata across datasets.
        
        Business logic:
        - Validates search term
        - Searches across specified datasets
        - Returns ranked results
        - Enforces result limits
        
        Args:
            search_term: Search query
            datasets: List of datasets to search (None = all)
            limit: Maximum results (max 1000)
            context: Service context
            
        Returns:
            ServiceResult with matching metadata entries
            
        Raises:
            ValidationError: If inputs invalid
            ServiceError: If search fails
        """
        self._log_operation(
            "search_metadata",
            context=context,
            search_term=search_term,
            datasets=datasets
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
            
            if datasets:
                for dataset in datasets:
                    self._validate_dataset(dataset)
            
            # Search metadata
            results = await self.metadata_repo.search_metadata(
                search_term=search_term.strip(),
                datasets=datasets,
                limit=limit
            )
            
            return ServiceResult.ok(
                data=results,
                metadata={
                    "search_term": search_term,
                    "datasets": datasets,
                    "result_count": len(results),
                    "has_more": len(results) == limit
                }
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "search_metadata", context)
    
    async def get_dataset_info(
        self,
        dataset: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get information about a dataset.
        
        Returns metadata about the dataset including:
        - Available dimensions
        - Record count (if available)
        - Last updated timestamp
        - Schema information
        
        Args:
            dataset: Dataset name
            context: Service context
            
        Returns:
            ServiceResult with dataset information
        """
        self._log_operation("get_dataset_info", context=context, dataset=dataset)
        
        try:
            self._validate_dataset(dataset)
            
            # Get dimensions
            dimensions = await self.metadata_repo.get_all_dimensions(dataset)
            
            info = {
                "dataset": dataset,
                "dimensions": list(dimensions.keys()),
                "dimension_count": len(dimensions),
                "available": True
            }
            
            return ServiceResult.ok(
                data=info,
                metadata={"dataset": dataset}
            )
            
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "get_dataset_info", context)
    
    async def list_locations(
        self,
        iso: str,
        limit: int = 100,
        offset: int = 0,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Tuple[List[Dict[str, Any]], int]]:
        """List ISO locations with pagination and caching.
        
        Args:
            iso: ISO/RTO identifier
            limit: Maximum results per page
            offset: Pagination offset
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with (locations list, total count) tuple
        """
        self._log_operation("list_locations", context=context, iso=iso, limit=limit)
        
        try:
            # Validate
            if not iso:
                raise ValidationError("ISO is required", field="iso")
            
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key("locations", iso=iso, limit=limit, offset=offset)
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={"source": "cache", "iso": iso}
                    )
            
            # Get from repository (stub - needs implementation in repository)
            locations = []  # await self.metadata_repo.get_iso_locations(iso)
            paginated = locations[offset:offset + limit]
            total = len(locations)
            result = (paginated, total)
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, result)
            
            return ServiceResult.ok(
                data=result,
                metadata={"source": "database", "iso": iso, "count": len(paginated)}
            )
        except ValidationError:
            raise
        except Exception as e:
            raise self._handle_error(e, "list_locations", context)
    
    async def list_units(
        self,
        limit: int = 100,
        offset: int = 0,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Tuple[List[Dict[str, Any]], int]]:
        """List units with pagination and caching.
        
        Args:
            limit: Maximum results per page
            offset: Pagination offset
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with (units list, total count) tuple
        """
        self._log_operation("list_units", context=context, limit=limit)
        
        try:
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key("units", limit=limit, offset=offset)
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={"source": "cache"}
                    )
            
            # Get from repository (stub - needs implementation)
            units = []  # await self.metadata_repo.get_units()
            paginated = units[offset:offset + limit]
            total = len(units)
            result = (paginated, total)
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, result)
            
            return ServiceResult.ok(
                data=result,
                metadata={"source": "database", "count": len(paginated)}
            )
        except Exception as e:
            raise self._handle_error(e, "list_units", context)
    
    async def list_calendars(
        self,
        limit: int = 100,
        offset: int = 0,
        use_cache: bool = True,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[Tuple[List[Dict[str, Any]], int]]:
        """List calendars with pagination and caching.
        
        Args:
            limit: Maximum results per page
            offset: Pagination offset
            use_cache: Whether to use caching
            context: Service context
            
        Returns:
            ServiceResult with (calendars list, total count) tuple
        """
        self._log_operation("list_calendars", context=context, limit=limit)
        
        try:
            # Try cache first
            cache_key = None
            if use_cache and self.cache:
                cache_key = self._build_cache_key("calendars", limit=limit, offset=offset)
                cached = await self._get_from_cache(cache_key)
                if cached is not None:
                    return ServiceResult.ok(
                        data=cached,
                        metadata={"source": "cache"}
                    )
            
            # Get from repository (stub - needs implementation)
            calendars = []  # await self.metadata_repo.get_calendars()
            paginated = calendars[offset:offset + limit]
            total = len(calendars)
            result = (paginated, total)
            
            # Cache results
            if use_cache and cache_key:
                await self._set_in_cache(cache_key, result)
            
            return ServiceResult.ok(
                data=result,
                metadata={"source": "database", "count": len(paginated)}
            )
        except Exception as e:
            raise self._handle_error(e, "list_calendars", context)
    
    # Private helper methods
    
    def _validate_dataset(self, dataset: str) -> None:
        """Validate dataset name."""
        valid_datasets = ["curves", "iso_metrics", "eia", "scenarios"]
        
        if not dataset:
            raise ValidationError("Dataset is required", field="dataset")
        
        if dataset not in valid_datasets:
            raise ValidationError(
                f"Invalid dataset. Must be one of: {', '.join(valid_datasets)}",
                field="dataset"
            )
    
    def _validate_dimension_name(self, dimension: str) -> None:
        """Validate dimension name."""
        if not dimension:
            raise ValidationError("Dimension is required", field="dimension")
        
        # Check for SQL injection attempts
        dangerous_chars = [";", "--", "/*", "*/", "xp_", "sp_"]
        if any(char in dimension.lower() for char in dangerous_chars):
            raise ValidationError(
                "Invalid dimension name",
                field="dimension"
            )

