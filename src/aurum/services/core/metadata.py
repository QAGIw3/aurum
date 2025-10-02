"""Metadata service for dimension and catalog operations.

Implements business logic for metadata queries, dimension discovery,
and catalog search.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ..base import BaseService, ServiceContext, ServiceResult, ServiceError, ValidationError, NotFoundError
from aurum.data.repositories import MetadataRepository

logger = logging.getLogger(__name__)


class MetadataService(BaseService):
    """Service for metadata operations.
    
    Metadata includes:
    - Dimension tables (ISOs, markets, locations, products)
    - Data catalogs
    - Reference data
    
    This service:
    - Validates metadata queries
    - Provides dimension discovery
    - Implements catalog search
    - Caches frequently accessed metadata
    """
    
    def __init__(self, metadata_repository: MetadataRepository):
        """Initialize service with dependencies.
        
        Args:
            metadata_repository: Repository for metadata access
        """
        super().__init__()
        self.metadata_repo = metadata_repository
    
    async def get_dimensions(
        self,
        dataset: str,
        dimension: str,
        context: Optional[ServiceContext] = None
    ) -> ServiceResult[List[str]]:
        """Get unique values for a dimension.
        
        Business logic:
        - Validates dataset and dimension names
        - Returns sorted unique values
        - Caches results for performance
        
        Args:
            dataset: Dataset name (e.g., "curves", "iso_metrics")
            dimension: Dimension name (e.g., "iso", "market")
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
            
            # Get dimensions from repository
            values = await self.metadata_repo.get_dimensions(dataset, dimension)
            
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
                    "count": len(values)
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

