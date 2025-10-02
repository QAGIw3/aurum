"""Metadata repository for dimension and catalog operations.

Provides domain-specific operations for metadata and dimensional data.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import BaseRepository
from ..dao import TrinoDAO

logger = logging.getLogger(__name__)


class MetadataRepository(BaseRepository):
    """Repository for metadata operations.
    
    Metadata includes:
    - Dimension tables (ISOs, markets, locations, products)
    - Data catalogs
    - Reference data
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._trino_dao: Optional[TrinoDAO] = None
    
    async def initialize(self) -> None:
        """Initialize repository and its DAOs."""
        self._trino_dao = TrinoDAO(self.settings)
        await self._trino_dao.initialize()
    
    async def close(self) -> None:
        """Close repository and its DAOs."""
        if self._trino_dao:
            await self._trino_dao.close()
    
    async def __aenter__(self) -> MetadataRepository:
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
    
    async def get_dimensions(
        self,
        dataset: str,
        dimension: str
    ) -> List[str]:
        """Get unique values for a dimension.
        
        Args:
            dataset: Dataset name (e.g., "curves", "iso_metrics")
            dimension: Dimension name (e.g., "iso", "market", "location")
            
        Returns:
            List of unique dimension values
        """
        # Map dataset to table
        table_mapping = {
            "curves": "iceberg.market.curve_observation",
            "iso_metrics": "timescale.public.iso_metrics",
            "eia": "iceberg.external.eia_observations",
        }
        
        table = table_mapping.get(dataset)
        if not table:
            raise ValueError(f"Unknown dataset: {dataset}")
        
        query = f"""
            SELECT DISTINCT {dimension}
            FROM {table}
            WHERE {dimension} IS NOT NULL
            ORDER BY {dimension}
        """
        
        results = await self._trino_dao.execute_query(query)
        return [row[dimension] for row in results]
    
    async def get_all_dimensions(
        self,
        dataset: str
    ) -> Dict[str, List[str]]:
        """Get all dimensions for a dataset.
        
        Args:
            dataset: Dataset name
            
        Returns:
            Dictionary mapping dimension names to their values
        """
        # Common dimensions by dataset
        dimension_mapping = {
            "curves": ["iso", "market", "location", "product", "block", "tenor_type"],
            "iso_metrics": ["iso", "metric_type", "region"],
            "eia": ["series_id", "frequency", "unit"],
        }
        
        dimensions = dimension_mapping.get(dataset, [])
        result = {}
        
        for dimension in dimensions:
            try:
                values = await self.get_dimensions(dataset, dimension)
                result[dimension] = values
            except Exception as e:
                logger.warning(f"Failed to get dimension {dimension}: {e}")
                result[dimension] = []
        
        return result
    
    async def search_metadata(
        self,
        search_term: str,
        datasets: Optional[List[str]] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search metadata across datasets.
        
        Args:
            search_term: Search term
            datasets: List of datasets to search (None = all)
            limit: Maximum number of results
            
        Returns:
            List of matching metadata entries
        """
        # This is a simplified implementation
        # In production, would use a proper search index (Elasticsearch, etc.)
        
        results = []
        search_datasets = datasets or ["curves", "iso_metrics", "eia"]
        
        for dataset in search_datasets:
            try:
                # Search in common text fields
                query = f"""
                    SELECT '{dataset}' as dataset, *
                    FROM iceberg.metadata.{dataset}_catalog
                    WHERE LOWER(name) LIKE :search_term
                       OR LOWER(description) LIKE :search_term
                    LIMIT :limit
                """
                
                matches = await self._trino_dao.execute_query(
                    query,
                    {"search_term": f"%{search_term.lower()}%", "limit": limit}
                )
                results.extend(matches)
                
            except Exception as e:
                logger.warning(f"Failed to search {dataset}: {e}")
        
        return results[:limit]

