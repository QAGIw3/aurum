"""Drought repository for drought index and USDM data operations.

Provides domain-specific operations for drought monitoring and analysis.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import date

from .base import BaseRepository
from ..dao import TrinoDAO

logger = logging.getLogger(__name__)


class DroughtRepository(BaseRepository):
    """Repository for drought data operations.

    Drought data includes:
    - Drought indices (SPI, SPEI, PDSI)
    - USDM (U.S. Drought Monitor) classifications
    - Regional drought conditions
    - Historical drought patterns
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

    async def __aenter__(self) -> DroughtRepository:
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def query_drought_indices(
        self,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        dataset: Optional[str] = None,
        index_id: Optional[str] = None,
        timescale: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        """Query drought indices with optional filtering.

        Args:
            region_type: Type of region (e.g., "state", "county", "basin")
            region_id: Specific region identifier
            dataset: Dataset name (e.g., "spi", "spei", "pdsi")
            index_id: Specific index identifier
            timescale: Time scale (e.g., "1-month", "3-month", "12-month")
            start_date: Start date for data
            end_date: End date for data
            limit: Maximum number of results

        Returns:
            List of drought index data points
        """
        query = """
            SELECT *
            FROM iceberg.climate.drought_indices
            WHERE 1=1
        """
        params: Dict[str, Any] = {"limit": limit}

        if region_type:
            query += " AND region_type = :region_type"
            params["region_type"] = region_type

        if region_id:
            query += " AND region_id = :region_id"
            params["region_id"] = region_id

        if dataset:
            query += " AND dataset = :dataset"
            params["dataset"] = dataset

        if index_id:
            query += " AND index_id = :index_id"
            params["index_id"] = index_id

        if timescale:
            query += " AND timescale = :timescale"
            params["timescale"] = timescale

        if start_date:
            query += " AND date >= :start_date"
            params["start_date"] = start_date.isoformat()

        if end_date:
            query += " AND date <= :end_date"
            params["end_date"] = end_date.isoformat()

        query += " ORDER BY date DESC, region_id LIMIT :limit"

        return await self._trino_dao.execute_query(query, params)

    async def query_usdm_data(
        self,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500
    ) -> List[Dict[str, Any]]:
        """Query USDM (U.S. Drought Monitor) data.

        Args:
            region_type: Type of region
            region_id: Specific region identifier
            start_date: Start date for data
            end_date: End date for data
            limit: Maximum number of results

        Returns:
            List of USDM classification data
        """
        query = """
            SELECT *
            FROM iceberg.climate.usdm_classifications
            WHERE 1=1
        """
        params: Dict[str, Any] = {"limit": limit}

        if region_type:
            query += " AND region_type = :region_type"
            params["region_type"] = region_type

        if region_id:
            query += " AND region_id = :region_id"
            params["region_id"] = region_id

        if start_date:
            query += " AND valid_date >= :start_date"
            params["start_date"] = start_date.isoformat()

        if end_date:
            query += " AND valid_date <= :end_date"
            params["end_date"] = end_date.isoformat()

        query += " ORDER BY valid_date DESC, region_id LIMIT :limit"

        return await self._trino_dao.execute_query(query, params)

    async def get_drought_statistics(
        self,
        region_type: str,
        region_id: str,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """Get drought statistics for a region over a time period.

        Args:
            region_type: Type of region
            region_id: Region identifier
            start_date: Start date for analysis
            end_date: End date for analysis

        Returns:
            Dictionary with drought statistics
        """
        query = """
            SELECT
                COUNT(*) as total_observations,
                AVG(index_value) as avg_index_value,
                MIN(index_value) as min_index_value,
                MAX(index_value) as max_index_value,
                COUNT(CASE WHEN index_value < -1.0 THEN 1 END) as drought_episodes,
                COUNT(CASE WHEN index_value < -2.0 THEN 1 END) as severe_drought_episodes
            FROM iceberg.climate.drought_indices
            WHERE region_type = :region_type
              AND region_id = :region_id
              AND date >= :start_date
              AND date <= :end_date
        """

        result = await self._trino_dao.execute_query_single(
            query,
            {
                "region_type": region_type,
                "region_id": region_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        )

        return result if result else {
            "total_observations": 0,
            "avg_index_value": 0.0,
            "min_index_value": 0.0,
            "max_index_value": 0.0,
            "drought_episodes": 0,
            "severe_drought_episodes": 0
        }

    async def get_latest_drought_data(
        self,
        region_type: Optional[str] = None,
        region_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get the latest drought data for regions.

        Args:
            region_type: Type of region (None = all)
            region_id: Specific region (None = all)
            limit: Maximum number of results

        Returns:
            List of latest drought data points
        """
        query = """
            SELECT *
            FROM iceberg.climate.drought_indices
            WHERE 1=1
        """
        params: Dict[str, Any] = {"limit": limit}

        if region_type:
            query += " AND region_type = :region_type"
            params["region_type"] = region_type

        if region_id:
            query += " AND region_id = :region_id"
            params["region_id"] = region_id

        query += """
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY region_type, region_id, dataset, index_id
                ORDER BY date DESC
            ) = 1
            LIMIT :limit
        """

        return await self._trino_dao.execute_query(query, params)

