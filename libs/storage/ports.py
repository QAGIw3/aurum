"""Repository interfaces for storage abstraction with hard boundaries."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from libs.core import CurveKey, PriceObservation


class SeriesRepository(ABC):
    """Abstract repository for time series data operations."""
    
    @abstractmethod
    async def get_observations(
        self,
        curve: CurveKey,
        start_date: date,
        end_date: date,
        limit: Optional[int] = None,
    ) -> List[PriceObservation]:
        """Get price observations for a curve within date range."""
        pass
    
    @abstractmethod
    async def store_observations(
        self,
        observations: List[PriceObservation],
    ) -> int:
        """Store price observations, return count of inserted rows."""
        pass
    
    @abstractmethod
    async def get_curve_metadata(
        self,
        curve: CurveKey,
    ) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific curve."""
        pass
    
    @abstractmethod
    async def list_curves(
        self,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Tuple[List[CurveKey], int]:
        """List available curves with optional filtering."""
        pass


class MetadataRepository(ABC):
    """Abstract repository for metadata operations."""
    
    @abstractmethod
    async def get_scenario(
        self,
        scenario_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get scenario by ID."""
        pass
    
    @abstractmethod
    async def create_scenario(
        self,
        scenario: Dict[str, Any],
    ) -> str:
        """Create a new scenario, return ID."""
        pass
    
    @abstractmethod
    async def update_scenario(
        self,
        scenario_id: str,
        updates: Dict[str, Any],
    ) -> bool:
        """Update scenario, return success."""
        pass
    
    @abstractmethod
    async def list_scenarios(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List scenarios with pagination."""
        pass


class AnalyticRepository(ABC):
    """Abstract repository for read-heavy analytical queries."""
    
    @abstractmethod
    async def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Execute analytical query and return results."""
        pass
    
    @abstractmethod
    async def get_table_stats(
        self,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get table statistics for query optimization."""
        pass
    
    @abstractmethod
    async def get_aggregated_data(
        self,
        table: str,
        groupby_columns: List[str],
        aggregations: Dict[str, str],
        filters: Optional[Dict[str, Any]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Get pre-aggregated data with time filters."""
        pass


class ReadOnlyRepository(ABC):
    """Abstract repository for read-only operations (ClickHouse, etc)."""
    
    @abstractmethod
    async def query_raw(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Execute raw query and return results."""
        pass
    
    @abstractmethod
    async def get_time_series(
        self,
        table: str,
        time_column: str,
        value_columns: List[str],
        start_time: datetime,
        end_time: datetime,
        filters: Optional[Dict[str, Any]] = None,
        bucket_size: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get time series data with optional bucketing."""
        pass