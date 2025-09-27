from __future__ import annotations

"""Scenario domain service with DAO pattern implementation."""

from typing import Any, Dict, List, Optional, Tuple

from .base_service import QueryableServiceInterface
from ..dao.scenario_dao import ScenarioDao


class ScenarioService(QueryableServiceInterface):
    """Scenario domain service implementing business logic and data access through DAO.
    
    Implements Phase 1.3 service layer decomposition with clear domain boundaries.
    """
    
    def __init__(self):
        self._dao = ScenarioDao()
    
    # Legacy compatibility method
    @staticmethod
    def create_scenario(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        from aurum.api import service as legacy

        return legacy.create_scenario(*_args, **_kwargs)  # type: ignore[attr-defined]
    
    # New DAO-based methods that replace service.py functions
    def query_scenario_outputs(
        self,
        *,
        scenario_id: str,
        tenant_id: Optional[str] = None,
        table_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        offset: int = 0,
        limit: int = 1000,
        cursor_after: Optional[Dict[str, Any]] = None,
        cursor_before: Optional[Dict[str, Any]] = None,
        descending: bool = False,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query scenario outputs data."""
        return self._dao.query_scenario_outputs(
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            table_name=table_name,
            columns=columns,
            offset=offset,
            limit=limit,
            cursor_after=cursor_after,
            cursor_before=cursor_before,
            descending=descending,
        )
    
    def query_scenario_metrics_latest(
        self,
        *,
        scenario_id: str,
        tenant_id: Optional[str] = None,
        metric_name: Optional[str] = None,
        limit: int = 1000,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query latest scenario metrics data."""
        return self._dao.query_scenario_metrics_latest(
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            metric_name=metric_name,
            limit=limit,
        )
    
    def invalidate_scenario_outputs_cache(
        self,
        *,
        tenant_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Invalidate scenario outputs cache."""
        return self._dao.invalidate_scenario_outputs_cache(
            tenant_id=tenant_id,
            scenario_id=scenario_id,
        )
    
    # ServiceInterface implementation
    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate scenario-specific caches."""
        return self.invalidate_scenario_outputs_cache()
    
    # QueryableServiceInterface implementation
    async def query_data(
        self, 
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query scenario data with pagination and filtering."""
        filters = filters or {}
        scenario_id = filters.get("scenario_id")
        if not scenario_id:
            return []
            
        data, _ = self.query_scenario_outputs(
            scenario_id=scenario_id,
            tenant_id=filters.get("tenant_id"),
            table_name=filters.get("table_name"),
            offset=offset,
            limit=limit,
        )
        return data

