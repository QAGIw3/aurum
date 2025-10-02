"""Scenario repository for scenario modeling operations.

Provides domain-specific operations for scenarios and scenario runs.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime
from uuid import UUID

from .base import BaseRepository
from ..dao import PostgresDAO, TrinoDAO

logger = logging.getLogger(__name__)


class ScenarioRepository(BaseRepository):
    """Repository for scenario operations.
    
    Scenarios represent what-if analyses and modeling runs.
    Metadata is stored in Postgres, outputs in Iceberg (via Trino).
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._postgres_dao: Optional[PostgresDAO] = None
        self._trino_dao: Optional[TrinoDAO] = None
    
    async def initialize(self) -> None:
        """Initialize repository and its DAOs."""
        self._postgres_dao = PostgresDAO(self.settings)
        self._trino_dao = TrinoDAO(self.settings)
        await self._postgres_dao.initialize()
        await self._trino_dao.initialize()
    
    async def close(self) -> None:
        """Close repository and its DAOs."""
        if self._postgres_dao:
            await self._postgres_dao.close()
        if self._trino_dao:
            await self._trino_dao.close()
    
    async def __aenter__(self) -> ScenarioRepository:
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
    
    async def find_by_id(self, scenario_id: UUID) -> Optional[Dict[str, Any]]:
        """Find scenario by ID.
        
        Args:
            scenario_id: Scenario UUID
            
        Returns:
            Scenario metadata or None
        """
        query = """
            SELECT *
            FROM scenarios
            WHERE id = :scenario_id
        """
        
        return await self._postgres_dao.execute_query_single(
            query,
            {"scenario_id": str(scenario_id)}
        )
    
    async def list_scenarios(
        self,
        tenant_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List scenarios with pagination.
        
        Args:
            tenant_id: Filter by tenant (for RLS)
            limit: Maximum number of results
            offset: Pagination offset
            
        Returns:
            List of scenarios
        """
        query = """
            SELECT *
            FROM scenarios
            WHERE 1=1
        """
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        
        if tenant_id:
            query += " AND tenant_id = :tenant_id"
            params["tenant_id"] = tenant_id
        
        query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        
        return await self._postgres_dao.execute_query(query, params)
    
    async def create_scenario(
        self,
        name: str,
        description: Optional[str] = None,
        assumptions: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new scenario.
        
        Args:
            name: Scenario name
            description: Scenario description
            assumptions: Scenario assumptions/parameters
            tenant_id: Tenant identifier
            
        Returns:
            Created scenario
        """
        import json
        from uuid import uuid4
        
        scenario_id = uuid4()
        query = """
            INSERT INTO scenarios (id, name, description, assumptions, tenant_id, created_at)
            VALUES (:id, :name, :description, :assumptions, :tenant_id, NOW())
            RETURNING *
        """
        
        params = {
            "id": str(scenario_id),
            "name": name,
            "description": description,
            "assumptions": json.dumps(assumptions) if assumptions else None,
            "tenant_id": tenant_id
        }
        
        return await self._postgres_dao.execute_query_single(query, params)
    
    async def get_scenario_outputs(
        self,
        scenario_id: UUID,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get scenario outputs from Iceberg.
        
        Args:
            scenario_id: Scenario UUID
            limit: Maximum number of results
            
        Returns:
            List of scenario outputs
        """
        query = """
            SELECT *
            FROM iceberg.scenarios.scenario_outputs
            WHERE scenario_id = :scenario_id
            ORDER BY created_at DESC
            LIMIT :limit
        """
        
        return await self._trino_dao.execute_query(
            query,
            {"scenario_id": str(scenario_id), "limit": limit}
        )

