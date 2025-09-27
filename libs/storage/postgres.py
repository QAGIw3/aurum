"""PostgreSQL metadata repository implementation using async SQLAlchemy 2.0."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, func, and_, text, update, delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

from libs.common.config import DatabaseSettings
from .ports import MetadataRepository

logger = logging.getLogger(__name__)


class PostgresMetaRepo(MetadataRepository):
    """PostgreSQL implementation of MetadataRepository using async SQLAlchemy 2.0."""
    
    def __init__(self, db_settings: DatabaseSettings):
        self.db_settings = db_settings
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[sessionmaker] = None
        
    async def _get_engine(self) -> AsyncEngine:
        """Get or create async database engine."""
        if self._engine is None:
            self._engine = create_async_engine(
                self.db_settings.postgres_dsn,
                pool_size=self.db_settings.pool_size,
                max_overflow=self.db_settings.max_overflow,
                pool_timeout=self.db_settings.pool_timeout,
                pool_recycle=self.db_settings.pool_recycle,
                echo=False,  # Set to True for SQL debugging
            )
            self._session_factory = sessionmaker(
                self._engine, 
                class_=AsyncSession,
                expire_on_commit=False
            )
        return self._engine
    
    async def _get_session(self) -> AsyncSession:
        """Get async database session."""
        if self._session_factory is None:
            await self._get_engine()
        return self._session_factory()
    
    async def get_scenario(
        self,
        scenario_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get scenario by ID."""
        async with await self._get_session() as session:
            query = text("""
                SELECT 
                    id,
                    name,
                    description,
                    parameters,
                    status,
                    created_by,
                    created_at,
                    updated_at,
                    version
                FROM scenarios 
                WHERE id = :scenario_id
            """)
            
            result = await session.execute(query, {'scenario_id': scenario_id})
            row = result.fetchone()
            
            if not row:
                return None
                
            return {
                'id': row.id,
                'name': row.name,
                'description': row.description,
                'parameters': json.loads(row.parameters) if row.parameters else {},
                'status': row.status,
                'created_by': row.created_by,
                'created_at': row.created_at.isoformat() if row.created_at else None,
                'updated_at': row.updated_at.isoformat() if row.updated_at else None,
                'version': row.version,
            }
    
    async def create_scenario(
        self,
        scenario: Dict[str, Any],
    ) -> str:
        """Create a new scenario, return ID."""
        async with await self._get_session() as session:
            query = text("""
                INSERT INTO scenarios (
                    name, description, parameters, status, created_by, 
                    created_at, updated_at, version
                )
                VALUES (
                    :name, :description, :parameters, :status, :created_by,
                    :now, :now, 1
                )
                RETURNING id
            """)
            
            now = datetime.utcnow()
            result = await session.execute(query, {
                'name': scenario.get('name'),
                'description': scenario.get('description'),
                'parameters': json.dumps(scenario.get('parameters', {})),
                'status': scenario.get('status', 'draft'),
                'created_by': scenario.get('created_by'),
                'now': now,
            })
            
            scenario_id = result.scalar()
            await session.commit()
            
            return str(scenario_id)
    
    async def update_scenario(
        self,
        scenario_id: str,
        updates: Dict[str, Any],
    ) -> bool:
        """Update scenario, return success."""
        async with await self._get_session() as session:
            # Build dynamic update query
            set_clauses = []
            params = {'scenario_id': scenario_id, 'now': datetime.utcnow()}
            
            if 'name' in updates:
                set_clauses.append('name = :name')
                params['name'] = updates['name']
                
            if 'description' in updates:
                set_clauses.append('description = :description')
                params['description'] = updates['description']
                
            if 'parameters' in updates:
                set_clauses.append('parameters = :parameters')
                params['parameters'] = json.dumps(updates['parameters'])
                
            if 'status' in updates:
                set_clauses.append('status = :status')
                params['status'] = updates['status']
            
            # Always update timestamp and increment version
            set_clauses.extend(['updated_at = :now', 'version = version + 1'])
            
            if not set_clauses:
                return False
                
            query = text(f"""
                UPDATE scenarios 
                SET {', '.join(set_clauses)}
                WHERE id = :scenario_id
            """)
            
            result = await session.execute(query, params)
            await session.commit()
            
            return result.rowcount > 0
    
    async def list_scenarios(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List scenarios with pagination."""
        async with await self._get_session() as session:
            # Count query
            count_result = await session.execute(
                text("SELECT COUNT(*) FROM scenarios")
            )
            total_count = count_result.scalar() or 0
            
            # Data query
            query = text("""
                SELECT 
                    id,
                    name,
                    description,
                    parameters,
                    status,
                    created_by,
                    created_at,
                    updated_at,
                    version
                FROM scenarios 
                ORDER BY updated_at DESC, created_at DESC
                LIMIT :limit OFFSET :offset
            """)
            
            result = await session.execute(query, {
                'limit': limit or 100,
                'offset': offset or 0,
            })
            
            scenarios = []
            for row in result.fetchall():
                scenario = {
                    'id': row.id,
                    'name': row.name,
                    'description': row.description,
                    'parameters': json.loads(row.parameters) if row.parameters else {},
                    'status': row.status,
                    'created_by': row.created_by,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'updated_at': row.updated_at.isoformat() if row.updated_at else None,
                    'version': row.version,
                }
                scenarios.append(scenario)
            
            return scenarios, total_count
    
    async def delete_scenario(
        self,
        scenario_id: str,
    ) -> bool:
        """Delete scenario by ID."""
        async with await self._get_session() as session:
            query = text("DELETE FROM scenarios WHERE id = :scenario_id")
            result = await session.execute(query, {'scenario_id': scenario_id})
            await session.commit()
            return result.rowcount > 0
    
    async def get_scenario_runs(
        self,
        scenario_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get runs for a specific scenario."""
        async with await self._get_session() as session:
            # Count query
            count_result = await session.execute(
                text("SELECT COUNT(*) FROM scenario_runs WHERE scenario_id = :scenario_id"),
                {'scenario_id': scenario_id}
            )
            total_count = count_result.scalar() or 0
            
            # Data query
            query = text("""
                SELECT 
                    id,
                    scenario_id,
                    status,
                    started_at,
                    completed_at,
                    error_message,
                    results,
                    metadata
                FROM scenario_runs 
                WHERE scenario_id = :scenario_id
                ORDER BY started_at DESC
                LIMIT :limit OFFSET :offset
            """)
            
            result = await session.execute(query, {
                'scenario_id': scenario_id,
                'limit': limit or 50,
                'offset': offset or 0,
            })
            
            runs = []
            for row in result.fetchall():
                run = {
                    'id': row.id,
                    'scenario_id': row.scenario_id,
                    'status': row.status,
                    'started_at': row.started_at.isoformat() if row.started_at else None,
                    'completed_at': row.completed_at.isoformat() if row.completed_at else None,
                    'error_message': row.error_message,
                    'results': json.loads(row.results) if row.results else {},
                    'metadata': json.loads(row.metadata) if row.metadata else {},
                }
                runs.append(run)
            
            return runs, total_count
    
    async def close(self):
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None