"""Repository implementation for Curve aggregate."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import delete as sql_delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ...domain.energy.models.curve import Curve, CurveId
from ...domain.shared_kernel.repositories import Repository
from ..messaging.event_bus import EventBus
from .mappers.curve_mapper import CurveMapper
from .models.curve_model import CurveORM


class CurveRepository(Repository[Curve]):
    """SQLAlchemy implementation of Curve repository."""
    
    def __init__(self, session: AsyncSession, event_bus: Optional[EventBus] = None):
        """Initialize with database session.
        
        Args:
            session: SQLAlchemy async session
            event_bus: Optional event bus for publishing domain events
        """
        self.session = session
        self.mapper = CurveMapper()
        self.event_bus = event_bus
    
    async def get_by_id(self, id: CurveId) -> Optional[Curve]:
        """Retrieve a curve by its ID.
        
        Args:
            id: The curve identifier
            
        Returns:
            The curve if found, None otherwise
        """
        stmt = (
            select(CurveORM)
            .where(CurveORM.id == id.value)
            .options(selectinload(CurveORM.points))
        )
        
        result = await self.session.execute(stmt)
        orm = result.scalar_one_or_none()
        
        if orm is None:
            return None
        
        return self.mapper.to_domain(orm)
    
    async def save(self, aggregate: Curve) -> None:
        """Save a curve aggregate.
        
        Args:
            aggregate: The curve to save
        """
        # Check if curve already exists
        stmt = select(CurveORM).where(CurveORM.id == aggregate.id.value)
        result = await self.session.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            # Update existing
            self.mapper.update_orm(existing, aggregate)
        else:
            # Create new
            orm = self.mapper.to_orm(aggregate)
            self.session.add(orm)
        
        # Publish domain events
        if self.event_bus:
            for event in aggregate.domain_events:
                await self.event_bus.publish(event)
        
        aggregate.clear_events()
    
    async def delete(self, id: CurveId) -> None:
        """Delete a curve by its ID.
        
        Args:
            id: The curve identifier
        """
        stmt = sql_delete(CurveORM).where(CurveORM.id == id.value)
        await self.session.execute(stmt)
    
    async def exists(self, id: CurveId) -> bool:
        """Check if a curve exists.
        
        Args:
            id: The curve identifier
            
        Returns:
            True if the curve exists, False otherwise
        """
        stmt = select(CurveORM.id).where(CurveORM.id == id.value)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none() is not None

