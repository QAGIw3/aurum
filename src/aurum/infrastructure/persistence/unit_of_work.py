"""SQLAlchemy implementation of Unit of Work pattern."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from ...application.common.unit_of_work import UnitOfWork


class SqlAlchemyUnitOfWork(UnitOfWork):
    """SQLAlchemy-based Unit of Work implementation.
    
    Manages database transactions using SQLAlchemy async sessions.
    """
    
    def __init__(self, session_factory):
        """Initialize with a session factory.
        
        Args:
            session_factory: Callable that returns AsyncSession
        """
        self.session_factory = session_factory
        self.session: Optional[AsyncSession] = None
    
    async def __aenter__(self) -> SqlAlchemyUnitOfWork:
        """Enter the unit of work context."""
        self.session = self.session_factory()
        return self
    
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the unit of work context."""
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
        
        if self.session:
            await self.session.close()
            self.session = None
    
    async def commit(self) -> None:
        """Commit the transaction."""
        if self.session:
            await self.session.commit()
    
    async def rollback(self) -> None:
        """Rollback the transaction."""
        if self.session:
            await self.session.rollback()

