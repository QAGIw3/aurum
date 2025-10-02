"""Pytest fixtures for integration tests."""

import asyncio
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import create_engine, event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from aurum.infrastructure.persistence.models.curve_model import Base as CurveBase
from aurum.infrastructure.persistence.models.iso_model import Base as IsoBase
from aurum.infrastructure.persistence.models.ppa_model import Base as PPABase
from aurum.infrastructure.messaging.event_bus import InMemoryEventBus


# Test database URL - using in-memory SQLite for fast tests
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def async_engine():
    """Create async engine for tests."""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False,
        poolclass=StaticPool,  # Use single connection for in-memory DB
    )
    
    # Create all tables
    async with engine.begin() as conn:
        # Import all base classes to ensure tables are created
        await conn.run_sync(CurveBase.metadata.create_all)
        await conn.run_sync(IsoBase.metadata.create_all)
        await conn.run_sync(PPABase.metadata.create_all)
    
    yield engine
    
    # Cleanup
    await engine.dispose()


@pytest_asyncio.fixture
async def async_session(async_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create async session for tests."""
    async_session_maker = async_sessionmaker(
        async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    async with async_session_maker() as session:
        yield session
        await session.rollback()  # Rollback any changes


@pytest.fixture
def event_bus():
    """Create event bus for tests."""
    return InMemoryEventBus()


@pytest_asyncio.fixture
async def db_session(async_session):
    """Alias for async_session for backwards compatibility."""
    return async_session
