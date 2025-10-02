"""Integration tests for Curve repository."""

from datetime import datetime
from decimal import Decimal

import pytest

from aurum.domain.energy.models.curve import (
    Curve,
    CurveId,
    CurveMetadata,
    CurvePoint,
    TenorType,
)
from aurum.domain.shared_kernel.value_objects import TenantId
from aurum.infrastructure.persistence.curve_repository import CurveRepository
from aurum.infrastructure.persistence.unit_of_work import SqlAlchemyUnitOfWork


@pytest.mark.asyncio
class TestCurveRepository:
    """Integration tests for CurveRepository."""
    
    async def test_save_and_retrieve_curve(self, async_session, event_bus):
        """Test saving and retrieving a curve."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        
        curve_id = CurveId.generate()
        tenant_id = TenantId.generate()
        metadata = CurveMetadata(
            curve_key="TEST_CURVE",
            as_of_date=datetime.utcnow(),
            currency="USD",
            tenor_type=TenorType.MONTHLY,
        )
        points = [
            CurvePoint(tenor=Decimal('1'), value=Decimal('100')),
            CurvePoint(tenor=Decimal('2'), value=Decimal('105')),
            CurvePoint(tenor=Decimal('3'), value=Decimal('103')),
        ]
        
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=points,
        )
        
        # Act
        await repository.save(curve)
        await async_session.commit()
        
        retrieved = await repository.get_by_id(curve_id)
        
        # Assert
        assert retrieved is not None
        assert retrieved.id == curve_id
        assert retrieved.tenant_id == tenant_id
        assert retrieved.metadata.curve_key == "TEST_CURVE"
        assert len(retrieved.points) == 3
        assert retrieved.points[0].tenor == Decimal('1')
        assert retrieved.points[0].value == Decimal('100')
    
    async def test_save_publishes_events(self, async_session, event_bus):
        """Test that saving a curve publishes domain events."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        
        curve_id = CurveId.generate()
        tenant_id = TenantId.generate()
        metadata = CurveMetadata(
            curve_key="EVENT_TEST",
            as_of_date=datetime.utcnow(),
        )
        points = [CurvePoint(tenor=Decimal('1'), value=Decimal('100'))]
        
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=points,
        )
        
        # Add a point to generate an event
        curve.add_point(CurvePoint(tenor=Decimal('2'), value=Decimal('105')))
        
        # Track published events
        published_events = []
        
        async def track_event(event):
            published_events.append(event)
        
        from aurum.domain.energy.models.curve import CurvePointAddedEvent
        from aurum.infrastructure.messaging.event_handler import EventHandler
        
        class TrackingHandler(EventHandler):
            async def handle(self, event):
                await track_event(event)
        
        event_bus.subscribe(CurvePointAddedEvent, TrackingHandler())
        
        # Act
        await repository.save(curve)
        await async_session.commit()
        
        # Assert
        assert len(published_events) == 1
        assert published_events[0].tenor == Decimal('2')
    
    async def test_update_existing_curve(self, async_session, event_bus):
        """Test updating an existing curve."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        
        curve_id = CurveId.generate()
        tenant_id = TenantId.generate()
        metadata = CurveMetadata(
            curve_key="UPDATE_TEST",
            as_of_date=datetime.utcnow(),
        )
        points = [CurvePoint(tenor=Decimal('1'), value=Decimal('100'))]
        
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=points,
        )
        
        await repository.save(curve)
        await async_session.commit()
        
        # Act - Retrieve and update
        retrieved = await repository.get_by_id(curve_id)
        retrieved.add_point(CurvePoint(tenor=Decimal('2'), value=Decimal('105')))
        
        await repository.save(retrieved)
        await async_session.commit()
        
        # Assert
        updated = await repository.get_by_id(curve_id)
        assert len(updated.points) == 2
        assert updated.points[1].tenor == Decimal('2')
    
    async def test_delete_curve(self, async_session, event_bus):
        """Test deleting a curve."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        
        curve_id = CurveId.generate()
        tenant_id = TenantId.generate()
        metadata = CurveMetadata(
            curve_key="DELETE_TEST",
            as_of_date=datetime.utcnow(),
        )
        points = [CurvePoint(tenor=Decimal('1'), value=Decimal('100'))]
        
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=points,
        )
        
        await repository.save(curve)
        await async_session.commit()
        
        # Act
        await repository.delete(curve_id)
        await async_session.commit()
        
        # Assert
        retrieved = await repository.get_by_id(curve_id)
        assert retrieved is None
        assert not await repository.exists(curve_id)
    
    async def test_exists_check(self, async_session, event_bus):
        """Test checking if a curve exists."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        
        curve_id = CurveId.generate()
        tenant_id = TenantId.generate()
        metadata = CurveMetadata(
            curve_key="EXISTS_TEST",
            as_of_date=datetime.utcnow(),
        )
        points = [CurvePoint(tenor=Decimal('1'), value=Decimal('100'))]
        
        curve = Curve(
            id=curve_id,
            tenant_id=tenant_id,
            metadata=metadata,
            points=points,
        )
        
        # Act & Assert - Before save
        assert not await repository.exists(curve_id)
        
        # Save
        await repository.save(curve)
        await async_session.commit()
        
        # Act & Assert - After save
        assert await repository.exists(curve_id)

