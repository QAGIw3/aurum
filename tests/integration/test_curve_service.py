"""Integration tests for Curve application service."""

from datetime import datetime
from decimal import Decimal

import pytest

from aurum.application.energy.curve_service import (
    CurveApplicationService,
    CreateCurveCommand,
    AddCurvePointCommand,
    UpdateCurvePointCommand,
)
from aurum.infrastructure.persistence.curve_repository import CurveRepository
from aurum.infrastructure.persistence.unit_of_work import SqlAlchemyUnitOfWork


@pytest.mark.asyncio
class TestCurveApplicationService:
    """Integration tests for CurveApplicationService."""
    
    async def test_create_curve_success(self, async_session, event_bus):
        """Test creating a curve successfully."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        uow = SqlAlchemyUnitOfWork(lambda: async_session)
        service = CurveApplicationService(repository, uow)
        
        command = CreateCurveCommand(
            tenant_id="550e8400-e29b-41d4-a716-446655440000",
            curve_key="PJM_DA_Q1_2025",
            as_of_date=datetime(2025, 1, 1),
            points=[(Decimal('1'), Decimal('100')), (Decimal('2'), Decimal('105'))],
            currency="USD",
            tenor_type="monthly",
            price_type="forward",
        )
        
        # Act
        result = await service.create_curve(command)
        
        # Assert
        assert result.is_success()
        curve_dto = result.value
        assert curve_dto.curve_key == "PJM_DA_Q1_2025"
        assert len(curve_dto.points) == 2
        assert curve_dto.metadata["currency"] == "USD"
        assert curve_dto.metadata["tenor_type"] == "monthly"
    
    async def test_create_curve_validation_error(self, async_session, event_bus):
        """Test curve creation with invalid data."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        uow = SqlAlchemyUnitOfWork(lambda: async_session)
        service = CurveApplicationService(repository, uow)
        
        command = CreateCurveCommand(
            tenant_id="550e8400-e29b-41d4-a716-446655440000",
            curve_key="",  # Invalid: empty key
            as_of_date=datetime(2025, 1, 1),
            points=[(Decimal('1'), Decimal('100'))],
        )
        
        # Act
        result = await service.create_curve(command)
        
        # Assert
        assert result.is_failure()
        assert result.error == "DOMAIN_ERROR"
        assert "empty" in result.message.lower()
    
    async def test_add_curve_point(self, async_session, event_bus):
        """Test adding a point to an existing curve."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        uow = SqlAlchemyUnitOfWork(lambda: async_session)
        service = CurveApplicationService(repository, uow)
        
        # Create curve first
        create_command = CreateCurveCommand(
            tenant_id="550e8400-e29b-41d4-a716-446655440000",
            curve_key="TEST_CURVE",
            as_of_date=datetime.utcnow(),
            points=[(Decimal('1'), Decimal('100'))],
        )
        create_result = await service.create_curve(create_command)
        assert create_result.is_success()
        curve_id = create_result.value.id
        
        # Act - Add point
        add_command = AddCurvePointCommand(
            curve_id=curve_id,
            tenor=Decimal('2'),
            value=Decimal('105'),
        )
        result = await service.add_curve_point(add_command)
        
        # Assert
        assert result.is_success()
        curve_dto = result.value
        assert len(curve_dto.points) == 2
        assert any(p['tenor'] == '2' for p in curve_dto.points)
    
    async def test_update_curve_point(self, async_session, event_bus):
        """Test updating a curve point."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        uow = SqlAlchemyUnitOfWork(lambda: async_session)
        service = CurveApplicationService(repository, uow)
        
        # Create curve
        create_command = CreateCurveCommand(
            tenant_id="550e8400-e29b-41d4-a716-446655440000",
            curve_key="TEST_CURVE",
            as_of_date=datetime.utcnow(),
            points=[(Decimal('1'), Decimal('100'))],
        )
        create_result = await service.create_curve(create_command)
        curve_id = create_result.value.id
        
        # Act - Update point
        update_command = UpdateCurvePointCommand(
            curve_id=curve_id,
            tenor=Decimal('1'),
            new_value=Decimal('110'),
        )
        result = await service.update_curve_point(update_command)
        
        # Assert
        assert result.is_success()
        curve_dto = result.value
        point = next(p for p in curve_dto.points if p['tenor'] == '1')
        assert point['value'] == '110'
    
    async def test_get_curve_not_found(self, async_session, event_bus):
        """Test getting a non-existent curve."""
        # Arrange
        repository = CurveRepository(async_session, event_bus)
        uow = SqlAlchemyUnitOfWork(lambda: async_session)
        service = CurveApplicationService(repository, uow)
        
        # Act
        result = await service.get_curve("00000000-0000-0000-0000-000000000000")
        
        # Assert
        assert result.is_failure()
        assert result.error == "NOT_FOUND"

