"""Application service for curve operations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from ..common.results import Result, success, failure
from ..common.unit_of_work import UnitOfWork
from ...domain.energy.models.curve import (
    Curve,
    CurveId,
    CurveMetadata,
    CurvePoint,
    TenorType,
)
from ...domain.shared_kernel.repositories import Repository
from ...domain.shared_kernel.value_objects import TenantId
from ...domain.shared_kernel.exceptions import DomainException


@dataclass(frozen=True)
class CreateCurveCommand:
    """Command to create a new curve."""
    
    tenant_id: str
    curve_key: str
    as_of_date: datetime
    points: List[tuple[Decimal, Decimal]]  # [(tenor, value), ...]
    currency: str = "USD"
    tenor_type: Optional[str] = None
    price_type: Optional[str] = None
    measure: str = "value"


@dataclass(frozen=True)
class AddCurvePointCommand:
    """Command to add a point to a curve."""
    
    curve_id: str
    tenor: Decimal
    value: Decimal


@dataclass(frozen=True)
class UpdateCurvePointCommand:
    """Command to update a curve point."""
    
    curve_id: str
    tenor: Decimal
    new_value: Decimal


@dataclass(frozen=True)
class CurveDTO:
    """Data transfer object for curve."""
    
    id: str
    tenant_id: str
    curve_key: str
    as_of_date: datetime
    points: List[dict]
    measure: str
    metadata: dict


class CurveApplicationService:
    """Application service for curve use cases.
    
    Orchestrates curve operations, coordinating between the domain model
    and infrastructure concerns like persistence and events.
    """
    
    def __init__(
        self,
        curve_repository: Repository[Curve],
        unit_of_work: UnitOfWork,
    ):
        """Initialize the service.
        
        Args:
            curve_repository: Repository for curve aggregates
            unit_of_work: Unit of work for transaction management
        """
        self.curve_repository = curve_repository
        self.unit_of_work = unit_of_work
    
    async def create_curve(self, command: CreateCurveCommand) -> Result[CurveDTO]:
        """Create a new curve.
        
        Args:
            command: The create curve command
            
        Returns:
            Result containing the created curve DTO or error
        """
        try:
            # Create domain entities
            curve_id = CurveId.generate()
            tenant_id = TenantId.from_string(command.tenant_id)
            
            tenor_type = TenorType(command.tenor_type) if command.tenor_type else None
            
            metadata = CurveMetadata(
                curve_key=command.curve_key,
                as_of_date=command.as_of_date,
                currency=command.currency,
                tenor_type=tenor_type,
                price_type=command.price_type,
            )
            
            points = [
                CurvePoint(tenor=tenor, value=value)
                for tenor, value in command.points
            ]
            
            curve = Curve(
                id=curve_id,
                tenant_id=tenant_id,
                metadata=metadata,
                points=points,
                measure=command.measure,
            )
            
            # Persist
            async with self.unit_of_work:
                await self.curve_repository.save(curve)
                await self.unit_of_work.commit()
            
            # Return DTO
            return success(self._to_dto(curve))
            
        except DomainException as e:
            return failure("DOMAIN_ERROR", str(e), e.details)
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to create curve: {str(e)}")
    
    async def add_curve_point(self, command: AddCurvePointCommand) -> Result[CurveDTO]:
        """Add a point to an existing curve.
        
        Args:
            command: The add point command
            
        Returns:
            Result containing the updated curve DTO or error
        """
        try:
            curve_id = CurveId.from_string(command.curve_id)
            
            async with self.unit_of_work:
                # Load aggregate
                curve = await self.curve_repository.get_by_id(curve_id)
                if curve is None:
                    return failure("NOT_FOUND", f"Curve {command.curve_id} not found")
                
                # Execute domain logic
                point = CurvePoint(tenor=command.tenor, value=command.value)
                curve.add_point(point)
                
                # Persist
                await self.curve_repository.save(curve)
                await self.unit_of_work.commit()
            
            return success(self._to_dto(curve))
            
        except DomainException as e:
            return failure("DOMAIN_ERROR", str(e), e.details)
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to add curve point: {str(e)}")
    
    async def update_curve_point(self, command: UpdateCurvePointCommand) -> Result[CurveDTO]:
        """Update a curve point.
        
        Args:
            command: The update point command
            
        Returns:
            Result containing the updated curve DTO or error
        """
        try:
            curve_id = CurveId.from_string(command.curve_id)
            
            async with self.unit_of_work:
                curve = await self.curve_repository.get_by_id(curve_id)
                if curve is None:
                    return failure("NOT_FOUND", f"Curve {command.curve_id} not found")
                
                curve.update_point(command.tenor, command.new_value)
                
                await self.curve_repository.save(curve)
                await self.unit_of_work.commit()
            
            return success(self._to_dto(curve))
            
        except DomainException as e:
            return failure("DOMAIN_ERROR", str(e), e.details)
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to update curve point: {str(e)}")
    
    async def get_curve(self, curve_id: str) -> Result[CurveDTO]:
        """Get a curve by ID.
        
        Args:
            curve_id: The curve identifier
            
        Returns:
            Result containing the curve DTO or error
        """
        try:
            curve = await self.curve_repository.get_by_id(CurveId.from_string(curve_id))
            if curve is None:
                return failure("NOT_FOUND", f"Curve {curve_id} not found")
            
            return success(self._to_dto(curve))
            
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to get curve: {str(e)}")
    
    def _to_dto(self, curve: Curve) -> CurveDTO:
        """Convert curve aggregate to DTO.
        
        Args:
            curve: The curve aggregate
            
        Returns:
            Curve DTO
        """
        return CurveDTO(
            id=str(curve.id),
            tenant_id=str(curve.tenant_id),
            curve_key=curve.metadata.curve_key,
            as_of_date=curve.metadata.as_of_date,
            points=[
                {
                    "tenor": str(p.tenor),
                    "value": str(p.value),
                    "timestamp": p.timestamp.isoformat() if p.timestamp else None,
                    "quality_flag": p.quality_flag,
                }
                for p in curve.points
            ],
            measure=curve.measure,
            metadata={
                "currency": curve.metadata.currency,
                "tenor_type": curve.metadata.tenor_type.value if curve.metadata.tenor_type else None,
                "price_type": curve.metadata.price_type,
                "day_count": curve.metadata.day_count,
                "calendar": curve.metadata.calendar,
                "asset_class": curve.metadata.asset_class,
            },
        )

