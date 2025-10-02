"""Mapper for Curve aggregate."""

from decimal import Decimal
from typing import List

from ....domain.energy.models.curve import (
    Curve,
    CurveId,
    CurveMetadata,
    CurvePoint,
    TenorType,
)
from ....domain.shared_kernel.value_objects import TenantId
from ..models.curve_model import CurveORM, CurvePointORM


class CurveMapper:
    """Maps between Curve domain model and CurveORM."""
    
    @staticmethod
    def to_orm(curve: Curve) -> CurveORM:
        """Convert domain Curve to ORM model.
        
        Args:
            curve: Domain curve aggregate
            
        Returns:
            CurveORM instance
        """
        orm = CurveORM(
            id=curve.id.value,
            tenant_id=curve.tenant_id.value,
            curve_key=curve.metadata.curve_key,
            as_of_date=curve.metadata.as_of_date,
            currency=curve.metadata.currency,
            tenor_type=curve.metadata.tenor_type.value if curve.metadata.tenor_type else None,
            price_type=curve.metadata.price_type,
            day_count=curve.metadata.day_count,
            calendar=curve.metadata.calendar,
            asset_class=curve.metadata.asset_class,
            source=curve.metadata.source,
            measure=curve.measure,
            created_at=curve.created_at,
            updated_at=curve.updated_at,
            version=curve.version,
        )
        
        # Add points
        orm.points = [
            CurvePointORM(
                tenor=point.tenor,
                value=point.value,
                timestamp=point.timestamp,
                quality_flag=point.quality_flag,
            )
            for point in curve.points
        ]
        
        return orm
    
    @staticmethod
    def to_domain(orm: CurveORM) -> Curve:
        """Convert ORM model to domain Curve.
        
        Args:
            orm: CurveORM instance
            
        Returns:
            Domain curve aggregate
        """
        metadata = CurveMetadata(
            curve_key=orm.curve_key,
            as_of_date=orm.as_of_date,
            currency=orm.currency,
            tenor_type=TenorType(orm.tenor_type) if orm.tenor_type else None,
            price_type=orm.price_type,
            day_count=orm.day_count,
            calendar=orm.calendar,
            asset_class=orm.asset_class,
            source=orm.source,
        )
        
        points = [
            CurvePoint(
                tenor=Decimal(str(point.tenor)),
                value=Decimal(str(point.value)),
                timestamp=point.timestamp,
                quality_flag=point.quality_flag,
            )
            for point in orm.points
        ]
        
        curve = Curve.__new__(Curve)  # Create without calling __init__
        curve.id = CurveId(value=orm.id)
        curve.tenant_id = TenantId(value=orm.tenant_id)
        curve.metadata = metadata
        curve.points = points
        curve.measure = orm.measure
        curve.created_at = orm.created_at
        curve.updated_at = orm.updated_at
        curve.version = orm.version
        curve._domain_events = []  # Initialize empty events list
        
        return curve
    
    @staticmethod
    def update_orm(orm: CurveORM, curve: Curve) -> None:
        """Update existing ORM with domain model changes.
        
        Args:
            orm: Existing CurveORM instance
            curve: Domain curve with updates
        """
        orm.curve_key = curve.metadata.curve_key
        orm.as_of_date = curve.metadata.as_of_date
        orm.currency = curve.metadata.currency
        orm.tenor_type = curve.metadata.tenor_type.value if curve.metadata.tenor_type else None
        orm.price_type = curve.metadata.price_type
        orm.day_count = curve.metadata.day_count
        orm.calendar = curve.metadata.calendar
        orm.asset_class = curve.metadata.asset_class
        orm.source = curve.metadata.source
        orm.measure = curve.measure
        orm.updated_at = curve.updated_at
        orm.version = curve.version
        
        # Update points - simple approach: clear and recreate
        orm.points.clear()
        orm.points = [
            CurvePointORM(
                tenor=point.tenor,
                value=point.value,
                timestamp=point.timestamp,
                quality_flag=point.quality_flag,
            )
            for point in curve.points
        ]

