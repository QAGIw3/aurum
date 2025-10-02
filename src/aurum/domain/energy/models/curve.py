"""Curve domain model - representing energy price curves."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional

from ...shared_kernel.entities import AggregateRoot, TenantEntity, DomainEvent
from ...shared_kernel.value_objects import EntityId, TenantId, MarketPrice
from ...shared_kernel.exceptions import ValidationError, BusinessRuleViolation


class TenorType(str, Enum):
    """Types of curve tenors."""
    DAILY = "daily"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    CUSTOM = "custom"


@dataclass(frozen=True)
class CurveId(EntityId):
    """Strongly-typed curve identifier."""
    pass


@dataclass(frozen=True)
class CurveMetadata:
    """Metadata describing a curve."""
    
    curve_key: str
    as_of_date: datetime
    currency: str = "USD"
    tenor_type: Optional[TenorType] = None
    price_type: Optional[str] = None  # e.g., "forward", "spot"
    day_count: Optional[str] = None  # e.g., "ACT/360", "30/360"
    calendar: Optional[str] = None  # e.g., "NERC", "NYSE"
    asset_class: Optional[str] = None  # e.g., "power", "gas", "coal"
    source: Optional[str] = None
    
    def __post_init__(self):
        if not self.curve_key:
            raise ValidationError("Curve key cannot be empty")
        if len(self.currency) != 3:
            raise ValidationError(f"Currency must be 3-letter ISO code, got {self.currency}")


@dataclass(frozen=True)
class CurvePoint:
    """A single point on a curve."""
    
    tenor: Decimal  # Time period (e.g., days, months)
    value: Decimal  # Price or value at this tenor
    timestamp: Optional[datetime] = None
    quality_flag: Optional[str] = None  # e.g., "observed", "interpolated", "extrapolated"
    
    def __post_init__(self):
        if not isinstance(self.value, Decimal):
            object.__setattr__(self, 'value', Decimal(str(self.value)))
        if not isinstance(self.tenor, Decimal):
            object.__setattr__(self, 'tenor', Decimal(str(self.tenor)))


@dataclass
class Curve(AggregateRoot, TenantEntity):
    """Aggregate root for energy price curves.
    
    Represents a complete energy price curve with business logic for:
    - Curve validation and integrity
    - Point interpolation
    - Shape analysis
    - Comparison with other curves
    """
    
    id: CurveId
    tenant_id: TenantId
    metadata: CurveMetadata
    points: List[CurvePoint] = field(default_factory=list)
    measure: str = "value"  # What this curve measures
    
    def __post_init__(self):
        """Validate curve invariants."""
        self._validate_curve()
    
    def _validate_curve(self) -> None:
        """Validate curve business rules."""
        if not self.points:
            raise ValidationError("Curve must have at least one point")
        
        # Check for duplicate tenors
        tenors = [p.tenor for p in self.points]
        if len(tenors) != len(set(tenors)):
            raise ValidationError("Curve cannot have duplicate tenor points")
        
        # Ensure points are sorted
        if tenors != sorted(tenors):
            # Auto-sort points
            object.__setattr__(self, 'points', sorted(self.points, key=lambda p: p.tenor))
    
    def add_point(self, point: CurvePoint) -> None:
        """Add a new point to the curve.
        
        Args:
            point: The curve point to add
            
        Raises:
            BusinessRuleViolation: If point tenor already exists
        """
        if any(p.tenor == point.tenor for p in self.points):
            raise BusinessRuleViolation(
                f"Point with tenor {point.tenor} already exists",
                {"tenor": str(point.tenor)}
            )
        
        self.points.append(point)
        self.points.sort(key=lambda p: p.tenor)
        self.updated_at = datetime.utcnow()
        self.record_event(CurvePointAddedEvent(
            aggregate_id=self.id,
            curve_key=self.metadata.curve_key,
            tenor=point.tenor,
            value=point.value
        ))
    
    def update_point(self, tenor: Decimal, new_value: Decimal) -> None:
        """Update the value of an existing point.
        
        Args:
            tenor: The tenor of the point to update
            new_value: The new value
            
        Raises:
            ValidationError: If point doesn't exist
        """
        for i, point in enumerate(self.points):
            if point.tenor == tenor:
                old_value = point.value
                self.points[i] = CurvePoint(
                    tenor=tenor,
                    value=new_value,
                    timestamp=datetime.utcnow(),
                    quality_flag=point.quality_flag
                )
                self.updated_at = datetime.utcnow()
                self.record_event(CurvePointUpdatedEvent(
                    aggregate_id=self.id,
                    curve_key=self.metadata.curve_key,
                    tenor=tenor,
                    old_value=old_value,
                    new_value=new_value
                ))
                return
        
        raise ValidationError(f"No point with tenor {tenor} found")
    
    def remove_point(self, tenor: Decimal) -> None:
        """Remove a point from the curve.
        
        Args:
            tenor: The tenor of the point to remove
            
        Raises:
            ValidationError: If point doesn't exist
            BusinessRuleViolation: If removing would leave curve empty
        """
        if len(self.points) <= 1:
            raise BusinessRuleViolation("Cannot remove last point from curve")
        
        original_length = len(self.points)
        self.points = [p for p in self.points if p.tenor != tenor]
        
        if len(self.points) == original_length:
            raise ValidationError(f"No point with tenor {tenor} found")
        
        self.updated_at = datetime.utcnow()
        self.record_event(CurvePointRemovedEvent(
            aggregate_id=self.id,
            curve_key=self.metadata.curve_key,
            tenor=tenor
        ))
    
    def get_value_at_tenor(self, tenor: Decimal) -> Optional[Decimal]:
        """Get the value at a specific tenor (exact match only).
        
        Args:
            tenor: The tenor to look up
            
        Returns:
            The value if found, None otherwise
        """
        for point in self.points:
            if point.tenor == tenor:
                return point.value
        return None
    
    @property
    def min_value(self) -> Decimal:
        """Get the minimum value on the curve."""
        return min(p.value for p in self.points)
    
    @property
    def max_value(self) -> Decimal:
        """Get the maximum value on the curve."""
        return max(p.value for p in self.points)
    
    @property
    def average_value(self) -> Decimal:
        """Get the average value across all points."""
        return sum(p.value for p in self.points) / len(self.points)


# Domain Events

@dataclass
class CurveCreatedEvent(DomainEvent):
    """Event raised when a new curve is created."""
    
    curve_key: str
    tenant_id: TenantId
    as_of_date: datetime


@dataclass
class CurvePointAddedEvent(DomainEvent):
    """Event raised when a point is added to a curve."""
    
    curve_key: str
    tenor: Decimal
    value: Decimal


@dataclass
class CurvePointUpdatedEvent(DomainEvent):
    """Event raised when a curve point is updated."""
    
    curve_key: str
    tenor: Decimal
    old_value: Decimal
    new_value: Decimal


@dataclass
class CurvePointRemovedEvent(DomainEvent):
    """Event raised when a point is removed from a curve."""
    
    curve_key: str
    tenor: Decimal

