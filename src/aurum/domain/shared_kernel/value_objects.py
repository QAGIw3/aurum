"""Shared value objects used across the domain."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4


@dataclass(frozen=True)
class EntityId:
    """Base class for strongly-typed entity identifiers."""
    
    value: UUID
    
    def __post_init__(self):
        if not isinstance(self.value, UUID):
            raise ValueError(f"EntityId must be a UUID, got {type(self.value)}")
    
    @classmethod
    def generate(cls) -> EntityId:
        """Generate a new unique identifier."""
        return cls(value=uuid4())
    
    @classmethod
    def from_string(cls, value: str) -> EntityId:
        """Create from string representation."""
        return cls(value=UUID(value))
    
    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True)
class TenantId(EntityId):
    """Strongly-typed tenant identifier."""
    pass


@dataclass(frozen=True)
class Money:
    """Value object representing monetary amounts."""
    
    amount: Decimal
    currency: str = "USD"
    
    def __post_init__(self):
        if not isinstance(self.amount, Decimal):
            object.__setattr__(self, 'amount', Decimal(str(self.amount)))
        
        if len(self.currency) != 3:
            raise ValueError(f"Currency must be 3-letter code, got {self.currency}")
    
    def __add__(self, other: Money) -> Money:
        if self.currency != other.currency:
            raise ValueError(f"Cannot add {self.currency} and {other.currency}")
        return Money(self.amount + other.amount, self.currency)
    
    def __sub__(self, other: Money) -> Money:
        if self.currency != other.currency:
            raise ValueError(f"Cannot subtract {self.currency} and {other.currency}")
        return Money(self.amount - other.amount, self.currency)
    
    def __mul__(self, scalar: float | int) -> Money:
        return Money(self.amount * Decimal(str(scalar)), self.currency)
    
    def __truediv__(self, scalar: float | int) -> Money:
        if scalar == 0:
            raise ValueError("Cannot divide by zero")
        return Money(self.amount / Decimal(str(scalar)), self.currency)


@dataclass(frozen=True)
class TimeRange:
    """Value object representing a time range."""
    
    start: datetime
    end: datetime
    
    def __post_init__(self):
        if self.start >= self.end:
            raise ValueError(f"Start time {self.start} must be before end time {self.end}")
    
    def contains(self, timestamp: datetime) -> bool:
        """Check if timestamp falls within this range."""
        return self.start <= timestamp <= self.end
    
    def overlaps(self, other: TimeRange) -> bool:
        """Check if this range overlaps with another."""
        return self.start < other.end and other.start < self.end
    
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        return (self.end - self.start).total_seconds()


@dataclass(frozen=True)
class MarketPrice:
    """Value object for market prices with units."""
    
    value: Decimal
    unit: str  # e.g., "USD/MWh", "USD/MMBtu"
    timestamp: datetime
    
    def __post_init__(self):
        if not isinstance(self.value, Decimal):
            object.__setattr__(self, 'value', Decimal(str(self.value)))


@dataclass(frozen=True)
class Location:
    """Value object representing a geographic location."""
    
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    zone: Optional[str] = None
    node: Optional[str] = None
    
    def __post_init__(self):
        if self.latitude is not None and not -90 <= self.latitude <= 90:
            raise ValueError(f"Latitude must be between -90 and 90, got {self.latitude}")
        if self.longitude is not None and not -180 <= self.longitude <= 180:
            raise ValueError(f"Longitude must be between -180 and 180, got {self.longitude}")

