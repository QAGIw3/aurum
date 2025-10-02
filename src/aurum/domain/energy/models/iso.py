"""ISO (Independent System Operator) domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional

from ...shared_kernel.entities import AggregateRoot, TenantEntity, DomainEvent
from ...shared_kernel.value_objects import EntityId, TenantId, Location
from ...shared_kernel.exceptions import ValidationError, BusinessRuleViolation


class IsoDataType(str, Enum):
    """Types of ISO market data."""
    LMP = "lmp"  # Locational Marginal Pricing
    LOAD = "load"  # System load/demand
    GENERATION_MIX = "genmix"  # Generation mix by fuel type
    ANCILLARY_SERVICES = "asm"  # Ancillary services markets
    PRICE_NODES = "pnode"  # Price node definitions


class MarketType(str, Enum):
    """Types of energy markets."""
    DAY_AHEAD = "DAM"  # Day-Ahead Market
    REAL_TIME = "RTM"  # Real-Time Market
    HOUR_AHEAD = "HAM"  # Hour-Ahead Market


@dataclass(frozen=True)
class IsoMarketId(EntityId):
    """Strongly-typed ISO market identifier."""
    pass


@dataclass(frozen=True)
class LocationalMarginalPrice:
    """Value object for LMP data."""
    
    node_id: str
    location: Optional[Location]
    energy_price: Decimal
    congestion_price: Decimal
    loss_price: Decimal
    timestamp: datetime
    market_type: MarketType
    
    def __post_init__(self):
        if not self.node_id:
            raise ValidationError("Node ID cannot be empty")
        
        # Convert to Decimal if needed
        for field_name in ['energy_price', 'congestion_price', 'loss_price']:
            value = getattr(self, field_name)
            if not isinstance(value, Decimal):
                object.__setattr__(self, field_name, Decimal(str(value)))
    
    @property
    def total_price(self) -> Decimal:
        """Calculate total LMP (energy + congestion + losses)."""
        return self.energy_price + self.congestion_price + self.loss_price


@dataclass(frozen=True)
class SystemLoad:
    """Value object for system load data."""
    
    zone_id: str
    load_mw: Decimal
    timestamp: datetime
    forecast: bool = False  # True if this is forecasted load
    
    def __post_init__(self):
        if not self.zone_id:
            raise ValidationError("Zone ID cannot be empty")
        
        if not isinstance(self.load_mw, Decimal):
            object.__setattr__(self, 'load_mw', Decimal(str(self.load_mw)))
        
        if self.load_mw < Decimal('0'):
            raise ValidationError(f"Load cannot be negative: {self.load_mw}")


@dataclass(frozen=True)
class GenerationMix:
    """Value object for generation mix data."""
    
    zone_id: str
    fuel_type: str  # e.g., "coal", "gas", "nuclear", "wind", "solar"
    generation_mw: Decimal
    percentage: Decimal  # Percentage of total generation
    timestamp: datetime
    
    def __post_init__(self):
        if not self.zone_id:
            raise ValidationError("Zone ID cannot be empty")
        
        if not self.fuel_type:
            raise ValidationError("Fuel type cannot be empty")
        
        for field_name in ['generation_mw', 'percentage']:
            value = getattr(self, field_name)
            if not isinstance(value, Decimal):
                object.__setattr__(self, field_name, Decimal(str(value)))
        
        if self.generation_mw < Decimal('0'):
            raise ValidationError(f"Generation cannot be negative: {self.generation_mw}")
        
        if not Decimal('0') <= self.percentage <= Decimal('100'):
            raise ValidationError(f"Percentage must be between 0 and 100: {self.percentage}")


@dataclass
class IsoMarket(AggregateRoot, TenantEntity):
    """Aggregate root for ISO market operations.
    
    Represents a specific ISO market region with its operational data.
    Enforces business rules around market data integrity and timing.
    """
    
    id: IsoMarketId
    tenant_id: TenantId
    iso_code: str  # e.g., "PJM", "CAISO", "MISO"
    iso_name: str
    timezone: str  # e.g., "America/New_York"
    active: bool = True
    
    # Market data
    _lmp_data: List[LocationalMarginalPrice] = field(default_factory=list, init=False, repr=False)
    _load_data: List[SystemLoad] = field(default_factory=list, init=False, repr=False)
    _generation_data: List[GenerationMix] = field(default_factory=list, init=False, repr=False)
    
    def __post_init__(self):
        """Validate market invariants."""
        self._validate_market()
    
    def _validate_market(self) -> None:
        """Validate market business rules."""
        if not self.iso_code:
            raise ValidationError("ISO code cannot be empty")
        
        if len(self.iso_code) > 10:
            raise ValidationError(f"ISO code too long: {self.iso_code}")
        
        if not self.iso_name:
            raise ValidationError("ISO name cannot be empty")
    
    def add_lmp_data(self, lmp: LocationalMarginalPrice) -> None:
        """Add LMP data to the market.
        
        Args:
            lmp: Locational Marginal Price data
        """
        # Business rule: Don't add duplicate data
        if any(
            existing.node_id == lmp.node_id and
            existing.timestamp == lmp.timestamp and
            existing.market_type == lmp.market_type
            for existing in self._lmp_data
        ):
            raise BusinessRuleViolation(
                f"LMP data already exists for node {lmp.node_id} at {lmp.timestamp}",
                {"node_id": lmp.node_id, "timestamp": str(lmp.timestamp)}
            )
        
        self._lmp_data.append(lmp)
        self.updated_at = datetime.utcnow()
        self.record_event(LMPDataAddedEvent(
            aggregate_id=self.id,
            iso_code=self.iso_code,
            node_id=lmp.node_id,
            timestamp=lmp.timestamp,
            total_price=lmp.total_price
        ))
    
    def add_load_data(self, load: SystemLoad) -> None:
        """Add system load data.
        
        Args:
            load: System load data
        """
        # Business rule: Don't add duplicate data
        if any(
            existing.zone_id == load.zone_id and
            existing.timestamp == load.timestamp
            for existing in self._load_data
        ):
            raise BusinessRuleViolation(
                f"Load data already exists for zone {load.zone_id} at {load.timestamp}",
                {"zone_id": load.zone_id, "timestamp": str(load.timestamp)}
            )
        
        self._load_data.append(load)
        self.updated_at = datetime.utcnow()
        self.record_event(LoadDataAddedEvent(
            aggregate_id=self.id,
            iso_code=self.iso_code,
            zone_id=load.zone_id,
            timestamp=load.timestamp,
            load_mw=load.load_mw
        ))
    
    def add_generation_mix(self, gen_mix: GenerationMix) -> None:
        """Add generation mix data.
        
        Args:
            gen_mix: Generation mix data
        """
        self._generation_data.append(gen_mix)
        self.updated_at = datetime.utcnow()
        self.record_event(GenerationMixAddedEvent(
            aggregate_id=self.id,
            iso_code=self.iso_code,
            zone_id=gen_mix.zone_id,
            fuel_type=gen_mix.fuel_type,
            timestamp=gen_mix.timestamp,
            generation_mw=gen_mix.generation_mw
        ))
    
    def get_lmp_for_node(self, node_id: str, start_time: datetime, end_time: datetime) -> List[LocationalMarginalPrice]:
        """Get LMP data for a specific node within a time range.
        
        Args:
            node_id: The node identifier
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            List of LMP data points
        """
        return [
            lmp for lmp in self._lmp_data
            if lmp.node_id == node_id and start_time <= lmp.timestamp <= end_time
        ]
    
    def deactivate(self) -> None:
        """Deactivate this ISO market."""
        if not self.active:
            raise BusinessRuleViolation("Market is already inactive")
        
        self.active = False
        self.updated_at = datetime.utcnow()
        self.record_event(IsoMarketDeactivatedEvent(
            aggregate_id=self.id,
            iso_code=self.iso_code
        ))


# Domain Events

@dataclass
class IsoMarketCreatedEvent(DomainEvent):
    """Event raised when an ISO market is created."""
    
    iso_code: str
    iso_name: str
    tenant_id: TenantId


@dataclass
class LMPDataAddedEvent(DomainEvent):
    """Event raised when LMP data is added."""
    
    iso_code: str
    node_id: str
    timestamp: datetime
    total_price: Decimal


@dataclass
class LoadDataAddedEvent(DomainEvent):
    """Event raised when load data is added."""
    
    iso_code: str
    zone_id: str
    timestamp: datetime
    load_mw: Decimal


@dataclass
class GenerationMixAddedEvent(DomainEvent):
    """Event raised when generation mix data is added."""
    
    iso_code: str
    zone_id: str
    fuel_type: str
    timestamp: datetime
    generation_mw: Decimal


@dataclass
class IsoMarketDeactivatedEvent(DomainEvent):
    """Event raised when an ISO market is deactivated."""
    
    iso_code: str

