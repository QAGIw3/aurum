"""PPA (Power Purchase Agreement) domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional

from ...shared_kernel.entities import AggregateRoot, TenantEntity, DomainEvent
from ...shared_kernel.value_objects import EntityId, TenantId, Money, TimeRange
from ...shared_kernel.exceptions import ValidationError, BusinessRuleViolation


class PPAStatus(str, Enum):
    """Status of a Power Purchase Agreement."""
    DRAFT = "draft"
    NEGOTIATION = "negotiation"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    TERMINATED = "terminated"


class PricingType(str, Enum):
    """Types of PPA pricing structures."""
    FIXED = "fixed"  # Fixed price per MWh
    INDEXED = "indexed"  # Indexed to market rates
    COLLAR = "collar"  # Price floor and ceiling
    HYBRID = "hybrid"  # Combination of pricing types


@dataclass(frozen=True)
class PPAId(EntityId):
    """Strongly-typed PPA identifier."""
    pass


@dataclass(frozen=True)
class PPATerms:
    """Value object representing PPA contract terms."""
    
    pricing_type: PricingType
    fixed_price_per_mwh: Optional[Money] = None
    floor_price: Optional[Money] = None
    ceiling_price: Optional[Money] = None
    index_reference: Optional[str] = None  # e.g., "PJM_DA_LMP"
    index_multiplier: Decimal = Decimal('1.0')
    
    delivery_start: datetime = None
    delivery_end: datetime = None
    minimum_annual_mwh: Optional[Decimal] = None
    maximum_annual_mwh: Optional[Decimal] = None
    
    def __post_init__(self):
        """Validate PPA terms."""
        if self.pricing_type == PricingType.FIXED and self.fixed_price_per_mwh is None:
            raise ValidationError("Fixed pricing requires fixed_price_per_mwh")
        
        if self.pricing_type == PricingType.INDEXED and self.index_reference is None:
            raise ValidationError("Indexed pricing requires index_reference")
        
        if self.pricing_type == PricingType.COLLAR:
            if self.floor_price is None or self.ceiling_price is None:
                raise ValidationError("Collar pricing requires both floor and ceiling prices")
            if self.floor_price.amount >= self.ceiling_price.amount:
                raise ValidationError("Floor price must be less than ceiling price")
        
        if self.delivery_start and self.delivery_end:
            if self.delivery_start >= self.delivery_end:
                raise ValidationError("Delivery start must be before delivery end")
        
        if not isinstance(self.index_multiplier, Decimal):
            object.__setattr__(self, 'index_multiplier', Decimal(str(self.index_multiplier)))


@dataclass(frozen=True)
class DeliverySchedule:
    """Value object for energy delivery schedule."""
    
    schedule_id: str
    delivery_date: datetime
    scheduled_mwh: Decimal
    actual_mwh: Optional[Decimal] = None
    delivery_location: Optional[str] = None
    
    def __post_init__(self):
        if not isinstance(self.scheduled_mwh, Decimal):
            object.__setattr__(self, 'scheduled_mwh', Decimal(str(self.scheduled_mwh)))
        
        if self.actual_mwh is not None and not isinstance(self.actual_mwh, Decimal):
            object.__setattr__(self, 'actual_mwh', Decimal(str(self.actual_mwh)))
        
        if self.scheduled_mwh <= Decimal('0'):
            raise ValidationError(f"Scheduled MWh must be positive: {self.scheduled_mwh}")
    
    @property
    def is_delivered(self) -> bool:
        """Check if delivery has been completed."""
        return self.actual_mwh is not None
    
    @property
    def delivery_variance(self) -> Optional[Decimal]:
        """Calculate variance between scheduled and actual delivery."""
        if self.actual_mwh is None:
            return None
        return self.actual_mwh - self.scheduled_mwh


@dataclass
class PowerPurchaseAgreement(AggregateRoot, TenantEntity):
    """Aggregate root for Power Purchase Agreements.
    
    Represents a complete PPA contract with business logic for:
    - Contract validation and lifecycle management
    - Delivery scheduling and tracking
    - Payment calculations
    - Contract amendments and termination
    """
    
    id: PPAId
    tenant_id: TenantId
    contract_number: str
    buyer_name: str
    seller_name: str
    status: PPAStatus
    terms: PPATerms
    
    # Delivery tracking
    _delivery_schedules: List[DeliverySchedule] = field(default_factory=list, init=False, repr=False)
    
    # Contract metadata
    signed_date: Optional[datetime] = None
    effective_date: Optional[datetime] = None
    expiration_date: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate PPA invariants."""
        self._validate_ppa()
    
    def _validate_ppa(self) -> None:
        """Validate PPA business rules."""
        if not self.contract_number:
            raise ValidationError("Contract number cannot be empty")
        
        if not self.buyer_name:
            raise ValidationError("Buyer name cannot be empty")
        
        if not self.seller_name:
            raise ValidationError("Seller name cannot be empty")
        
        if self.buyer_name == self.seller_name:
            raise ValidationError("Buyer and seller cannot be the same party")
    
    def activate(self) -> None:
        """Activate the PPA contract.
        
        Raises:
            BusinessRuleViolation: If contract cannot be activated
        """
        if self.status != PPAStatus.DRAFT and self.status != PPAStatus.NEGOTIATION:
            raise BusinessRuleViolation(
                f"Cannot activate PPA from status {self.status}",
                {"current_status": self.status.value}
            )
        
        if self.signed_date is None:
            raise BusinessRuleViolation("Cannot activate unsigned PPA")
        
        self.status = PPAStatus.ACTIVE
        self.effective_date = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        self.record_event(PPAActivatedEvent(
            aggregate_id=self.id,
            contract_number=self.contract_number,
            buyer_name=self.buyer_name,
            seller_name=self.seller_name,
            effective_date=self.effective_date
        ))
    
    def suspend(self, reason: str) -> None:
        """Suspend the PPA contract.
        
        Args:
            reason: Reason for suspension
            
        Raises:
            BusinessRuleViolation: If contract cannot be suspended
        """
        if self.status != PPAStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Can only suspend active PPAs, current status: {self.status}",
                {"current_status": self.status.value}
            )
        
        self.status = PPAStatus.SUSPENDED
        self.updated_at = datetime.utcnow()
        
        self.record_event(PPASuspendedEvent(
            aggregate_id=self.id,
            contract_number=self.contract_number,
            reason=reason
        ))
    
    def resume(self) -> None:
        """Resume a suspended PPA contract.
        
        Raises:
            BusinessRuleViolation: If contract cannot be resumed
        """
        if self.status != PPAStatus.SUSPENDED:
            raise BusinessRuleViolation(
                f"Can only resume suspended PPAs, current status: {self.status}",
                {"current_status": self.status.value}
            )
        
        self.status = PPAStatus.ACTIVE
        self.updated_at = datetime.utcnow()
        
        self.record_event(PPAResumedEvent(
            aggregate_id=self.id,
            contract_number=self.contract_number
        ))
    
    def terminate(self, reason: str) -> None:
        """Terminate the PPA contract.
        
        Args:
            reason: Reason for termination
        """
        if self.status == PPAStatus.TERMINATED or self.status == PPAStatus.COMPLETED:
            raise BusinessRuleViolation(
                f"PPA already in final status: {self.status}",
                {"current_status": self.status.value}
            )
        
        self.status = PPAStatus.TERMINATED
        self.updated_at = datetime.utcnow()
        
        self.record_event(PPATerminatedEvent(
            aggregate_id=self.id,
            contract_number=self.contract_number,
            reason=reason
        ))
    
    def add_delivery_schedule(self, schedule: DeliverySchedule) -> None:
        """Add a delivery schedule to the PPA.
        
        Args:
            schedule: The delivery schedule to add
            
        Raises:
            BusinessRuleViolation: If schedule conflicts with contract terms
        """
        if self.status != PPAStatus.ACTIVE:
            raise BusinessRuleViolation(
                f"Cannot add delivery schedules to PPA in status {self.status}",
                {"current_status": self.status.value}
            )
        
        # Check if schedule already exists
        if any(s.schedule_id == schedule.schedule_id for s in self._delivery_schedules):
            raise BusinessRuleViolation(
                f"Delivery schedule {schedule.schedule_id} already exists",
                {"schedule_id": schedule.schedule_id}
            )
        
        # Validate against contract terms
        if self.terms.delivery_start and schedule.delivery_date < self.terms.delivery_start:
            raise BusinessRuleViolation(
                "Delivery date is before contract start date",
                {"delivery_date": str(schedule.delivery_date), "contract_start": str(self.terms.delivery_start)}
            )
        
        if self.terms.delivery_end and schedule.delivery_date > self.terms.delivery_end:
            raise BusinessRuleViolation(
                "Delivery date is after contract end date",
                {"delivery_date": str(schedule.delivery_date), "contract_end": str(self.terms.delivery_end)}
            )
        
        self._delivery_schedules.append(schedule)
        self.updated_at = datetime.utcnow()
        
        self.record_event(DeliveryScheduleAddedEvent(
            aggregate_id=self.id,
            contract_number=self.contract_number,
            schedule_id=schedule.schedule_id,
            delivery_date=schedule.delivery_date,
            scheduled_mwh=schedule.scheduled_mwh
        ))
    
    def record_actual_delivery(self, schedule_id: str, actual_mwh: Decimal) -> None:
        """Record actual delivery for a scheduled delivery.
        
        Args:
            schedule_id: The schedule identifier
            actual_mwh: Actual energy delivered
            
        Raises:
            ValidationError: If schedule not found
        """
        for i, schedule in enumerate(self._delivery_schedules):
            if schedule.schedule_id == schedule_id:
                if schedule.is_delivered:
                    raise BusinessRuleViolation(
                        f"Delivery already recorded for schedule {schedule_id}",
                        {"schedule_id": schedule_id}
                    )
                
                updated_schedule = DeliverySchedule(
                    schedule_id=schedule.schedule_id,
                    delivery_date=schedule.delivery_date,
                    scheduled_mwh=schedule.scheduled_mwh,
                    actual_mwh=actual_mwh,
                    delivery_location=schedule.delivery_location
                )
                
                self._delivery_schedules[i] = updated_schedule
                self.updated_at = datetime.utcnow()
                
                self.record_event(ActualDeliveryRecordedEvent(
                    aggregate_id=self.id,
                    contract_number=self.contract_number,
                    schedule_id=schedule_id,
                    scheduled_mwh=schedule.scheduled_mwh,
                    actual_mwh=actual_mwh,
                    variance=updated_schedule.delivery_variance
                ))
                return
        
        raise ValidationError(f"Delivery schedule {schedule_id} not found")
    
    def get_total_scheduled_mwh(self) -> Decimal:
        """Calculate total scheduled energy delivery."""
        return sum(s.scheduled_mwh for s in self._delivery_schedules)
    
    def get_total_delivered_mwh(self) -> Decimal:
        """Calculate total energy actually delivered."""
        return sum(
            s.actual_mwh
            for s in self._delivery_schedules
            if s.actual_mwh is not None
        )
    
    def get_delivery_performance(self) -> Decimal:
        """Calculate delivery performance (actual/scheduled).
        
        Returns:
            Percentage of scheduled energy delivered (0-100+)
        """
        total_scheduled = self.get_total_scheduled_mwh()
        if total_scheduled == Decimal('0'):
            return Decimal('0')
        
        total_delivered = self.get_total_delivered_mwh()
        return (total_delivered / total_scheduled) * Decimal('100')


# Domain Events

@dataclass
class PPACreatedEvent(DomainEvent):
    """Event raised when a PPA is created."""
    
    contract_number: str
    buyer_name: str
    seller_name: str
    tenant_id: TenantId


@dataclass
class PPAActivatedEvent(DomainEvent):
    """Event raised when a PPA is activated."""
    
    contract_number: str
    buyer_name: str
    seller_name: str
    effective_date: datetime


@dataclass
class PPASuspendedEvent(DomainEvent):
    """Event raised when a PPA is suspended."""
    
    contract_number: str
    reason: str


@dataclass
class PPAResumedEvent(DomainEvent):
    """Event raised when a PPA is resumed."""
    
    contract_number: str


@dataclass
class PPATerminatedEvent(DomainEvent):
    """Event raised when a PPA is terminated."""
    
    contract_number: str
    reason: str


@dataclass
class DeliveryScheduleAddedEvent(DomainEvent):
    """Event raised when a delivery schedule is added."""
    
    contract_number: str
    schedule_id: str
    delivery_date: datetime
    scheduled_mwh: Decimal


@dataclass
class ActualDeliveryRecordedEvent(DomainEvent):
    """Event raised when actual delivery is recorded."""
    
    contract_number: str
    schedule_id: str
    scheduled_mwh: Decimal
    actual_mwh: Decimal
    variance: Optional[Decimal]

