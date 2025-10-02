"""SQLAlchemy ORM models for PPA aggregate."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class PPAORM(Base):
    """ORM model for PowerPurchaseAgreement aggregate root."""
    
    __tablename__ = "ppas"
    
    id = Column(PG_UUID(as_uuid=True), primary_key=True)
    tenant_id = Column(PG_UUID(as_uuid=True), nullable=False, index=True)
    
    contract_number = Column(String(100), nullable=False, unique=True, index=True)
    buyer_name = Column(String(255), nullable=False)
    seller_name = Column(String(255), nullable=False)
    status = Column(String(50), nullable=False, index=True)  # draft, negotiation, active, suspended, completed, terminated
    
    # Terms
    pricing_type = Column(String(50), nullable=False)  # fixed, indexed, collar, hybrid
    fixed_price_amount = Column(Numeric(precision=20, scale=6))
    fixed_price_currency = Column(String(3))
    floor_price_amount = Column(Numeric(precision=20, scale=6))
    floor_price_currency = Column(String(3))
    ceiling_price_amount = Column(Numeric(precision=20, scale=6))
    ceiling_price_currency = Column(String(3))
    index_reference = Column(String(100))
    index_multiplier = Column(Numeric(precision=10, scale=4), default=1.0)
    
    delivery_start = Column(DateTime)
    delivery_end = Column(DateTime)
    minimum_annual_mwh = Column(Numeric(precision=20, scale=6))
    maximum_annual_mwh = Column(Numeric(precision=20, scale=6))
    
    # Contract metadata
    signed_date = Column(DateTime)
    effective_date = Column(DateTime)
    expiration_date = Column(DateTime)
    
    # Audit fields
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    version = Column(Integer, nullable=False, default=0)
    
    # Relationships
    delivery_schedules = relationship("DeliveryScheduleORM", back_populates="ppa", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<PPAORM(id={self.id}, contract_number={self.contract_number}, status={self.status})>"


class DeliveryScheduleORM(Base):
    """ORM model for DeliverySchedule value object."""
    
    __tablename__ = "delivery_schedules"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ppa_id = Column(PG_UUID(as_uuid=True), ForeignKey("ppas.id"), nullable=False, index=True)
    
    schedule_id = Column(String(100), nullable=False, unique=True, index=True)
    delivery_date = Column(DateTime, nullable=False, index=True)
    scheduled_mwh = Column(Numeric(precision=20, scale=6), nullable=False)
    actual_mwh = Column(Numeric(precision=20, scale=6))
    delivery_location = Column(String(255))
    
    # Relationship
    ppa = relationship("PPAORM", back_populates="delivery_schedules")
    
    def __repr__(self):
        return f"<DeliveryScheduleORM(schedule_id={self.schedule_id}, delivery_date={self.delivery_date})>"

