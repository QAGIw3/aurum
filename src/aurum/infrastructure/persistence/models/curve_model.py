"""SQLAlchemy ORM models for Curve aggregate."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class CurveORM(Base):
    """ORM model for Curve aggregate root."""
    
    __tablename__ = "curves"
    
    id = Column(PG_UUID(as_uuid=True), primary_key=True)
    tenant_id = Column(PG_UUID(as_uuid=True), nullable=False, index=True)
    
    # Metadata
    curve_key = Column(String(255), nullable=False, index=True)
    as_of_date = Column(DateTime, nullable=False, index=True)
    currency = Column(String(3), nullable=False, default="USD")
    tenor_type = Column(String(50))
    price_type = Column(String(50))
    day_count = Column(String(50))
    calendar = Column(String(50))
    asset_class = Column(String(50))
    source = Column(String(100))
    
    measure = Column(String(50), nullable=False, default="value")
    
    # Audit fields
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    version = Column(Integer, nullable=False, default=0)
    
    # Relationships
    points = relationship("CurvePointORM", back_populates="curve", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<CurveORM(id={self.id}, curve_key={self.curve_key}, as_of_date={self.as_of_date})>"


class CurvePointORM(Base):
    """ORM model for CurvePoint value object."""
    
    __tablename__ = "curve_points"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    curve_id = Column(PG_UUID(as_uuid=True), ForeignKey("curves.id"), nullable=False, index=True)
    
    tenor = Column(Numeric(precision=20, scale=6), nullable=False)
    value = Column(Numeric(precision=20, scale=6), nullable=False)
    timestamp = Column(DateTime)
    quality_flag = Column(String(50))
    
    # Relationship
    curve = relationship("CurveORM", back_populates="points")
    
    def __repr__(self):
        return f"<CurvePointORM(curve_id={self.curve_id}, tenor={self.tenor}, value={self.value})>"

