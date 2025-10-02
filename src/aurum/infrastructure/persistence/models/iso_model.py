"""SQLAlchemy ORM models for ISO Market aggregate."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class IsoMarketORM(Base):
    """ORM model for IsoMarket aggregate root."""
    
    __tablename__ = "iso_markets"
    
    id = Column(PG_UUID(as_uuid=True), primary_key=True)
    tenant_id = Column(PG_UUID(as_uuid=True), nullable=False, index=True)
    
    iso_code = Column(String(10), nullable=False, unique=True, index=True)
    iso_name = Column(String(255), nullable=False)
    timezone = Column(String(50), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    
    # Audit fields
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    version = Column(Integer, nullable=False, default=0)
    
    # Relationships
    lmp_data = relationship("LMPDataORM", back_populates="iso_market", cascade="all, delete-orphan")
    load_data = relationship("LoadDataORM", back_populates="iso_market", cascade="all, delete-orphan")
    generation_data = relationship("GenerationMixORM", back_populates="iso_market", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<IsoMarketORM(id={self.id}, iso_code={self.iso_code}, iso_name={self.iso_name})>"


class LMPDataORM(Base):
    """ORM model for Locational Marginal Price data."""
    
    __tablename__ = "lmp_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    iso_market_id = Column(PG_UUID(as_uuid=True), ForeignKey("iso_markets.id"), nullable=False, index=True)
    
    node_id = Column(String(100), nullable=False, index=True)
    location_zone = Column(String(100))
    location_node = Column(String(100))
    
    energy_price = Column(Numeric(precision=20, scale=6), nullable=False)
    congestion_price = Column(Numeric(precision=20, scale=6), nullable=False)
    loss_price = Column(Numeric(precision=20, scale=6), nullable=False)
    
    timestamp = Column(DateTime, nullable=False, index=True)
    market_type = Column(String(10), nullable=False)  # DAM, RTM, HAM
    
    # Relationship
    iso_market = relationship("IsoMarketORM", back_populates="lmp_data")
    
    def __repr__(self):
        return f"<LMPDataORM(node_id={self.node_id}, timestamp={self.timestamp})>"


class LoadDataORM(Base):
    """ORM model for system load data."""
    
    __tablename__ = "load_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    iso_market_id = Column(PG_UUID(as_uuid=True), ForeignKey("iso_markets.id"), nullable=False, index=True)
    
    zone_id = Column(String(100), nullable=False, index=True)
    load_mw = Column(Numeric(precision=20, scale=6), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    forecast = Column(Boolean, nullable=False, default=False)
    
    # Relationship
    iso_market = relationship("IsoMarketORM", back_populates="load_data")
    
    def __repr__(self):
        return f"<LoadDataORM(zone_id={self.zone_id}, load_mw={self.load_mw}, timestamp={self.timestamp})>"


class GenerationMixORM(Base):
    """ORM model for generation mix data."""
    
    __tablename__ = "generation_mix_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    iso_market_id = Column(PG_UUID(as_uuid=True), ForeignKey("iso_markets.id"), nullable=False, index=True)
    
    zone_id = Column(String(100), nullable=False, index=True)
    fuel_type = Column(String(50), nullable=False, index=True)
    generation_mw = Column(Numeric(precision=20, scale=6), nullable=False)
    percentage = Column(Numeric(precision=5, scale=2), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # Relationship
    iso_market = relationship("IsoMarketORM", back_populates="generation_data")
    
    def __repr__(self):
        return f"<GenerationMixORM(zone_id={self.zone_id}, fuel_type={self.fuel_type}, timestamp={self.timestamp})>"

