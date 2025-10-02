"""Read models for ISO market queries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import Column, DateTime, Float, Integer, String, Index
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class LMPSummaryProjection(Base):
    """Projection for LMP statistics by node and time period.
    
    Pre-aggregated statistics for fast analytics queries.
    """
    
    __tablename__ = "lmp_summary_projection"
    
    # Composite key
    iso_code = Column(String(10), primary_key=True)
    node_id = Column(String(100), primary_key=True)
    date = Column(DateTime, primary_key=True)
    market_type = Column(String(10), primary_key=True)
    
    # Aggregated statistics
    avg_energy_price = Column(Float)
    min_energy_price = Column(Float)
    max_energy_price = Column(Float)
    avg_congestion_price = Column(Float)
    avg_loss_price = Column(Float)
    avg_total_price = Column(Float)
    data_points = Column(Integer)  # Number of observations
    
    # Timestamps
    last_updated = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_lmp_summary_iso_date', 'iso_code', 'date'),
        Index('ix_lmp_summary_node_date', 'node_id', 'date'),
    )


@dataclass
class IsoMarketReadModel:
    """Read model for ISO market data."""
    
    iso_code: str
    node_id: str
    date: datetime
    market_type: str
    
    avg_energy_price: Decimal
    min_energy_price: Decimal
    max_energy_price: Decimal
    avg_congestion_price: Decimal
    avg_loss_price: Decimal
    avg_total_price: Decimal
    data_points: int
    
    last_updated: datetime
    
    @classmethod
    def from_projection(cls, projection: LMPSummaryProjection) -> IsoMarketReadModel:
        """Create read model from projection."""
        return cls(
            iso_code=projection.iso_code,
            node_id=projection.node_id,
            date=projection.date,
            market_type=projection.market_type,
            avg_energy_price=Decimal(str(projection.avg_energy_price)),
            min_energy_price=Decimal(str(projection.min_energy_price)),
            max_energy_price=Decimal(str(projection.max_energy_price)),
            avg_congestion_price=Decimal(str(projection.avg_congestion_price)),
            avg_loss_price=Decimal(str(projection.avg_loss_price)),
            avg_total_price=Decimal(str(projection.avg_total_price)),
            data_points=projection.data_points,
            last_updated=projection.last_updated,
        )

