"""Read models for curve queries - optimized for reads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import Column, DateTime, Float, Integer, String, Index
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CurveSummaryProjection(Base):
    """Materialized view / projection for curve summaries.
    
    This is a denormalized read model optimized for fast queries.
    Updated by event handlers when curves change.
    """
    
    __tablename__ = "curve_summary_projection"
    
    # Identifiers
    curve_id = Column(PG_UUID(as_uuid=True), primary_key=True)
    tenant_id = Column(PG_UUID(as_uuid=True), nullable=False, index=True)
    curve_key = Column(String(255), nullable=False, index=True)
    
    # Summary statistics (denormalized for fast access)
    as_of_date = Column(DateTime, nullable=False, index=True)
    point_count = Column(Integer, nullable=False)
    min_tenor = Column(Float)
    max_tenor = Column(Float)
    min_value = Column(Float)
    max_value = Column(Float)
    avg_value = Column(Float)
    
    # Metadata (commonly queried fields)
    currency = Column(String(3), nullable=False)
    tenor_type = Column(String(50))
    price_type = Column(String(50))
    asset_class = Column(String(50))
    
    # Timestamps
    last_updated = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    # Indexes for common queries
    __table_args__ = (
        Index('ix_curve_summary_tenant_key', 'tenant_id', 'curve_key'),
        Index('ix_curve_summary_tenant_date', 'tenant_id', 'as_of_date'),
        Index('ix_curve_summary_asset_date', 'asset_class', 'as_of_date'),
    )


@dataclass
class CurveReadModel:
    """Read model DTO for curve data."""
    
    curve_id: str
    tenant_id: str
    curve_key: str
    as_of_date: datetime
    
    # Summary statistics
    point_count: int
    min_tenor: Optional[Decimal]
    max_tenor: Optional[Decimal]
    min_value: Optional[Decimal]
    max_value: Optional[Decimal]
    avg_value: Optional[Decimal]
    
    # Metadata
    currency: str
    tenor_type: Optional[str]
    price_type: Optional[str]
    asset_class: Optional[str]
    
    last_updated: datetime
    
    @classmethod
    def from_projection(cls, projection: CurveSummaryProjection) -> CurveReadModel:
        """Create read model from projection.
        
        Args:
            projection: Database projection
            
        Returns:
            Read model DTO
        """
        return cls(
            curve_id=str(projection.curve_id),
            tenant_id=str(projection.tenant_id),
            curve_key=projection.curve_key,
            as_of_date=projection.as_of_date,
            point_count=projection.point_count,
            min_tenor=Decimal(str(projection.min_tenor)) if projection.min_tenor else None,
            max_tenor=Decimal(str(projection.max_tenor)) if projection.max_tenor else None,
            min_value=Decimal(str(projection.min_value)) if projection.min_value else None,
            max_value=Decimal(str(projection.max_value)) if projection.max_value else None,
            avg_value=Decimal(str(projection.avg_value)) if projection.avg_value else None,
            currency=projection.currency,
            tenor_type=projection.tenor_type,
            price_type=projection.price_type,
            asset_class=projection.asset_class,
            last_updated=projection.last_updated,
        )


class CurveReadModelService:
    """Service for querying curve read models.
    
    Provides optimized queries for common use cases without
    needing to load full aggregates.
    """
    
    def __init__(self, session):
        """Initialize with database session.
        
        Args:
            session: SQLAlchemy async session
        """
        self.session = session
    
    async def get_curve_summary(self, curve_id: str) -> Optional[CurveReadModel]:
        """Get curve summary by ID.
        
        Args:
            curve_id: Curve identifier
            
        Returns:
            Curve read model or None
        """
        from sqlalchemy import select
        
        stmt = select(CurveSummaryProjection).where(
            CurveSummaryProjection.curve_id == curve_id
        )
        
        result = await self.session.execute(stmt)
        projection = result.scalar_one_or_none()
        
        if projection is None:
            return None
        
        return CurveReadModel.from_projection(projection)
    
    async def list_curves_for_tenant(
        self,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[CurveReadModel]:
        """List curves for a tenant.
        
        Args:
            tenant_id: Tenant identifier
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            List of curve read models
        """
        from sqlalchemy import select
        
        stmt = (
            select(CurveSummaryProjection)
            .where(CurveSummaryProjection.tenant_id == tenant_id)
            .order_by(CurveSummaryProjection.as_of_date.desc())
            .limit(limit)
            .offset(offset)
        )
        
        result = await self.session.execute(stmt)
        projections = result.scalars().all()
        
        return [CurveReadModel.from_projection(p) for p in projections]
    
    async def search_curves(
        self,
        tenant_id: str,
        curve_key_pattern: Optional[str] = None,
        asset_class: Optional[str] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[CurveReadModel]:
        """Search curves with filters.
        
        Args:
            tenant_id: Tenant identifier
            curve_key_pattern: Pattern to match curve keys (SQL LIKE)
            asset_class: Filter by asset class
            from_date: Filter curves from this date
            to_date: Filter curves to this date
            limit: Maximum results
            
        Returns:
            List of matching curves
        """
        from sqlalchemy import select
        
        stmt = select(CurveSummaryProjection).where(
            CurveSummaryProjection.tenant_id == tenant_id
        )
        
        if curve_key_pattern:
            stmt = stmt.where(CurveSummaryProjection.curve_key.like(curve_key_pattern))
        
        if asset_class:
            stmt = stmt.where(CurveSummaryProjection.asset_class == asset_class)
        
        if from_date:
            stmt = stmt.where(CurveSummaryProjection.as_of_date >= from_date)
        
        if to_date:
            stmt = stmt.where(CurveSummaryProjection.as_of_date <= to_date)
        
        stmt = stmt.order_by(CurveSummaryProjection.as_of_date.desc()).limit(limit)
        
        result = await self.session.execute(stmt)
        projections = result.scalars().all()
        
        return [CurveReadModel.from_projection(p) for p in projections]

