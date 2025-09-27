"""TimescaleDB repository implementation using async SQLAlchemy 2.0."""
from __future__ import annotations

import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

from libs.core import CurveKey, PriceObservation
from libs.common.config import DatabaseSettings
from .ports import SeriesRepository

logger = logging.getLogger(__name__)


class TimescaleSeriesRepo(SeriesRepository):
    """TimescaleDB implementation of SeriesRepository using async SQLAlchemy 2.0."""
    
    def __init__(self, db_settings: DatabaseSettings):
        self.db_settings = db_settings
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[sessionmaker] = None
        
    async def _get_engine(self) -> AsyncEngine:
        """Get or create async database engine."""
        if self._engine is None:
            self._engine = create_async_engine(
                self.db_settings.timescale_dsn,
                pool_size=self.db_settings.pool_size,
                max_overflow=self.db_settings.max_overflow,
                pool_timeout=self.db_settings.pool_timeout,
                pool_recycle=self.db_settings.pool_recycle,
                echo=False,  # Set to True for SQL debugging
            )
            self._session_factory = sessionmaker(
                self._engine, 
                class_=AsyncSession,
                expire_on_commit=False
            )
        return self._engine
    
    async def _get_session(self) -> AsyncSession:
        """Get async database session."""
        if self._session_factory is None:
            await self._get_engine()
        return self._session_factory()
    
    async def get_observations(
        self,
        curve: CurveKey,
        start_date: date,
        end_date: date,
        limit: Optional[int] = None,
    ) -> List[PriceObservation]:
        """Get price observations using SQLAlchemy 2.0 style queries."""
        async with await self._get_session() as session:
            # Using raw SQL for now - would be replaced with SQLAlchemy models
            query = text("""
                SELECT 
                    iso_code,
                    market_type,
                    location,
                    product,
                    price_block,
                    interval_start,
                    interval_end,
                    delivery_date,
                    price,
                    currency_code,
                    unit_code
                FROM iso_lmp_unified 
                WHERE iso_code = :iso
                  AND market_type = :market
                  AND location = :location
                  AND delivery_date >= :start_date
                  AND delivery_date <= :end_date
                  AND (:product IS NULL OR product = :product)
                ORDER BY interval_start DESC
                LIMIT :limit
            """)
            
            result = await session.execute(query, {
                'iso': curve.iso.value,
                'market': curve.market.value,
                'location': curve.location,
                'product': curve.product,
                'start_date': start_date,
                'end_date': end_date,
                'limit': limit or 10000,
            })
            
            observations = []
            for row in result.fetchall():
                # Create CurveKey from row data
                row_curve = CurveKey(
                    iso=row.iso_code,
                    market=row.market_type,
                    location=row.location,
                    product=row.product,
                    block=row.price_block,
                )
                
                # Create PriceObservation
                obs = PriceObservation(
                    curve=row_curve,
                    interval_start=row.interval_start,
                    interval_end=row.interval_end,
                    delivery_date=row.delivery_date,
                    price=row.price,
                    currency=row.currency_code or 'USD',
                    unit=row.unit_code or 'MWh',
                )
                observations.append(obs)
            
            return observations
    
    async def store_observations(
        self,
        observations: List[PriceObservation],
    ) -> int:
        """Store price observations using upsert logic."""
        if not observations:
            return 0
            
        async with await self._get_session() as session:
            # Convert observations to dict format for bulk insert
            data = []
            for obs in observations:
                data.append({
                    'iso_code': obs.curve.iso.value,
                    'market_type': obs.curve.market.value,
                    'location': obs.curve.location,
                    'product': obs.curve.product,
                    'price_block': obs.curve.block.value if obs.curve.block else None,
                    'interval_start': obs.interval_start,
                    'interval_end': obs.interval_end,
                    'delivery_date': obs.delivery_date,
                    'price': obs.price,
                    'currency_code': obs.currency.value,
                    'unit_code': obs.unit.value,
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow(),
                })
            
            # Use PostgreSQL-specific upsert
            query = text("""
                INSERT INTO iso_lmp_unified (
                    iso_code, market_type, location, product, price_block,
                    interval_start, interval_end, delivery_date, price,
                    currency_code, unit_code, created_at, updated_at
                )
                SELECT 
                    unnest(:iso_codes) as iso_code,
                    unnest(:market_types) as market_type,
                    unnest(:locations) as location,
                    unnest(:products) as product,
                    unnest(:price_blocks) as price_block,
                    unnest(:interval_starts) as interval_start,
                    unnest(:interval_ends) as interval_end,
                    unnest(:delivery_dates) as delivery_date,
                    unnest(:prices) as price,
                    unnest(:currency_codes) as currency_code,
                    unnest(:unit_codes) as unit_code,
                    unnest(:created_ats) as created_at,
                    unnest(:updated_ats) as updated_at
                ON CONFLICT (iso_code, market_type, location, interval_start, delivery_date)
                DO UPDATE SET
                    price = EXCLUDED.price,
                    updated_at = EXCLUDED.updated_at
            """)
            
            # Extract arrays for bulk insert
            params = {
                'iso_codes': [d['iso_code'] for d in data],
                'market_types': [d['market_type'] for d in data],
                'locations': [d['location'] for d in data],
                'products': [d['product'] for d in data],
                'price_blocks': [d['price_block'] for d in data],
                'interval_starts': [d['interval_start'] for d in data],
                'interval_ends': [d['interval_end'] for d in data],
                'delivery_dates': [d['delivery_date'] for d in data],
                'prices': [d['price'] for d in data],
                'currency_codes': [d['currency_code'] for d in data],
                'unit_codes': [d['unit_code'] for d in data],
                'created_ats': [d['created_at'] for d in data],
                'updated_ats': [d['updated_at'] for d in data],
            }
            
            result = await session.execute(query, params)
            await session.commit()
            
            return len(observations)
    
    async def get_curve_metadata(
        self,
        curve: CurveKey,
    ) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific curve."""
        async with await self._get_session() as session:
            query = text("""
                SELECT 
                    iso_code,
                    market_type,
                    location,
                    product,
                    MIN(delivery_date) as start_date,
                    MAX(delivery_date) as end_date,
                    COUNT(*) as observation_count,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price
                FROM iso_lmp_unified 
                WHERE iso_code = :iso
                  AND market_type = :market
                  AND location = :location
                  AND (:product IS NULL OR product = :product)
                GROUP BY iso_code, market_type, location, product
            """)
            
            result = await session.execute(query, {
                'iso': curve.iso.value,
                'market': curve.market.value,
                'location': curve.location,
                'product': curve.product,
            })
            
            row = result.fetchone()
            if not row:
                return None
                
            return {
                'curve': curve.model_dump(),
                'start_date': row.start_date.isoformat() if row.start_date else None,
                'end_date': row.end_date.isoformat() if row.end_date else None,
                'observation_count': row.observation_count,
                'statistics': {
                    'avg_price': float(row.avg_price) if row.avg_price else None,
                    'min_price': float(row.min_price) if row.min_price else None,
                    'max_price': float(row.max_price) if row.max_price else None,
                }
            }
    
    async def list_curves(
        self,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Tuple[List[CurveKey], int]:
        """List available curves with optional filtering."""
        async with await self._get_session() as session:
            # Count query
            count_query = text("""
                SELECT COUNT(DISTINCT (iso_code, market_type, location, product))
                FROM iso_lmp_unified
                WHERE (:iso IS NULL OR iso_code = :iso)
                  AND (:market IS NULL OR market_type = :market)
            """)
            
            count_result = await session.execute(count_query, {
                'iso': iso,
                'market': market,
            })
            total_count = count_result.scalar() or 0
            
            # Data query
            data_query = text("""
                SELECT DISTINCT 
                    iso_code,
                    market_type,
                    location,
                    product,
                    price_block
                FROM iso_lmp_unified
                WHERE (:iso IS NULL OR iso_code = :iso)
                  AND (:market IS NULL OR market_type = :market)
                ORDER BY iso_code, market_type, location, product
                LIMIT :limit OFFSET :offset
            """)
            
            data_result = await session.execute(data_query, {
                'iso': iso,
                'market': market,
                'limit': limit or 1000,
                'offset': offset or 0,
            })
            
            curves = []
            for row in data_result.fetchall():
                curve = CurveKey(
                    iso=row.iso_code,
                    market=row.market_type,
                    location=row.location,
                    product=row.product,
                    block=row.price_block,
                )
                curves.append(curve)
            
            return curves, total_count
    
    async def close(self):
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None