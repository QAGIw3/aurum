"""Timescale-specific operations for performance optimization."""
from __future__ import annotations

import logging
from typing import Dict, List, Any, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .timescale import TimescaleSeriesRepo

logger = logging.getLogger(__name__)


class TimescalePerformanceOps:
    """Performance optimization operations for TimescaleDB."""
    
    def __init__(self, timescale_repo: TimescaleSeriesRepo):
        self.repo = timescale_repo
    
    async def ensure_hypertables(self) -> Dict[str, Any]:
        """Ensure hypertables are properly configured."""
        async with await self.repo._get_session() as session:
            operations = []
            
            # Check if iso_lmp_unified is a hypertable
            check_query = text("""
                SELECT tablename, schemaname 
                FROM _timescaledb_catalog.hypertable 
                WHERE tablename = 'iso_lmp_unified'
            """)
            
            result = await session.execute(check_query)
            existing_hypertables = result.fetchall()
            
            if not existing_hypertables:
                # Create hypertable
                hypertable_query = text("""
                    SELECT create_hypertable(
                        'iso_lmp_unified', 
                        'interval_start',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE
                    )
                """)
                
                try:
                    await session.execute(hypertable_query)
                    operations.append("created_hypertable_iso_lmp_unified")
                    logger.info("Created hypertable for iso_lmp_unified")
                except Exception as e:
                    logger.warning(f"Failed to create hypertable: {e}")
                    operations.append(f"hypertable_error: {e}")
            else:
                operations.append("hypertable_already_exists")
            
            return {
                "operations": operations,
                "existing_hypertables": len(existing_hypertables),
            }
    
    async def configure_compression(self) -> Dict[str, Any]:
        """Configure native compression policies."""
        async with await self.repo._get_session() as session:
            operations = []
            
            try:
                # Enable compression on iso_lmp_unified
                compression_query = text("""
                    ALTER TABLE iso_lmp_unified SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'iso_code,market_type,location',
                        timescaledb.compress_orderby = 'interval_start DESC'
                    )
                """)
                
                await session.execute(compression_query)
                operations.append("enabled_compression")
                
                # Add compression policy (compress chunks older than 7 days)
                policy_query = text("""
                    SELECT add_compression_policy(
                        'iso_lmp_unified', 
                        INTERVAL '7 days',
                        if_not_exists => TRUE
                    )
                """)
                
                await session.execute(policy_query)
                operations.append("added_compression_policy")
                
                logger.info("Configured compression for iso_lmp_unified")
                
            except Exception as e:
                logger.warning(f"Failed to configure compression: {e}")
                operations.append(f"compression_error: {e}")
            
            return {"operations": operations}
    
    async def configure_retention(self, retention_days: int = 365) -> Dict[str, Any]:
        """Configure data retention policy."""
        async with await self.repo._get_session() as session:
            operations = []
            
            try:
                retention_query = text("""
                    SELECT add_retention_policy(
                        'iso_lmp_unified',
                        INTERVAL :retention_interval,
                        if_not_exists => TRUE
                    )
                """)
                
                await session.execute(retention_query, {
                    'retention_interval': f'{retention_days} days'
                })
                operations.append(f"added_retention_policy_{retention_days}_days")
                
                logger.info(f"Configured {retention_days} day retention policy")
                
            except Exception as e:
                logger.warning(f"Failed to configure retention: {e}")
                operations.append(f"retention_error: {e}")
            
            return {"operations": operations}
    
    async def create_continuous_aggregates(self) -> Dict[str, Any]:
        """Create continuous aggregates for heavy rollups."""
        async with await self.repo._get_session() as session:
            operations = []
            
            # Hourly aggregate
            hourly_cagg_query = text("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_price_summary
                WITH (timescaledb.continuous) AS
                SELECT 
                    time_bucket('1 hour', interval_start) AS hour_bucket,
                    iso_code,
                    market_type,
                    location,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    COUNT(*) as observation_count
                FROM iso_lmp_unified
                GROUP BY hour_bucket, iso_code, market_type, location
                WITH DATA
            """)
            
            try:
                await session.execute(hourly_cagg_query)
                operations.append("created_hourly_cagg")
                
                # Add refresh policy
                refresh_policy_query = text("""
                    SELECT add_continuous_aggregate_policy(
                        'hourly_price_summary',
                        start_offset => INTERVAL '2 hours',
                        end_offset => INTERVAL '1 hour',
                        schedule_interval => INTERVAL '1 hour',
                        if_not_exists => TRUE
                    )
                """)
                
                await session.execute(refresh_policy_query)
                operations.append("added_hourly_cagg_refresh_policy")
                
                logger.info("Created hourly continuous aggregate")
                
            except Exception as e:
                logger.warning(f"Failed to create hourly CAGG: {e}")
                operations.append(f"hourly_cagg_error: {e}")
            
            # Daily aggregate
            daily_cagg_query = text("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS daily_price_summary
                WITH (timescaledb.continuous) AS
                SELECT 
                    time_bucket('1 day', interval_start) AS day_bucket,
                    iso_code,
                    market_type,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    COUNT(*) as observation_count
                FROM iso_lmp_unified
                GROUP BY day_bucket, iso_code, market_type
                WITH DATA
            """)
            
            try:
                await session.execute(daily_cagg_query)
                operations.append("created_daily_cagg")
                
                # Add refresh policy
                daily_refresh_policy_query = text("""
                    SELECT add_continuous_aggregate_policy(
                        'daily_price_summary',
                        start_offset => INTERVAL '1 day',
                        end_offset => INTERVAL '1 hour',
                        schedule_interval => INTERVAL '1 hour',
                        if_not_exists => TRUE
                    )
                """)
                
                await session.execute(daily_refresh_policy_query)
                operations.append("added_daily_cagg_refresh_policy")
                
                logger.info("Created daily continuous aggregate")
                
            except Exception as e:
                logger.warning(f"Failed to create daily CAGG: {e}")
                operations.append(f"daily_cagg_error: {e}")
            
            await session.commit()
            return {"operations": operations}
    
    async def get_hypertable_stats(self) -> Dict[str, Any]:
        """Get hypertable performance statistics."""
        async with await self.repo._get_session() as session:
            stats_query = text("""
                SELECT 
                    schemaname,
                    tablename,
                    num_chunks,
                    table_bytes,
                    index_bytes,
                    toast_bytes,
                    total_bytes,
                    compression_status
                FROM timescaledb_information.hypertables h
                LEFT JOIN (
                    SELECT 
                        hypertable_schema,
                        hypertable_name,
                        CASE 
                            WHEN count(*) FILTER (WHERE compression_status = 'Compressed') > 0 
                            THEN 'Partially Compressed'
                            ELSE 'Uncompressed'
                        END as compression_status
                    FROM timescaledb_information.chunks
                    GROUP BY hypertable_schema, hypertable_name
                ) c ON h.schemaname = c.hypertable_schema 
                     AND h.tablename = c.hypertable_name
                WHERE tablename = 'iso_lmp_unified'
            """)
            
            result = await session.execute(stats_query)
            row = result.fetchone()
            
            if row:
                return {
                    "schema": row.schemaname,
                    "table": row.tablename,
                    "num_chunks": row.num_chunks,
                    "table_bytes": row.table_bytes,
                    "index_bytes": row.index_bytes,
                    "total_bytes": row.total_bytes,
                    "compression_status": row.compression_status,
                }
            
            return {"error": "Hypertable not found"}
    
    async def optimize_all(self) -> Dict[str, Any]:
        """Run all optimization operations."""
        results = {}
        
        # Ensure hypertables
        results["hypertables"] = await self.ensure_hypertables()
        
        # Configure compression
        results["compression"] = await self.configure_compression()
        
        # Configure retention (1 year default)
        results["retention"] = await self.configure_retention(365)
        
        # Create continuous aggregates
        results["continuous_aggregates"] = await self.create_continuous_aggregates()
        
        # Get final stats
        results["stats"] = await self.get_hypertable_stats()
        
        return results