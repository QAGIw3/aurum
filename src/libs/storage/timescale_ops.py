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
            operations: List[str] = []

            # Check if iso_lmp_unified is a hypertable
            check_query = text(
                """
                SELECT tablename, schemaname
                FROM _timescaledb_catalog.hypertable
                WHERE tablename = 'iso_lmp_unified'
                """
            )

            result = await session.execute(check_query)
            existing_hypertables = result.fetchall()

            if not existing_hypertables:
                hypertable_query = text(
                    """
                    SELECT create_hypertable(
                        'iso_lmp_unified',
                        'interval_start',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE
                    )
                    """
                )

                try:
                    await session.execute(hypertable_query)
                    await session.commit()
                    operations.append("created_hypertable_iso_lmp_unified")
                    logger.info("Created hypertable for iso_lmp_unified")
                except Exception as exc:
                    await session.rollback()
                    logger.warning("Failed to create hypertable: %s", exc)
                    operations.append(f"hypertable_error: {exc}")
            else:
                operations.append("hypertable_already_exists")

            # Ensure chunk interval (applies both after creation and for existing tables)
            chunk_interval_query = text(
                """
                SELECT set_chunk_time_interval('iso_lmp_unified', INTERVAL '1 day')
                """
            )

            try:
                await session.execute(chunk_interval_query)
                await session.commit()
                operations.append("chunk_interval_set_1_day")
            except Exception as exc:
                await session.rollback()
                logger.debug("Failed to set chunk interval: %s", exc)
                operations.append(f"chunk_interval_warning: {exc}")

            return {
                "operations": operations,
                "existing_hypertables": len(existing_hypertables),
            }
    
    async def configure_compression(self) -> Dict[str, Any]:
        """Configure native compression policies."""
        async with await self.repo._get_session() as session:
            operations = []
            
            try:
                compression_query = text(
                    """
                    ALTER TABLE iso_lmp_unified SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'iso_code,market_type,location',
                        timescaledb.compress_orderby = 'interval_start DESC'
                    )
                    """
                )

                await session.execute(compression_query)
                operations.append("enabled_compression")

                policy_query = text(
                    """
                    SELECT add_compression_policy(
                        'iso_lmp_unified',
                        INTERVAL '7 days',
                        if_not_exists => TRUE
                    )
                    """
                )

                await session.execute(policy_query)
                operations.append("added_compression_policy")

                logger.info("Configured compression for iso_lmp_unified")

                await session.commit()

            except Exception as exc:
                await session.rollback()
                logger.warning("Failed to configure compression: %s", exc)
                operations.append(f"compression_error: {exc}")

            return {"operations": operations}

    async def configure_retention(
        self,
        raw_retention_days: int = 365,
        aggregate_retention_days: Optional[Dict[str, int]] = None,
    ) -> Dict[str, Any]:
        """Configure retention policy for raw and aggregate data."""
        async with await self.repo._get_session() as session:
            operations: List[str] = []

            try:
                raw_retention_query = text(
                    f"""
                    SELECT add_retention_policy(
                        'iso_lmp_unified',
                        INTERVAL '{raw_retention_days} days',
                        if_not_exists => TRUE
                    )
                    """
                )

                await session.execute(raw_retention_query)
                operations.append(f"raw_retention_{raw_retention_days}_days")
                logger.info("Configured raw retention for iso_lmp_unified", extra={"days": raw_retention_days})
                await session.commit()

            except Exception as exc:
                await session.rollback()
                logger.warning("Failed to configure raw retention: %s", exc)
                operations.append(f"raw_retention_error: {exc}")

            if aggregate_retention_days:
                for table_name, days in aggregate_retention_days.items():
                    retention_sql = text(
                        f"""
                        SELECT add_retention_policy(
                            '{table_name}',
                            INTERVAL '{days} days',
                            if_not_exists => TRUE
                        )
                        """
                    )

                    try:
                        await session.execute(retention_sql)
                        operations.append(f"{table_name}_retention_{days}_days")
                        logger.info(
                            "Configured retention for continuous aggregate",
                            extra={"table": table_name, "days": days},
                        )
                        await session.commit()
                    except Exception as exc:
                        await session.rollback()
                        logger.warning(
                            "Failed to configure retention for %s: %s", table_name, exc
                        )
                        operations.append(f"{table_name}_retention_error: {exc}")

            return {"operations": operations}
    
    async def create_continuous_aggregates(self) -> Dict[str, Any]:
        """Create continuous aggregates for heavy rollups."""
        async with await self.repo._get_session() as session:
            operations: List[str] = []

            # Hourly aggregate (raw to near-real-time rollup)
            hourly_cagg_query = text(
                """
                CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_price_summary
                WITH (timescaledb.continuous) AS
                SELECT
                    time_bucket('1 hour', interval_start) AS hour_bucket,
                    iso_code,
                    market_type,
                    location,
                    AVG(price) AS avg_price,
                    MIN(price) AS min_price,
                    MAX(price) AS max_price,
                    COUNT(*) AS observation_count
                FROM iso_lmp_unified
                GROUP BY hour_bucket, iso_code, market_type, location
                WITH DATA
                """
            )

            hourly_policy_query = text(
                """
                SELECT add_continuous_aggregate_policy(
                    'hourly_price_summary',
                    start_offset => INTERVAL '2 hours',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '1 hour',
                    if_not_exists => TRUE
                )
                """
            )

            hourly_refresh_query = text(
                """
                CALL refresh_continuous_aggregate(
                    'hourly_price_summary',
                    NOW() - INTERVAL '6 hours',
                    NOW()
                )
                """
            )

            try:
                await session.execute(hourly_cagg_query)
                await session.execute(hourly_policy_query)
                await session.commit()
                operations.extend(
                    [
                        "created_hourly_cagg",
                        "added_hourly_cagg_refresh_policy",
                    ]
                )
                logger.info("Created hourly continuous aggregate")
            except Exception as exc:
                await session.rollback()
                logger.warning("Failed to create hourly CAGG: %s", exc)
                operations.append(f"hourly_cagg_error: {exc}")
            else:
                try:
                    await session.execute(hourly_refresh_query)
                    await session.commit()
                    operations.append("refreshed_hourly_cagg")
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "Failed to refresh hourly continuous aggregate: %s", exc
                    )
                    operations.append(f"hourly_cagg_refresh_error: {exc}")

            # Daily aggregate (longer horizon rollup)
            daily_cagg_query = text(
                """
                CREATE MATERIALIZED VIEW IF NOT EXISTS daily_price_summary
                WITH (timescaledb.continuous) AS
                SELECT
                    time_bucket('1 day', interval_start) AS day_bucket,
                    iso_code,
                    market_type,
                    AVG(price) AS avg_price,
                    MIN(price) AS min_price,
                    MAX(price) AS max_price,
                    COUNT(*) AS observation_count
                FROM iso_lmp_unified
                GROUP BY day_bucket, iso_code, market_type
                WITH DATA
                """
            )

            daily_policy_query = text(
                """
                SELECT add_continuous_aggregate_policy(
                    'daily_price_summary',
                    start_offset => INTERVAL '1 day',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '1 hour',
                    if_not_exists => TRUE
                )
                """
            )

            daily_refresh_query = text(
                """
                CALL refresh_continuous_aggregate(
                    'daily_price_summary',
                    NOW() - INTERVAL '14 days',
                    NOW()
                )
                """
            )

            try:
                await session.execute(daily_cagg_query)
                await session.execute(daily_policy_query)
                await session.commit()
                operations.extend(
                    [
                        "created_daily_cagg",
                        "added_daily_cagg_refresh_policy",
                    ]
                )
                logger.info("Created daily continuous aggregate")
            except Exception as exc:
                await session.rollback()
                logger.warning("Failed to create daily CAGG: %s", exc)
                operations.append(f"daily_cagg_error: {exc}")
            else:
                try:
                    await session.execute(daily_refresh_query)
                    await session.commit()
                    operations.append("refreshed_daily_cagg")
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "Failed to refresh daily continuous aggregate: %s", exc
                    )
                    operations.append(f"daily_cagg_refresh_error: {exc}")

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
        
        # Create continuous aggregates before enforcing retention so policies can target them
        results["continuous_aggregates"] = await self.create_continuous_aggregates()

        # Configure retention windows for raw timeseries and rollups
        results["retention"] = await self.configure_retention(
            raw_retention_days=180,
            aggregate_retention_days={
                "hourly_price_summary": 365,
                "daily_price_summary": 730,
            },
        )
        
        # Get final stats
        results["stats"] = await self.get_hypertable_stats()
        
        return results
