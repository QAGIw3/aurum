"""TimescaleDB maintenance backend implementation."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import asyncpg
from asyncpg import Pool

from .base import BaseMaintenanceBackend
from ..interfaces import OperationType
from aurum.core.settings import get_settings

logger = logging.getLogger(__name__)


class TimescaleMaintenanceBackend(BaseMaintenanceBackend):
    """TimescaleDB maintenance backend for hypertable operations."""
    
    def __init__(self):
        super().__init__()
        self._pool: Optional[Pool] = None
        self._settings = get_settings()
    
    @property
    def backend_type(self) -> str:
        return "timescale"
    
    @property
    def supported_operations(self) -> List[OperationType]:
        return [
            OperationType.RETENTION,
            OperationType.VACUUM,
            OperationType.COMPRESSION,
            OperationType.REORDER,
        ]
    
    async def _create_connection(self) -> Pool:
        """Create connection pool to TimescaleDB."""
        if self._pool is None:
            database_url = (
                f"postgresql://{self._settings.database.postgres_user}:"
                f"{self._settings.database.postgres_password}@"
                f"{self._settings.database.postgres_host}:"
                f"{self._settings.database.postgres_port}/"
                f"{self._settings.database.postgres_database}"
            )
            
            self._pool = await asyncpg.create_pool(
                database_url,
                min_size=2,
                max_size=10,
                command_timeout=60,
            )
            logger.info("Created TimescaleDB connection pool")
        
        return self._pool
    
    async def _close_connection(self, connection: Pool) -> None:
        """Close TimescaleDB connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Closed TimescaleDB connection pool")
    
    async def _health_check_impl(self, connection: Pool) -> bool:
        """Health check for TimescaleDB."""
        try:
            async with connection.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.error(f"TimescaleDB health check failed: {e}")
            return False
    
    async def _get_table_metadata_impl(
        self, connection: Pool, table_name: str
    ) -> Dict[str, Any]:
        """Get TimescaleDB hypertable metadata."""
        async with connection.acquire() as conn:
            # Check if it's a hypertable
            is_hypertable = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM timescaledb_information.hypertables
                    WHERE hypertable_name = $1
                )
                """,
                table_name
            )
            
            if not is_hypertable:
                # Regular table metadata
                metadata = await conn.fetchrow(
                    """
                    SELECT 
                        pg_size_pretty(pg_total_relation_size($1::regclass)) as total_size,
                        pg_size_pretty(pg_relation_size($1::regclass)) as table_size,
                        n_live_tup as row_count,
                        n_dead_tup as dead_rows
                    FROM pg_stat_user_tables
                    WHERE relname = $1
                    """,
                    table_name
                )
                
                return {
                    "table_name": table_name,
                    "backend": "timescale",
                    "is_hypertable": False,
                    "total_size": metadata["total_size"] if metadata else "0",
                    "table_size": metadata["table_size"] if metadata else "0",
                    "row_count": metadata["row_count"] if metadata else 0,
                    "dead_rows": metadata["dead_rows"] if metadata else 0,
                }
            
            # Hypertable metadata
            hypertable_info = await conn.fetchrow(
                """
                SELECT
                    h.hypertable_name,
                    h.owner,
                    h.num_dimensions,
                    h.num_chunks,
                    h.compression_enabled,
                    h.replication_factor,
                    pg_size_pretty(hypertable_size(format('%I.%I', h.hypertable_schema, h.hypertable_name))) as total_size,
                    pg_size_pretty(hypertable_detailed_size(format('%I.%I', h.hypertable_schema, h.hypertable_name))) as detailed_size
                FROM timescaledb_information.hypertables h
                WHERE h.hypertable_name = $1
                """,
                table_name
            )
            
            # Get compression stats
            compression_stats = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE is_compressed) as compressed_chunks,
                    COUNT(*) as total_chunks,
                    pg_size_pretty(SUM(compressed_total_bytes)) as compressed_size,
                    pg_size_pretty(SUM(uncompressed_total_bytes)) as uncompressed_size
                FROM timescaledb_information.compressed_chunk_stats
                WHERE hypertable_name = $1
                """,
                table_name
            )
            
            return {
                "table_name": table_name,
                "backend": "timescale",
                "is_hypertable": True,
                "owner": hypertable_info["owner"],
                "num_dimensions": hypertable_info["num_dimensions"],
                "num_chunks": hypertable_info["num_chunks"],
                "compression_enabled": hypertable_info["compression_enabled"],
                "total_size": hypertable_info["total_size"],
                "detailed_size": hypertable_info["detailed_size"],
                "compressed_chunks": compression_stats["compressed_chunks"] if compression_stats else 0,
                "total_chunks": compression_stats["total_chunks"] if compression_stats else 0,
                "compressed_size": compression_stats["compressed_size"],
                "uncompressed_size": compression_stats["uncompressed_size"],
            }
    
    async def apply_retention_policy(
        self,
        table_name: str,
        retention_period: timedelta,
        **kwargs
    ) -> Dict[str, Any]:
        """Apply retention policy to TimescaleDB hypertable."""
        async with self._get_connection() as pool:
            async with pool.acquire() as conn:
                # Check if it's a hypertable
                is_hypertable = await conn.fetchval(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM timescaledb_information.hypertables
                        WHERE hypertable_name = $1
                    )
                    """,
                    table_name
                )
                
                if not is_hypertable:
                    raise ValueError(f"{table_name} is not a TimescaleDB hypertable")
                
                # Add retention policy
                await conn.execute(
                    """
                    SELECT add_retention_policy($1, $2::interval, if_not_exists => true)
                    """,
                    table_name,
                    retention_period
                )
                
                # Run retention job immediately
                job_id = await conn.fetchval(
                    """
                    SELECT job_id FROM timescaledb_information.jobs
                    WHERE proc_name = 'policy_retention'
                    AND hypertable_name = $1
                    """,
                    table_name
                )
                
                if job_id:
                    await conn.execute(
                        "CALL run_job($1)",
                        job_id
                    )
                
                return {
                    "status": "success",
                    "operation": "retention",
                    "table": table_name,
                    "retention_period": str(retention_period),
                    "timestamp": datetime.utcnow().isoformat(),
                }
    
    async def vacuum_table(
        self,
        table_name: str,
        full: bool = False,
        analyze: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """Vacuum TimescaleDB table or hypertable."""
        async with self._get_connection() as pool:
            async with pool.acquire() as conn:
                # Build VACUUM command
                vacuum_cmd = "VACUUM"
                if full:
                    vacuum_cmd += " FULL"
                if analyze:
                    vacuum_cmd += " ANALYZE"
                vacuum_cmd += f" {table_name}"
                
                # Execute VACUUM
                await conn.execute(vacuum_cmd)
                
                # Get updated stats
                stats = await conn.fetchrow(
                    """
                    SELECT
                        n_live_tup as live_rows,
                        n_dead_tup as dead_rows,
                        last_vacuum,
                        last_analyze
                    FROM pg_stat_user_tables
                    WHERE relname = $1
                    """,
                    table_name
                )
                
                return {
                    "status": "success",
                    "operation": "vacuum",
                    "table": table_name,
                    "full": full,
                    "analyze": analyze,
                    "live_rows": stats["live_rows"] if stats else 0,
                    "dead_rows": stats["dead_rows"] if stats else 0,
                    "last_vacuum": stats["last_vacuum"].isoformat() if stats and stats["last_vacuum"] else None,
                    "last_analyze": stats["last_analyze"].isoformat() if stats and stats["last_analyze"] else None,
                    "timestamp": datetime.utcnow().isoformat(),
                }
    
    async def compress_chunks(
        self,
        table_name: str,
        older_than: Optional[timedelta] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Compress chunks in TimescaleDB hypertable."""
        async with self._get_connection() as pool:
            async with pool.acquire() as conn:
                # Check if compression is enabled
                compression_enabled = await conn.fetchval(
                    """
                    SELECT compression_enabled
                    FROM timescaledb_information.hypertables
                    WHERE hypertable_name = $1
                    """,
                    table_name
                )
                
                if not compression_enabled:
                    # Enable compression first
                    await conn.execute(
                        "ALTER TABLE %s SET (timescaledb.compress)",
                        table_name
                    )
                
                # Compress chunks
                if older_than:
                    compress_after = datetime.utcnow() - older_than
                    result = await conn.fetch(
                        """
                        SELECT compress_chunk(c.chunk_schema||'.'||c.chunk_name)
                        FROM timescaledb_information.chunks c
                        WHERE c.hypertable_name = $1
                        AND c.range_end < $2
                        AND NOT c.is_compressed
                        """,
                        table_name,
                        compress_after
                    )
                else:
                    # Compress all uncompressed chunks
                    result = await conn.fetch(
                        """
                        SELECT compress_chunk(c.chunk_schema||'.'||c.chunk_name)
                        FROM timescaledb_information.chunks c
                        WHERE c.hypertable_name = $1
                        AND NOT c.is_compressed
                        """,
                        table_name
                    )
                
                chunks_compressed = len(result)
                
                # Get compression stats
                stats = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE is_compressed) as compressed_chunks,
                        COUNT(*) as total_chunks,
                        pg_size_pretty(SUM(compressed_total_bytes)) as compressed_size,
                        pg_size_pretty(SUM(uncompressed_total_bytes)) as uncompressed_size
                    FROM timescaledb_information.compressed_chunk_stats
                    WHERE hypertable_name = $1
                    """,
                    table_name
                )
                
                return {
                    "status": "success",
                    "operation": "compression",
                    "table": table_name,
                    "chunks_compressed": chunks_compressed,
                    "total_compressed": stats["compressed_chunks"] if stats else 0,
                    "total_chunks": stats["total_chunks"] if stats else 0,
                    "compressed_size": stats["compressed_size"],
                    "uncompressed_size": stats["uncompressed_size"],
                    "timestamp": datetime.utcnow().isoformat(),
                }
    
    async def reorder_chunks(
        self,
        table_name: str,
        index_name: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Reorder chunks in TimescaleDB hypertable by index."""
        async with self._get_connection() as pool:
            async with pool.acquire() as conn:
                # Get chunks to reorder
                chunks = await conn.fetch(
                    """
                    SELECT chunk_schema||'.'||chunk_name as chunk_full_name
                    FROM timescaledb_information.chunks
                    WHERE hypertable_name = $1
                    AND NOT is_compressed
                    ORDER BY range_start DESC
                    LIMIT 10  -- Reorder only recent chunks
                    """,
                    table_name
                )
                
                reordered_count = 0
                for chunk in chunks:
                    try:
                        await conn.execute(
                            f"CLUSTER {chunk['chunk_full_name']} USING {index_name}"
                        )
                        reordered_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to reorder chunk {chunk['chunk_full_name']}: {e}")
                
                return {
                    "status": "success",
                    "operation": "reorder",
                    "table": table_name,
                    "index": index_name,
                    "chunks_reordered": reordered_count,
                    "timestamp": datetime.utcnow().isoformat(),
                }