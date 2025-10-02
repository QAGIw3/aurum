"""ClickHouse maintenance backend implementation."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from .base import BaseMaintenanceBackend
from ..interfaces import OperationType
from aurum.core.settings import get_settings

logger = logging.getLogger(__name__)


class ClickHouseMaintenanceBackend(BaseMaintenanceBackend):
    """ClickHouse maintenance backend for table operations."""
    
    def __init__(self):
        super().__init__()
        self._client: Optional[Client] = None
        self._settings = get_settings()
    
    @property
    def backend_type(self) -> str:
        return "clickhouse"
    
    @property
    def supported_operations(self) -> List[OperationType]:
        return [
            OperationType.COMPACTION,
            OperationType.RETENTION,
            OperationType.PARTITION_DROP,
            OperationType.OPTIMIZE,
        ]
    
    async def _create_connection(self) -> Client:
        """Create connection to ClickHouse."""
        if self._client is None:
            self._client = Client(
                host=self._settings.database.clickhouse_host,
                port=self._settings.database.clickhouse_port,
                user=self._settings.database.clickhouse_user,
                password=self._settings.database.clickhouse_password,
                database=self._settings.database.clickhouse_database,
                settings={
                    'use_numpy': False,
                    'max_execution_time': 300,  # 5 minutes
                }
            )
            logger.info("Created ClickHouse client connection")
        
        return self._client
    
    async def _close_connection(self, connection: Client) -> None:
        """Close ClickHouse connection."""
        if self._client:
            self._client.disconnect()
            self._client = None
            logger.info("Closed ClickHouse connection")
    
    async def _health_check_impl(self, connection: Client) -> bool:
        """Health check for ClickHouse."""
        try:
            result = connection.execute("SELECT 1")[0][0]
            return result == 1
        except Exception as e:
            logger.error(f"ClickHouse health check failed: {e}")
            return False
    
    async def _get_table_metadata_impl(
        self, connection: Client, table_name: str
    ) -> Dict[str, Any]:
        """Get ClickHouse table metadata."""
        try:
            # Get basic table info
            table_info = connection.execute(
                f"""
                SELECT 
                    engine,
                    total_rows,
                    total_bytes,
                    formatReadableSize(total_bytes) as total_size
                FROM system.tables
                WHERE name = '{table_name}'
                AND database = currentDatabase()
                """
            )
            
            if not table_info:
                return {
                    "table_name": table_name,
                    "backend": "clickhouse",
                    "exists": False
                }
            
            info = table_info[0]
            
            # Get partition information
            partitions = connection.execute(
                f"""
                SELECT 
                    partition,
                    name,
                    rows,
                    formatReadableSize(bytes_on_disk) as size_on_disk,
                    modification_time
                FROM system.parts
                WHERE table = '{table_name}'
                AND active
                ORDER BY modification_time DESC
                LIMIT 10
                """
            )
            
            return {
                "table_name": table_name,
                "backend": "clickhouse",
                "exists": True,
                "engine": info[0],
                "total_rows": info[1],
                "total_bytes": info[2],
                "total_size": info[3],
                "partition_count": len(partitions),
                "partitions": [
                    {
                        "partition": p[0],
                        "name": p[1],
                        "rows": p[2],
                        "size": p[3],
                        "modified": p[4].isoformat() if p[4] else None
                    }
                    for p in partitions
                ]
            }
            
        except ClickHouseError as e:
            logger.error(f"Error getting ClickHouse table metadata: {e}")
            return {
                "table_name": table_name,
                "backend": "clickhouse",
                "error": str(e)
            }
    
    async def optimize_table(
        self,
        table_name: str,
        final: bool = False,
        deduplicate: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """Optimize ClickHouse table (merge parts)."""
        async with self._get_connection() as client:
            try:
                # Build OPTIMIZE command
                optimize_cmd = f"OPTIMIZE TABLE {table_name}"
                if final:
                    optimize_cmd += " FINAL"
                if deduplicate:
                    optimize_cmd += " DEDUPLICATE"
                
                # Execute optimization
                client.execute(optimize_cmd)
                
                # Get updated table stats
                stats = client.execute(
                    f"""
                    SELECT
                        count() as part_count,
                        sum(rows) as total_rows,
                        formatReadableSize(sum(bytes_on_disk)) as total_size,
                        max(modification_time) as last_modified
                    FROM system.parts
                    WHERE table = '{table_name}'
                    AND active
                    """
                )[0]
                
                return {
                    "status": "success",
                    "operation": "optimize",
                    "table": table_name,
                    "final": final,
                    "deduplicate": deduplicate,
                    "part_count": stats[0],
                    "total_rows": stats[1],
                    "total_size": stats[2],
                    "last_modified": stats[3].isoformat() if stats[3] else None,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
            except ClickHouseError as e:
                logger.error(f"Failed to optimize table {table_name}: {e}")
                return {
                    "status": "error",
                    "operation": "optimize",
                    "table": table_name,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
    
    async def drop_old_partitions(
        self,
        table_name: str,
        retention_period: timedelta,
        **kwargs
    ) -> Dict[str, Any]:
        """Drop old partitions based on retention period."""
        async with self._get_connection() as client:
            try:
                # Calculate cutoff date
                cutoff_date = datetime.utcnow() - retention_period
                
                # Get partitions to drop
                partitions_to_drop = client.execute(
                    f"""
                    SELECT DISTINCT 
                        partition,
                        min(min_time) as min_time,
                        sum(rows) as total_rows
                    FROM system.parts
                    WHERE table = '{table_name}'
                    AND active
                    AND min_time < '{cutoff_date.strftime('%Y-%m-%d')}'
                    GROUP BY partition
                    ORDER BY min_time
                    """
                )
                
                dropped_partitions = []
                total_rows_dropped = 0
                
                for partition in partitions_to_drop:
                    try:
                        client.execute(
                            f"ALTER TABLE {table_name} DROP PARTITION '{partition[0]}'"
                        )
                        dropped_partitions.append({
                            "partition": partition[0],
                            "min_time": partition[1].isoformat() if partition[1] else None,
                            "rows": partition[2]
                        })
                        total_rows_dropped += partition[2]
                    except Exception as e:
                        logger.warning(f"Failed to drop partition {partition[0]}: {e}")
                
                return {
                    "status": "success",
                    "operation": "partition_drop",
                    "table": table_name,
                    "retention_period": str(retention_period),
                    "cutoff_date": cutoff_date.isoformat(),
                    "partitions_dropped": len(dropped_partitions),
                    "rows_dropped": total_rows_dropped,
                    "dropped_partitions": dropped_partitions,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
            except ClickHouseError as e:
                logger.error(f"Failed to drop partitions for {table_name}: {e}")
                return {
                    "status": "error",
                    "operation": "partition_drop",
                    "table": table_name,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }