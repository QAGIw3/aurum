"""Async TimescaleDB DAO for time-series operations with connection pooling."""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from .base_async_dao import BaseAsyncDao

LOGGER = logging.getLogger(__name__)


class TimescaleAsyncDao(BaseAsyncDao):
    """Async Data Access Object for TimescaleDB time-series queries with connection pooling."""
    
    @property
    def dao_name(self) -> str:
        return "timescale"
    
    async def execute_time_series_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        enable_compression: bool = True
    ) -> List[Dict[str, Any]]:
        """Execute a time-series query optimized for TimescaleDB.
        
        Args:
            query: PostgreSQL/TimescaleDB SQL query
            params: Query parameters
            enable_compression: Whether to enable compression-aware optimizations
            
        Returns:
            Query results as list of dictionaries
        """
        try:
            backend = await self._get_backend()
            if backend.name != "timescale":
                raise ValueError(f"Expected TimescaleDB backend, got {backend.name}")
            
            result = await self.execute_query(query, params)
            
            LOGGER.info(
                "Time-series query executed successfully",
                extra={
                    "query_hash": hashlib.md5(query.encode()).hexdigest()[:8],
                    "compression_aware": enable_compression,
                    "rows_returned": len(result),
                    "backend": backend.name
                }
            )
            
            return result
            
        except Exception as e:
            LOGGER.error(
                "Time-series query failed",
                extra={
                    "query_hash": hashlib.md5(query.encode()).hexdigest()[:8],
                    "compression_aware": enable_compression,
                    "error": str(e)
                }
            )
            raise
    
    async def query_time_bucket_data(
        self,
        table: str,
        timestamp_column: str = "timestamp",
        value_columns: Optional[List[str]] = None,
        bucket_width: str = "1 hour",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        aggregation: str = "avg",
        limit: int = 10000,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Query time-bucketed data using TimescaleDB's time_bucket function.
        
        Args:
            table: Hypertable name
            timestamp_column: Name of timestamp column
            value_columns: List of value columns to aggregate
            bucket_width: Time bucket width (e.g., "1 hour", "1 day", "5 minutes")
            start_time: Start time filter
            end_time: End time filter
            aggregation: Aggregation function (avg, sum, max, min, count, first, last)
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            Time-bucketed data results
        """
        value_columns = value_columns or ["value"]
        
        # Build aggregation expressions
        agg_expressions = []
        for col in value_columns:
            if aggregation in ["first", "last"]:
                # Use TimescaleDB-specific functions
                agg_expressions.append(f"{aggregation}({col}, {timestamp_column}) as {col}_{aggregation}")
            else:
                agg_expressions.append(f"{aggregation}({col}) as {col}_{aggregation}")
        
        query = f"""
        SELECT 
            time_bucket('{bucket_width}', {timestamp_column}) as time_bucket,
            {', '.join(agg_expressions)}
        FROM {table}
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        if start_time:
            query += f" AND {timestamp_column} >= %(start_time)s"
            params["start_time"] = start_time
        
        if end_time:
            query += f" AND {timestamp_column} <= %(end_time)s"
            params["end_time"] = end_time
        
        query += " GROUP BY time_bucket ORDER BY time_bucket"
        query += " LIMIT %(limit)s OFFSET %(offset)s"
        params["limit"] = limit
        params["offset"] = offset
        
        return await self.execute_time_series_query(query, params)
    
    async def query_continuous_aggregate(
        self,
        cagg_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        dimensions: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10000,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Query a TimescaleDB continuous aggregate.
        
        Args:
            cagg_name: Continuous aggregate view name
            start_time: Start time filter
            end_time: End time filter
            dimensions: Additional columns to select
            filters: Additional filters to apply
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            Continuous aggregate results
        """
        dimensions = dimensions or []
        filters = filters or {}
        
        # Build SELECT clause
        select_columns = ["*"] if not dimensions else dimensions
        
        query = f"""
        SELECT {', '.join(select_columns)}
        FROM {cagg_name}
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        if start_time:
            query += " AND time_bucket >= %(start_time)s"
            params["start_time"] = start_time
        
        if end_time:
            query += " AND time_bucket <= %(end_time)s"
            params["end_time"] = end_time
        
        # Add custom filters
        for column, value in filters.items():
            if isinstance(value, list):
                placeholders = ', '.join([f"%(filter_{column}_{i})s" for i in range(len(value))])
                query += f" AND {column} = ANY(ARRAY[{placeholders}])"
                for i, v in enumerate(value):
                    params[f"filter_{column}_{i}"] = v
            else:
                query += f" AND {column} = %(filter_{column})s"
                params[f"filter_{column}"] = value
        
        query += " ORDER BY time_bucket"
        query += " LIMIT %(limit)s OFFSET %(offset)s"
        params["limit"] = limit
        params["offset"] = offset
        
        return await self.execute_time_series_query(query, params)
    
    async def get_hypertable_info(self, table: str) -> Dict[str, Any]:
        """Get information about a TimescaleDB hypertable.
        
        Args:
            table: Hypertable name
            
        Returns:
            Hypertable information
        """
        query = """
        SELECT 
            hypertable_schema,
            hypertable_name,
            owner,
            num_dimensions,
            num_chunks,
            compression_enabled,
            tablespace
        FROM timescaledb_information.hypertables 
        WHERE hypertable_name = %(table)s
        """
        
        result = await self.execute_query(query, {"table": table})
        return result[0] if result else {}
    
    async def get_chunk_info(
        self,
        table: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get chunk information for a hypertable.
        
        Args:
            table: Hypertable name
            start_time: Filter chunks containing data after this time
            end_time: Filter chunks containing data before this time
            
        Returns:
            Chunk information
        """
        query = """
        SELECT 
            chunk_schema,
            chunk_name,
            primary_dimension,
            primary_dimension_type,
            range_start,
            range_end,
            is_compressed,
            chunk_tablespace,
            data_nodes
        FROM timescaledb_information.chunks 
        WHERE hypertable_name = %(table)s
        """
        
        params: Dict[str, Any] = {"table": table}
        
        if start_time:
            query += " AND range_end > %(start_time)s"
            params["start_time"] = start_time
        
        if end_time:
            query += " AND range_start < %(end_time)s"
            params["end_time"] = end_time
        
        query += " ORDER BY range_start"
        
        return await self.execute_query(query, params)
    
    async def compress_chunks(
        self,
        table: str,
        older_than: Optional[timedelta] = None
    ) -> Dict[str, Any]:
        """Compress chunks older than specified time.
        
        Args:
            table: Hypertable name
            older_than: Compress chunks older than this duration (default: 7 days)
            
        Returns:
            Compression result
        """
        older_than = older_than or timedelta(days=7)
        
        query = f"""
        SELECT compress_chunk(chunk_name) 
        FROM timescaledb_information.chunks 
        WHERE hypertable_name = %(table)s 
        AND NOT is_compressed 
        AND range_end < NOW() - INTERVAL '{older_than.total_seconds()} seconds'
        """
        
        try:
            result = await self.execute_query(query, {"table": table})
            return {
                "success": True,
                "table": table,
                "chunks_compressed": len(result)
            }
        except Exception as e:
            return {
                "success": False,
                "table": table,
                "error": str(e)
            }
    
    async def create_continuous_aggregate(
        self,
        cagg_name: str,
        table: str,
        time_column: str,
        bucket_width: str,
        select_clause: str,
        group_by_clause: Optional[str] = None,
        with_no_data: bool = True
    ) -> Dict[str, Any]:
        """Create a continuous aggregate view.
        
        Args:
            cagg_name: Name for the continuous aggregate
            table: Source hypertable name
            time_column: Time column name
            bucket_width: Time bucket width
            select_clause: SELECT clause for aggregation
            group_by_clause: Additional GROUP BY columns
            with_no_data: Whether to create with no data initially
            
        Returns:
            Creation result
        """
        group_by = f"time_bucket('{bucket_width}', {time_column})"
        if group_by_clause:
            group_by += f", {group_by_clause}"
        
        no_data_clause = "WITH NO DATA" if with_no_data else ""
        
        query = f"""
        CREATE MATERIALIZED VIEW {cagg_name} {no_data_clause}
        AS SELECT 
            time_bucket('{bucket_width}', {time_column}) AS time_bucket,
            {select_clause}
        FROM {table}
        GROUP BY {group_by}
        """
        
        try:
            await self.execute_query(query)
            return {
                "success": True,
                "cagg_name": cagg_name,
                "table": table
            }
        except Exception as e:
            return {
                "success": False,
                "cagg_name": cagg_name,
                "table": table,
                "error": str(e)
            }