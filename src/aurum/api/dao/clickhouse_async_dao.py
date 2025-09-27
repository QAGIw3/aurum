"""Async ClickHouse DAO for OLAP operations with connection pooling."""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date

from .base_async_dao import BaseAsyncDao

LOGGER = logging.getLogger(__name__)


class ClickHouseAsyncDao(BaseAsyncDao):
    """Async Data Access Object for ClickHouse OLAP queries with connection pooling."""
    
    @property
    def dao_name(self) -> str:
        return "clickhouse"
    
    async def execute_analytical_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        optimize_for: str = "speed"
    ) -> List[Dict[str, Any]]:
        """Execute an analytical query optimized for ClickHouse.
        
        Args:
            query: ClickHouse SQL query
            params: Query parameters
            optimize_for: Optimization strategy ("speed" or "memory")
            
        Returns:
            Query results as list of dictionaries
        """
        try:
            backend = await self._get_backend()
            if backend.name != "clickhouse":
                raise ValueError(f"Expected ClickHouse backend, got {backend.name}")
            
            # Add ClickHouse-specific settings for optimization
            optimized_query = self._add_optimization_settings(query, optimize_for)
            
            result = await self.execute_query(optimized_query, params)
            
            LOGGER.info(
                "Analytical query executed successfully",
                extra={
                    "query_hash": hashlib.md5(query.encode()).hexdigest()[:8],
                    "optimization": optimize_for,
                    "rows_returned": len(result),
                    "backend": backend.name
                }
            )
            
            return result
            
        except Exception as e:
            LOGGER.error(
                "Analytical query failed",
                extra={
                    "query_hash": hashlib.md5(query.encode()).hexdigest()[:8],
                    "optimization": optimize_for,
                    "error": str(e)
                }
            )
            raise
    
    def _add_optimization_settings(self, query: str, optimize_for: str) -> str:
        """Add ClickHouse-specific optimization settings to query."""
        if optimize_for == "memory":
            settings = "SETTINGS max_memory_usage = 1000000000, max_threads = 2"
        else:  # speed
            settings = "SETTINGS max_threads = 0, max_memory_usage = 10000000000"
        
        # Add settings to the end of the query if not already present
        if "SETTINGS" not in query.upper():
            query = f"{query.rstrip(';')} {settings}"
        
        return query
    
    async def query_time_series_data(
        self,
        table: str,
        timestamp_column: str = "timestamp",
        value_columns: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        group_by_interval: Optional[str] = None,
        aggregation: str = "avg",
        limit: int = 10000,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Query time series data from ClickHouse with aggregation.
        
        Args:
            table: Table name
            timestamp_column: Name of timestamp column
            value_columns: List of value columns to aggregate
            start_time: Start time filter
            end_time: End time filter
            group_by_interval: Time interval for grouping (e.g., "1 HOUR", "1 DAY")
            aggregation: Aggregation function (avg, sum, max, min, count)
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            Time series data results
        """
        value_columns = value_columns or ["value"]
        
        # Build aggregation expressions
        agg_expressions = []
        for col in value_columns:
            agg_expressions.append(f"{aggregation}({col}) as {col}_{aggregation}")
        
        if group_by_interval:
            # Use toStartOfInterval for time grouping
            query = f"""
            SELECT 
                toStartOfInterval({timestamp_column}, INTERVAL {group_by_interval}) as time_bucket,
                {', '.join(agg_expressions)}
            FROM {table}
            WHERE 1=1
            """
            group_by_clause = "GROUP BY time_bucket ORDER BY time_bucket"
        else:
            query = f"""
            SELECT 
                {timestamp_column},
                {', '.join(value_columns)}
            FROM {table}
            WHERE 1=1
            """
            group_by_clause = f"ORDER BY {timestamp_column}"
        
        params: Dict[str, Any] = {}
        
        if start_time:
            query += f" AND {timestamp_column} >= %(start_time)s"
            params["start_time"] = start_time
        
        if end_time:
            query += f" AND {timestamp_column} <= %(end_time)s"
            params["end_time"] = end_time
        
        query += f" {group_by_clause} LIMIT %(limit)s OFFSET %(offset)s"
        params["limit"] = limit
        params["offset"] = offset
        
        return await self.execute_analytical_query(query, params)
    
    async def query_aggregated_metrics(
        self,
        table: str,
        metrics: Dict[str, str],  # column_name -> aggregation_function
        dimensions: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Query aggregated metrics with grouping dimensions.
        
        Args:
            table: Table name
            metrics: Dictionary of column_name -> aggregation_function
            dimensions: List of columns to group by
            filters: Dictionary of column_name -> filter_value
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            Aggregated metrics results
        """
        dimensions = dimensions or []
        filters = filters or {}
        
        # Build metric expressions
        metric_expressions = []
        for column, agg_func in metrics.items():
            metric_expressions.append(f"{agg_func}({column}) as {column}_{agg_func}")
        
        # Build SELECT clause
        select_columns = dimensions + metric_expressions
        
        query = f"""
        SELECT {', '.join(select_columns)}
        FROM {table}
        WHERE 1=1
        """
        
        params: Dict[str, Any] = {}
        
        # Add filters
        for column, value in filters.items():
            if isinstance(value, list):
                placeholders = ', '.join([f"%(filter_{column}_{i})s" for i in range(len(value))])
                query += f" AND {column} IN ({placeholders})"
                for i, v in enumerate(value):
                    params[f"filter_{column}_{i}"] = v
            else:
                query += f" AND {column} = %(filter_{column})s"
                params[f"filter_{column}"] = value
        
        # Add GROUP BY if dimensions exist
        if dimensions:
            query += f" GROUP BY {', '.join(dimensions)}"
            query += f" ORDER BY {', '.join(dimensions)}"
        
        query += " LIMIT %(limit)s OFFSET %(offset)s"
        params["limit"] = limit
        params["offset"] = offset
        
        return await self.execute_analytical_query(query, params)
    
    async def get_table_partitions(self, table: str) -> List[Dict[str, Any]]:
        """Get partition information for a ClickHouse table.
        
        Args:
            table: Table name
            
        Returns:
            Partition information
        """
        query = f"""
        SELECT 
            partition,
            name,
            active,
            marks,
            rows,
            bytes_on_disk,
            primary_key_bytes_in_memory,
            modification_time
        FROM system.parts 
        WHERE table = %(table)s AND active = 1
        ORDER BY partition, name
        """
        
        return await self.execute_query(query, {"table": table})
    
    async def optimize_table(self, table: str, partition: Optional[str] = None) -> Dict[str, Any]:
        """Optimize a ClickHouse table by merging parts.
        
        Args:
            table: Table name
            partition: Specific partition to optimize (optional)
            
        Returns:
            Optimization result
        """
        if partition:
            query = f"OPTIMIZE TABLE {table} PARTITION '{partition}'"
        else:
            query = f"OPTIMIZE TABLE {table}"
        
        try:
            await self.execute_query(query)
            return {
                "success": True,
                "table": table,
                "partition": partition
            }
        except Exception as e:
            return {
                "success": False,
                "table": table,
                "partition": partition,
                "error": str(e)
            }