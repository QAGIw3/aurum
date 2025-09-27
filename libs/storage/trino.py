"""Trino analytical repository implementation for read-heavy queries."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from libs.common.config import DatabaseSettings
from .ports import AnalyticRepository

logger = logging.getLogger(__name__)


class TrinoAnalyticRepo(AnalyticRepository):
    """Trino implementation of AnalyticRepository for read-heavy analytical queries."""
    
    def __init__(self, db_settings: DatabaseSettings):
        self.db_settings = db_settings
        self._client: Optional[httpx.AsyncClient] = None
        
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client for Trino REST API."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=f"http://{self.db_settings.trino_host}:{self.db_settings.trino_port}",
                headers={
                    "X-Trino-User": self.db_settings.trino_user,
                    "X-Trino-Catalog": self.db_settings.trino_catalog,
                    "X-Trino-Schema": self.db_settings.trino_schema,
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
        return self._client
    
    async def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Execute analytical query using Trino REST API."""
        client = await self._get_client()
        
        try:
            # Start query execution
            response = await client.post("/v1/statement", json={"query": query})
            response.raise_for_status()
            
            query_info = response.json()
            next_uri = query_info.get("nextUri")
            
            results = []
            columns = None
            
            # Poll for results
            while next_uri:
                response = await client.get(next_uri)
                response.raise_for_status()
                
                query_info = response.json()
                
                # Get column information from first result
                if columns is None and "columns" in query_info:
                    columns = [col["name"] for col in query_info["columns"]]
                
                # Process data rows
                if "data" in query_info:
                    for row in query_info["data"]:
                        if columns:
                            row_dict = dict(zip(columns, row))
                            results.append(row_dict)
                
                next_uri = query_info.get("nextUri")
                
                # Check for query completion or error
                if query_info.get("stats", {}).get("state") == "FAILED":
                    error = query_info.get("error", {})
                    raise RuntimeError(f"Trino query failed: {error.get('message', 'Unknown error')}")
            
            return results
            
        except httpx.HTTPError as e:
            logger.error(f"Trino HTTP error: {e}")
            raise RuntimeError(f"Failed to execute Trino query: {e}")
        except Exception as e:
            logger.error(f"Trino query error: {e}")
            raise
    
    async def get_table_stats(
        self,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get table statistics for query optimization."""
        catalog = catalog or self.db_settings.trino_catalog
        schema = schema or self.db_settings.trino_schema
        
        query = f"""
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        columns_info = await self.execute_query(query)
        
        # Get row count and basic statistics
        stats_query = f"""
        SELECT 
            COUNT(*) as row_count,
            approx_distinct(1) as approx_row_count
        FROM {catalog}.{schema}.{table_name}
        """
        
        try:
            stats_result = await self.execute_query(stats_query)
            stats = stats_result[0] if stats_result else {}
        except Exception as e:
            logger.warning(f"Failed to get table stats for {table_name}: {e}")
            stats = {}
        
        return {
            "table_name": table_name,
            "catalog": catalog,
            "schema": schema,
            "columns": columns_info,
            "statistics": stats,
        }
    
    async def get_aggregated_data(
        self,
        table: str,
        groupby_columns: List[str],
        aggregations: Dict[str, str],
        filters: Optional[Dict[str, Any]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Get pre-aggregated data with time filters."""
        # Build SELECT clause with aggregations
        select_parts = groupby_columns.copy()
        for agg_name, agg_expr in aggregations.items():
            select_parts.append(f"{agg_expr} as {agg_name}")
        
        select_clause = ", ".join(select_parts)
        
        # Build WHERE clause
        where_conditions = []
        
        if start_time:
            where_conditions.append(f"delivery_date >= DATE '{start_time.date()}'")
        
        if end_time:
            where_conditions.append(f"delivery_date <= DATE '{end_time.date()}'")
        
        if filters:
            for col, value in filters.items():
                if isinstance(value, str):
                    where_conditions.append(f"{col} = '{value}'")
                elif isinstance(value, (int, float)):
                    where_conditions.append(f"{col} = {value}")
                elif isinstance(value, list):
                    quoted_values = [f"'{v}'" if isinstance(v, str) else str(v) for v in value]
                    where_conditions.append(f"{col} IN ({', '.join(quoted_values)})")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Build GROUP BY clause
        groupby_clause = ", ".join(groupby_columns) if groupby_columns else ""
        
        # Construct full query
        query = f"""
        SELECT {select_clause}
        FROM {self.db_settings.trino_catalog}.{self.db_settings.trino_schema}.{table}
        WHERE {where_clause}
        """
        
        if groupby_clause:
            query += f" GROUP BY {groupby_clause}"
        
        query += " ORDER BY " + ", ".join(groupby_columns) if groupby_columns else " LIMIT 1000"
        
        return await self.execute_query(query)
    
    async def get_time_series_rollup(
        self,
        table: str,
        time_column: str = "delivery_date",
        value_column: str = "price",
        groupby_columns: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        time_bucket: str = "day",
        aggregation: str = "avg",
    ) -> List[Dict[str, Any]]:
        """Get time series data with time bucketing and aggregation."""
        groupby_columns = groupby_columns or []
        
        # Time bucketing expression
        if time_bucket == "hour":
            time_expr = f"date_trunc('hour', {time_column})"
        elif time_bucket == "day":
            time_expr = f"date_trunc('day', {time_column})"
        elif time_bucket == "week":
            time_expr = f"date_trunc('week', {time_column})"
        elif time_bucket == "month":
            time_expr = f"date_trunc('month', {time_column})"
        else:
            time_expr = time_column
        
        # Aggregation expression
        agg_expr = f"{aggregation}({value_column})"
        
        # Build query
        select_parts = [f"{time_expr} as time_bucket"] + groupby_columns + [f"{agg_expr} as {aggregation}_{value_column}"]
        select_clause = ", ".join(select_parts)
        
        where_conditions = []
        if start_time:
            where_conditions.append(f"{time_column} >= TIMESTAMP '{start_time.isoformat()}'")
        if end_time:
            where_conditions.append(f"{time_column} <= TIMESTAMP '{end_time.isoformat()}'")
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        groupby_clause = ", ".join([time_expr] + groupby_columns)
        
        query = f"""
        SELECT {select_clause}
        FROM {self.db_settings.trino_catalog}.{self.db_settings.trino_schema}.{table}
        WHERE {where_clause}
        GROUP BY {groupby_clause}
        ORDER BY time_bucket, {', '.join(groupby_columns) if groupby_columns else '1'}
        """
        
        return await self.execute_query(query)
    
    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None