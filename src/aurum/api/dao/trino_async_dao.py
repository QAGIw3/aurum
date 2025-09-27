"""Async Trino DAO for federated query operations with connection pooling."""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Optional, Tuple

from .base_async_dao import BaseAsyncDao

LOGGER = logging.getLogger(__name__)


class TrinoAsyncDao(BaseAsyncDao):
    """Async Data Access Object for Trino federated queries with connection pooling."""
    
    @property
    def dao_name(self) -> str:
        return "trino"
    
    async def execute_federated_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        catalogs: Optional[List[str]] = None,
        schemas: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a federated query across multiple catalogs/schemas.
        
        Args:
            query: Trino SQL query
            params: Query parameters
            catalogs: List of catalogs to include (for validation)
            schemas: List of schemas to include (for validation)
            
        Returns:
            Query results as list of dictionaries
        """
        try:
            # Add federated query metadata for observability
            backend = await self._get_backend()
            if backend.name != "trino":
                raise ValueError(f"Expected Trino backend, got {backend.name}")
            
            result = await self.execute_query(query, params)
            
            LOGGER.info(
                "Federated query executed successfully",
                extra={
                    "query_hash": hashlib.md5(query.encode()).hexdigest()[:8],
                    "catalogs": catalogs,
                    "schemas": schemas,
                    "rows_returned": len(result),
                    "backend": backend.name
                }
            )
            
            return result
            
        except Exception as e:
            LOGGER.error(
                "Federated query failed",
                extra={
                    "query_hash": hashlib.md5(query.encode()).hexdigest()[:8],
                    "catalogs": catalogs,
                    "schemas": schemas,
                    "error": str(e)
                }
            )
            raise
    
    async def query_iceberg_tables(
        self,
        catalog: str = "iceberg",
        schema: Optional[str] = None,
        table_pattern: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Query Iceberg tables metadata.
        
        Args:
            catalog: Iceberg catalog name
            schema: Schema to filter by
            table_pattern: SQL LIKE pattern for table names
            limit: Maximum number of results
            offset: Offset for pagination
            
        Returns:
            List of table metadata
        """
        query = f"SELECT table_catalog, table_schema, table_name, table_type FROM {catalog}.information_schema.tables WHERE 1=1"
        params: Dict[str, Any] = {}
        
        if schema:
            query += " AND table_schema = %(schema)s"
            params["schema"] = schema
            
        if table_pattern:
            query += " AND table_name LIKE %(table_pattern)s"
            params["table_pattern"] = table_pattern
        
        query += " ORDER BY table_schema, table_name LIMIT %(limit)s OFFSET %(offset)s"
        params["limit"] = limit
        params["offset"] = offset
        
        return await self.execute_query(query, params)
    
    async def query_catalog_schemas(
        self,
        catalog: str,
        schema_pattern: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query available schemas in a catalog.
        
        Args:
            catalog: Catalog name
            schema_pattern: SQL LIKE pattern for schema names
            
        Returns:
            List of schema information
        """
        query = f"SELECT schema_name FROM {catalog}.information_schema.schemata WHERE 1=1"
        params: Dict[str, Any] = {}
        
        if schema_pattern:
            query += " AND schema_name LIKE %(schema_pattern)s"
            params["schema_pattern"] = schema_pattern
        
        query += " ORDER BY schema_name"
        
        return await self.execute_query(query, params)
    
    async def get_table_statistics(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> Dict[str, Any]:
        """Get table statistics for query optimization.
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            
        Returns:
            Table statistics dictionary
        """
        # Query basic table info
        table_info_query = f"""
        SELECT 
            table_catalog,
            table_schema,
            table_name,
            table_type
        FROM {catalog}.information_schema.tables 
        WHERE table_schema = %(schema)s AND table_name = %(table)s
        """
        
        # Query column info
        columns_query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable
        FROM {catalog}.information_schema.columns 
        WHERE table_schema = %(schema)s AND table_name = %(table)s
        ORDER BY ordinal_position
        """
        
        params = {"schema": schema, "table": table}
        
        table_info = await self.execute_query(table_info_query, params)
        columns_info = await self.execute_query(columns_query, params)
        
        return {
            "table_info": table_info[0] if table_info else None,
            "columns": columns_info,
            "column_count": len(columns_info)
        }
    
    async def validate_query_syntax(self, query: str) -> Dict[str, Any]:
        """Validate Trino query syntax without executing.
        
        Args:
            query: SQL query to validate
            
        Returns:
            Validation result with plan information
        """
        explain_query = f"EXPLAIN (TYPE VALIDATE) {query}"
        
        try:
            result = await self.execute_query(explain_query)
            return {
                "valid": True,
                "plan": result
            }
        except Exception as e:
            return {
                "valid": False,
                "error": str(e)
            }