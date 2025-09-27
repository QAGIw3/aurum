"""Catalog router for data discovery and metadata."""
from __future__ import annotations

from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from libs.storage import TrinoAnalyticRepo
from ..main import get_trino_repo

router = APIRouter()


@router.get("/tables")
async def list_tables(
    request: Request,
    response: Response,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    repo: TrinoAnalyticRepo = Depends(get_trino_repo),
) -> dict:
    """List available tables in the data catalog."""
    
    try:
        # Build query to list tables
        catalog_filter = f"table_catalog = '{catalog}'" if catalog else "1=1"
        schema_filter = f"table_schema = '{schema}'" if schema else "1=1"
        
        query = f"""
        SELECT 
            table_catalog,
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables 
        WHERE {catalog_filter} AND {schema_filter}
        ORDER BY table_catalog, table_schema, table_name
        """
        
        tables = await repo.execute_query(query)
        
        result = {
            "tables": tables,
            "count": len(tables),
        }
        
        # Cache for 1 hour
        response.headers["Cache-Control"] = "max-age=3600"
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {str(e)}")


@router.get("/tables/{table_name}/stats")
async def get_table_stats(
    table_name: str,
    request: Request,
    response: Response,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    repo: TrinoAnalyticRepo = Depends(get_trino_repo),
) -> dict:
    """Get detailed statistics for a table."""
    
    try:
        stats = await repo.get_table_stats(
            table_name=table_name,
            catalog=catalog,
            schema=schema,
        )
        
        # Generate ETag for caching
        import hashlib
        etag_content = f"{table_name}-{catalog}-{schema}-stats"
        etag = hashlib.md5(etag_content.encode()).hexdigest()
        
        if request.headers.get("if-none-match") == f'"{etag}"':
            return Response(status_code=304)
        
        response.headers["ETag"] = f'"{etag}"'
        response.headers["Cache-Control"] = "max-age=7200"  # 2 hours
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get table stats: {str(e)}")


@router.get("/dimensions")
async def get_dimensions(
    request: Request,
    response: Response,
    table: Optional[str] = "iso_lmp_unified",
    repo: TrinoAnalyticRepo = Depends(get_trino_repo),
) -> dict:
    """Get available dimensions for filtering."""
    
    try:
        # Get distinct values for common dimensions
        query = f"""
        SELECT 
            'iso_code' as dimension,
            iso_code as value,
            COUNT(*) as count
        FROM iceberg.market.{table}
        WHERE iso_code IS NOT NULL
        GROUP BY iso_code
        
        UNION ALL
        
        SELECT 
            'market_type' as dimension,
            market_type as value,
            COUNT(*) as count
        FROM iceberg.market.{table}
        WHERE market_type IS NOT NULL
        GROUP BY market_type
        
        UNION ALL
        
        SELECT 
            'location' as dimension,
            location as value,
            COUNT(*) as count
        FROM iceberg.market.{table}
        WHERE location IS NOT NULL
        GROUP BY location
        ORDER BY dimension, count DESC
        """
        
        results = await repo.execute_query(query)
        
        # Group by dimension
        dimensions = {}
        for row in results:
            dim = row['dimension']
            if dim not in dimensions:
                dimensions[dim] = []
            dimensions[dim].append({
                'value': row['value'],
                'count': row['count']
            })
        
        result = {
            "dimensions": dimensions,
            "table": table,
        }
        
        # Cache for 4 hours
        response.headers["Cache-Control"] = "max-age=14400"
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dimensions: {str(e)}")


@router.post("/query")
async def execute_analytical_query(
    request: Request,
    query_request: dict,
    repo: TrinoAnalyticRepo = Depends(get_trino_repo),
) -> dict:
    """Execute analytical query with safety checks."""
    
    query = query_request.get("query", "").strip()
    parameters = query_request.get("parameters", {})
    
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")
    
    # Safety checks - only allow SELECT statements
    if not query.upper().startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
    
    # Block dangerous keywords
    dangerous_keywords = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
    query_upper = query.upper()
    for keyword in dangerous_keywords:
        if keyword in query_upper:
            raise HTTPException(status_code=400, detail=f"Keyword '{keyword}' is not allowed")
    
    try:
        results = await repo.execute_query(query, parameters)
        
        return {
            "results": results,
            "count": len(results),
            "query": query,
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")