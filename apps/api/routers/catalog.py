"""Catalog router for data discovery and metadata."""
from __future__ import annotations

from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from aurum.api.http.responses import respond_with_etag

from aurum.libs.services.catalog_service import CatalogService

router = APIRouter()


@router.get("/series")
async def list_catalog_series(
    request: Request,
    response: Response,
    tenant_id: str,
    provider: Optional[str] = None,
    dataset_code: Optional[str] = None,
    status: Optional[str] = None,
    iso_code: Optional[str] = None,
    iso_market: Optional[str] = None,
    iso_product: Optional[str] = None,
    iso_location_type: Optional[str] = None,
    iso_location_name: Optional[str] = None,
    iso_location_id: Optional[str] = None,
    canonical_region_id: Optional[str] = None,
    geography_type: Optional[str] = None,
    category: Optional[str] = None,
    tags: Optional[List[str]] = None,
    limit: int = 50,
    cursor: Optional[str] = None,
) -> dict:
    svc = CatalogService()
    filters: Dict[str, Any] = {
        "provider": provider,
        "dataset_code": dataset_code,
        "status": status,
        "iso_code": iso_code,
        "iso_market": iso_market,
        "iso_product": iso_product,
        "iso_location_type": iso_location_type,
        "iso_location_name": iso_location_name,
        "iso_location_id": iso_location_id,
        "canonical_region_id": canonical_region_id,
        "geography_type": geography_type,
        "category": category,
        "tags": tags,
    }
    offset = 0
    items, has_more = await svc.list_series(
        tenant_id=tenant_id, filters=filters, limit=limit, offset=offset
    )
    return {
        "data": items,
        "meta": {
            "tenant_id": tenant_id,
            "returned_count": len(items),
            "has_more": has_more,
        },
        "links": {
            "self": str(request.url),
        },
    }


@router.get("/tables/{table_name}/stats")
async def get_table_stats(
    table_name: str,
    request: Request,
    response: Response,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> dict:
    """Get detailed statistics for a table."""
    
    try:
        from aurum.libs.storage.trino import TrinoAnalyticRepo
        from aurum.core import get_settings
        repo = TrinoAnalyticRepo(get_settings().database)
        stats = await repo.get_table_stats(table_name=table_name, catalog=catalog, schema=schema)
        
        # Standardized ETag/Cache-Control handling
        return respond_with_etag(
            {"data": stats},
            request,
            response,
            cache_seconds=7200,
        )["data"]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get table stats: {str(e)}")


@router.get("/dimensions")
async def get_dimensions(
    request: Request,
    response: Response,
    table: Optional[str] = "iso_lmp_unified",
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
        
        from aurum.libs.storage.trino import TrinoAnalyticRepo
        from aurum.core.settings import get_settings
        repo = TrinoAnalyticRepo(get_settings().database)
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
        
        # Standard Cache-Control via helper
        return respond_with_etag(
            result,
            request,
            response,
            cache_seconds=14400,
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dimensions: {str(e)}")


@router.post("/query")
async def execute_analytical_query(
    request: Request,
    query_request: dict,
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
        from aurum.libs.storage.trino import TrinoAnalyticRepo
        from aurum.core.settings import get_settings
        repo = TrinoAnalyticRepo(get_settings().database)
        results = await repo.execute_query(query)
        
        return {
            "results": results,
            "count": len(results),
            "query": query,
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")