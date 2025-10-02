"""V2 Metadata API routes using modern patterns."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, Depends, HTTPException, Query, status
except ImportError:
    APIRouter = None  # type: ignore
    Depends = None  # type: ignore
    HTTPException = None  # type: ignore
    Query = None  # type: ignore
    status = None  # type: ignore

from aurum.services.core import MetadataService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError
from aurum.core.container import get_service

logger = logging.getLogger(__name__)


def get_metadata_service() -> MetadataService:
    """Dependency provider for MetadataService."""
    import asyncio
    return asyncio.run(get_service(MetadataService))


router = APIRouter(prefix="/v2/metadata", tags=["metadata-v2"])


@router.get("/dimensions/{dataset}/{dimension}")
async def get_dimensions(
    dataset: str,
    dimension: str,
    use_cache: bool = Query(True, description="Use caching"),
    service: MetadataService = Depends(get_metadata_service)
) -> Dict[str, Any]:
    """Get unique values for a dimension with caching.
    
    Example:
        GET /v2/metadata/dimensions/curves/iso
    """
    try:
        context = ServiceContext()
        
        result = await service.get_dimensions(
            dataset=dataset,
            dimension=dimension,
            use_cache=use_cache,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting dimensions: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get dimensions"
        )


@router.get("/dimensions/{dataset}")
async def get_all_dimensions(
    dataset: str,
    service: MetadataService = Depends(get_metadata_service)
) -> Dict[str, Any]:
    """Get all dimensions for a dataset.
    
    Example:
        GET /v2/metadata/dimensions/curves
    """
    try:
        context = ServiceContext()
        
        result = await service.get_all_dimensions(
            dataset=dataset,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting all dimensions: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get dimensions"
        )


@router.get("/search")
async def search_metadata(
    q: str = Query(..., description="Search term", min_length=2),
    datasets: Optional[List[str]] = Query(None, description="Datasets to search"),
    limit: int = Query(100, ge=1, le=1000),
    service: MetadataService = Depends(get_metadata_service)
) -> Dict[str, Any]:
    """Search metadata across datasets.
    
    Example:
        GET /v2/metadata/search?q=power&limit=50
    """
    try:
        context = ServiceContext()
        
        result = await service.search_metadata(
            search_term=q,
            datasets=datasets,
            limit=limit,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error searching metadata: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search metadata"
        )


@router.get("/locations/{iso}")
async def list_locations(
    iso: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    use_cache: bool = Query(True),
    service: MetadataService = Depends(get_metadata_service)
) -> Dict[str, Any]:
    """List locations for an ISO.
    
    Example:
        GET /v2/metadata/locations/PJM?limit=100
    """
    try:
        context = ServiceContext()
        
        result = await service.list_locations(
            iso=iso,
            limit=limit,
            offset=offset,
            use_cache=use_cache,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error listing locations: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list locations"
        )


@router.get("/units")
async def list_units(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    use_cache: bool = Query(True),
    service: MetadataService = Depends(get_metadata_service)
) -> Dict[str, Any]:
    """List available units."""
    try:
        context = ServiceContext()
        
        result = await service.list_units(
            limit=limit,
            offset=offset,
            use_cache=use_cache,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except Exception as e:
        logger.error(f"Error listing units: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list units"
        )


@router.get("/calendars")
async def list_calendars(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    use_cache: bool = Query(True),
    service: MetadataService = Depends(get_metadata_service)
) -> Dict[str, Any]:
    """List available calendars."""
    try:
        context = ServiceContext()
        
        result = await service.list_calendars(
            limit=limit,
            offset=offset,
            use_cache=use_cache,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except Exception as e:
        logger.error(f"Error listing calendars: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list calendars"
        )


__all__ = ["router"]

