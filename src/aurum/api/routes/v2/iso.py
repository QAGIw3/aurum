"""V2 ISO (Independent System Operator) API routes using modern patterns."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, Depends, HTTPException, Query, status
except ImportError:
    APIRouter = None  # type: ignore
    Depends = None  # type: ignore
    HTTPException = None  # type: ignore
    Query = None  # type: ignore
    status = None  # type: ignore

from aurum.services.core import IsoService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError
from aurum.core.container import get_service

logger = logging.getLogger(__name__)


def get_iso_service() -> IsoService:
    """Dependency provider for IsoService."""
    import asyncio
    return asyncio.run(get_service(IsoService))


router = APIRouter(prefix="/v2/iso", tags=["iso-v2"])


@router.get("/lmp")
async def get_lmp_data(
    iso: str = Query(..., description="ISO identifier (PJM, ERCOT, CAISO, etc.)"),
    node: Optional[str] = Query(None, description="Node/location identifier"),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    market_type: Optional[str] = Query(None, description="Market type (DA, RT, RUC)"),
    limit: int = Query(100, ge=1, le=10000),
    service: IsoService = Depends(get_iso_service)
) -> Dict[str, Any]:
    """Get ISO LMP (Locational Marginal Pricing) data.
    
    Example:
        GET /v2/iso/lmp?iso=PJM&market_type=DA&limit=1000
    """
    try:
        context = ServiceContext()
        
        result = await service.get_lmp_data(
            iso=iso,
            node=node,
            start_date=start_date,
            end_date=end_date,
            market_type=market_type,
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
    except NotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting LMP data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get LMP data"
        )


@router.get("/{iso}/nodes")
async def list_iso_nodes(
    iso: str,
    limit: int = Query(100, ge=1, le=1000),
    service: IsoService = Depends(get_iso_service)
) -> Dict[str, Any]:
    """List nodes/locations for an ISO.
    
    Example:
        GET /v2/iso/PJM/nodes?limit=100
    """
    try:
        context = ServiceContext()
        
        result = await service.list_iso_nodes(
            iso=iso,
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
    except NotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error listing ISO nodes: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list ISO nodes"
        )


__all__ = ["router"]

