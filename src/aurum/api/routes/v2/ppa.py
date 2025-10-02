"""V2 PPA (Power Purchase Agreement) API routes using modern patterns."""

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

from aurum.services.core import PpaService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError
from aurum.core.container import get_service

logger = logging.getLogger(__name__)


def get_ppa_service() -> PpaService:
    """Dependency provider for PpaService."""
    import asyncio
    return asyncio.run(get_service(PpaService))


router = APIRouter(prefix="/v2/ppa", tags=["ppa-v2"])


@router.get("/contracts")
async def list_contracts(
    contract_ids: Optional[List[str]] = Query(None),
    counterparty: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    use_cache: bool = Query(True),
    service: PpaService = Depends(get_ppa_service)
) -> Dict[str, Any]:
    """List PPA contracts with optional filtering and caching.
    
    Example:
        GET /v2/ppa/contracts?counterparty=AcmePower&limit=50
    """
    try:
        context = ServiceContext()
        
        result = await service.get_ppa_contracts(
            contract_ids=contract_ids,
            counterparty=counterparty,
            limit=limit,
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
        logger.error(f"Error listing contracts: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list contracts"
        )


@router.get("/valuations")
async def get_valuations(
    contract_id: Optional[str] = Query(None),
    asof_date: Optional[date] = Query(None),
    valuation_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    use_cache: bool = Query(True),
    service: PpaService = Depends(get_ppa_service)
) -> Dict[str, Any]:
    """Get PPA valuations with optional filtering and caching.
    
    Example:
        GET /v2/ppa/valuations?contract_id=C001&asof_date=2025-01-01
    """
    try:
        context = ServiceContext()
        
        result = await service.get_ppa_valuations(
            contract_id=contract_id,
            asof_date=asof_date,
            valuation_type=valuation_type,
            limit=limit,
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
    except NotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting valuations: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get valuations"
        )


@router.get("/contracts/{contract_id}/risk")
async def get_contract_risk(
    contract_id: str,
    asof_date: Optional[date] = Query(None),
    risk_metrics: Optional[List[str]] = Query(None),
    service: PpaService = Depends(get_ppa_service)
) -> Dict[str, Any]:
    """Get risk metrics for a specific contract.
    
    Example:
        GET /v2/ppa/contracts/C001/risk?asof_date=2025-01-01
    """
    try:
        context = ServiceContext()
        
        result = await service.get_contract_risk_metrics(
            contract_id=contract_id,
            asof_date=asof_date,
            risk_metrics=risk_metrics,
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
        logger.error(f"Error getting contract risk: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get contract risk"
        )


__all__ = ["router"]

