"""V2 Scenarios API routes using modern patterns."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, Depends, HTTPException, Query, status, Body
    from pydantic import BaseModel
except ImportError:
    APIRouter = None  # type: ignore
    Depends = None  # type: ignore
    HTTPException = None  # type: ignore
    Query = None  # type: ignore
    status = None  # type: ignore
    Body = None  # type: ignore
    BaseModel = None  # type: ignore

from aurum.services.core import ScenarioService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError
from aurum.core.container import get_service

logger = logging.getLogger(__name__)


class CreateScenarioRequest(BaseModel):
    """Request model for creating scenarios."""
    name: str
    description: Optional[str] = None
    assumptions: Optional[Dict[str, Any]] = None


def get_scenario_service() -> ScenarioService:
    """Dependency provider for ScenarioService."""
    import asyncio
    return asyncio.run(get_service(ScenarioService))


router = APIRouter(prefix="/v2/scenarios", tags=["scenarios-v2"])


@router.get("/")
async def list_scenarios(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    service: ScenarioService = Depends(get_scenario_service)
) -> Dict[str, Any]:
    """List scenarios with pagination."""
    try:
        context = ServiceContext()
        
        result = await service.list_scenarios(
            limit=limit,
            offset=offset,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except Exception as e:
        logger.error(f"Error in list_scenarios: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list scenarios"
        )


@router.post("/")
async def create_scenario(
    request: CreateScenarioRequest,
    service: ScenarioService = Depends(get_scenario_service)
) -> Dict[str, Any]:
    """Create a new scenario."""
    try:
        context = ServiceContext()
        
        result = await service.create_scenario(
            name=request.name,
            description=request.description,
            assumptions=request.assumptions,
            context=context
        )
        
        if not result.success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.error
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
        logger.error(f"Error creating scenario: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create scenario"
        )


@router.get("/{scenario_id}")
async def get_scenario(
    scenario_id: str,
    use_cache: bool = Query(True),
    service: ScenarioService = Depends(get_scenario_service)
) -> Dict[str, Any]:
    """Get scenario by ID with optional caching."""
    try:
        context = ServiceContext()
        
        result = await service.get_scenario(
            scenario_id=scenario_id,
            use_cache=use_cache,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except NotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting scenario: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get scenario"
        )


@router.get("/{scenario_id}/outputs")
async def get_scenario_outputs(
    scenario_id: str,
    limit: int = Query(1000, ge=1, le=10000),
    service: ScenarioService = Depends(get_scenario_service)
) -> Dict[str, Any]:
    """Get scenario outputs/results."""
    try:
        context = ServiceContext()
        
        result = await service.get_scenario_outputs(
            scenario_id=scenario_id,
            limit=limit,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except NotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting scenario outputs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get scenario outputs"
        )


__all__ = ["router"]

