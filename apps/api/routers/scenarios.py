"""Scenarios router for scenario management."""
from __future__ import annotations

from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from libs.storage import PostgresMetaRepo
from ..main import get_postgres_repo

router = APIRouter()


class ScenarioCreate(BaseModel):
    """Scenario creation request."""
    name: str
    description: Optional[str] = None
    parameters: Dict[str, Any] = {}
    created_by: Optional[str] = None


class ScenarioUpdate(BaseModel):
    """Scenario update request."""
    name: Optional[str] = None
    description: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    status: Optional[str] = None


@router.get("/")
async def list_scenarios(
    request: Request,
    response: Response,
    limit: Optional[int] = 100,
    offset: Optional[int] = 0,
    repo: PostgresMetaRepo = Depends(get_postgres_repo),
) -> dict:
    """List scenarios with pagination."""
    
    try:
        scenarios, total_count = await repo.list_scenarios(
            limit=limit,
            offset=offset,
        )
        
        result = {
            "scenarios": scenarios,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "count": len(scenarios),
            }
        }
        
        # Cache for 5 minutes
        response.headers["Cache-Control"] = "max-age=300"
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list scenarios: {str(e)}")


@router.get("/{scenario_id}")
async def get_scenario(
    scenario_id: str,
    request: Request,
    response: Response,
    repo: PostgresMetaRepo = Depends(get_postgres_repo),
) -> dict:
    """Get scenario by ID."""
    
    try:
        scenario = await repo.get_scenario(scenario_id)
        
        if not scenario:
            raise HTTPException(status_code=404, detail="Scenario not found")
        
        # Generate ETag for caching
        import hashlib
        etag_content = f"{scenario_id}-{scenario.get('version', 1)}"
        etag = hashlib.md5(etag_content.encode()).hexdigest()
        
        if request.headers.get("if-none-match") == f'"{etag}"':
            return Response(status_code=304)
        
        response.headers["ETag"] = f'"{etag}"'
        response.headers["Cache-Control"] = "max-age=600"  # 10 minute cache
        
        return scenario
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scenario: {str(e)}")


@router.post("/")
async def create_scenario(
    scenario_data: ScenarioCreate,
    repo: PostgresMetaRepo = Depends(get_postgres_repo),
) -> dict:
    """Create a new scenario."""
    
    try:
        scenario_id = await repo.create_scenario(scenario_data.model_dump())
        
        return {
            "id": scenario_id,
            "message": "Scenario created successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create scenario: {str(e)}")


@router.put("/{scenario_id}")
async def update_scenario(
    scenario_id: str,
    updates: ScenarioUpdate,
    repo: PostgresMetaRepo = Depends(get_postgres_repo),
) -> dict:
    """Update scenario."""
    
    try:
        # Filter out None values
        update_data = {k: v for k, v in updates.model_dump().items() if v is not None}
        
        success = await repo.update_scenario(scenario_id, update_data)
        
        if not success:
            raise HTTPException(status_code=404, detail="Scenario not found")
        
        return {"message": "Scenario updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update scenario: {str(e)}")


@router.get("/{scenario_id}/runs")
async def get_scenario_runs(
    scenario_id: str,
    request: Request,
    response: Response,
    limit: Optional[int] = 50,
    offset: Optional[int] = 0,
    repo: PostgresMetaRepo = Depends(get_postgres_repo),
) -> dict:
    """Get runs for a specific scenario."""
    
    try:
        runs, total_count = await repo.get_scenario_runs(
            scenario_id=scenario_id,
            limit=limit,
            offset=offset,
        )
        
        result = {
            "runs": runs,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "count": len(runs),
            }
        }
        
        # Cache for 2 minutes
        response.headers["Cache-Control"] = "max-age=120"
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scenario runs: {str(e)}")