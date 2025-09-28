"""v2 Developer Workspace API for notebooks and API exploration.

This module provides REST endpoints for:
- Managing notebook environments and sessions
- Deploying notebook templates and examples
- Interactive API documentation and testing
- Developer environment provisioning and monitoring
- Notebook resource allocation and lifecycle management
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from ..auth import Permission, require_permission
from ..deps import get_principal, get_settings
from ..services.developer_workspace_service import (
    get_developer_workspace_service,
    NotebookEnvironment,
    NotebookSession,
    NotebookTemplate
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/developer-workspace", tags=["developer-workspace"])


class EnvironmentCreateRequest(BaseModel):
    """Request to create a notebook environment."""

    environment_id: str = Field(..., description="Environment identifier")
    environment_name: str = Field(..., description="Environment name")
    description: str = Field(..., description="Environment description")
    base_image: str = Field("jupyter/scipy-notebook:latest", description="Base Docker image")
    resource_limits: Dict[str, str] = Field(default_factory=lambda: {"cpu": "2", "memory": "4Gi"}, description="Resource limits")
    resource_requests: Dict[str, str] = Field(default_factory=lambda: {"cpu": "500m", "memory": "1Gi"}, description="Resource requests")
    storage_size: str = Field("10Gi", description="Storage size")
    environment_variables: Dict[str, str] = Field(default_factory=dict, description="Environment variables")
    allowed_packages: List[str] = Field(default_factory=list, description="Allowed packages")
    network_policy: str = Field("restricted", description="Network policy")


class SessionCreateRequest(BaseModel):
    """Request to create a notebook session."""

    environment_id: str = Field(..., description="Environment to use")
    template_id: Optional[str] = Field(None, description="Template to deploy")
    customizations: Dict[str, any] = Field(default_factory=dict, description="Template customizations")


class EnvironmentResponse(BaseModel):
    """Response containing environment information."""

    environment_id: str
    environment_name: str
    description: str
    base_image: str
    resource_limits: Dict[str, str]
    resource_requests: Dict[str, str]
    storage_size: str
    environment_variables: Dict[str, str]
    allowed_packages: List[str]
    network_policy: str
    created_at: datetime


class SessionResponse(BaseModel):
    """Response containing session information."""

    session_id: str
    environment_id: str
    user_id: str
    tenant_id: str
    status: str
    pod_name: Optional[str]
    pod_ip: Optional[str]
    notebook_url: Optional[str]
    start_time: Optional[datetime]
    last_activity: Optional[datetime]
    resource_usage: Dict[str, any]


class TemplateResponse(BaseModel):
    """Response containing template information."""

    template_id: str
    template_name: str
    description: str
    category: str
    required_packages: List[str]
    sample_queries: List[Dict[str, any]]
    documentation_links: List[str]
    tags: List[str]


class DeveloperGuideResponse(BaseModel):
    """Response containing developer guide."""

    user_id: str
    guide_content: str
    generated_at: datetime


@router.post("/environments", response_model=EnvironmentResponse, status_code=201)
async def create_notebook_environment(
    request: Request,
    environment_data: EnvironmentCreateRequest
) -> EnvironmentResponse:
    """Create a new notebook environment."""
    start_time = time.perf_counter()

    try:
        service = get_developer_workspace_service()

        # Create environment configuration
        environment = NotebookEnvironment(
            environment_id=environment_data.environment_id,
            environment_name=environment_data.environment_name,
            description=environment_data.description,
            base_image=environment_data.base_image,
            resource_limits=environment_data.resource_limits,
            resource_requests=environment_data.resource_requests,
            storage_size=environment_data.storage_size,
            environment_variables=environment_data.environment_variables,
            allowed_packages=environment_data.allowed_packages,
            network_policy=environment_data.network_policy
        )

        # Create environment
        env_id = await service.create_notebook_environment(environment)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="create_notebook_environment",
            query_time_ms=query_time_ms
        )

        return EnvironmentResponse(
            environment_id=environment.environment_id,
            environment_name=environment.environment_name,
            description=environment.description,
            base_image=environment.base_image,
            resource_limits=environment.resource_limits,
            resource_requests=environment.resource_requests,
            storage_size=environment.storage_size,
            environment_variables=environment.environment_variables,
            allowed_packages=environment.allowed_packages,
            network_policy=environment.network_policy,
            created_at=datetime.utcnow()
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="create_notebook_environment",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create notebook environment: {str(exc)}"
        )


@router.get("/environments", response_model=Dict[str, any])
async def list_environments(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """List available notebook environments."""
    start_time = time.perf_counter()

    try:
        service = get_developer_workspace_service()

        # Get service health to extract environment info
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_environments",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": {
                "environments_available": health.get("environments_available", 0),
                "available_environments": ["ml_standard", "api_explorer"],  # Mock
                "environments": [
                    {
                        "environment_id": "ml_standard",
                        "environment_name": "ML Development",
                        "description": "Standard ML development environment",
                        "base_image": "jupyter/scipy-notebook:latest"
                    },
                    {
                        "environment_id": "api_explorer",
                        "environment_name": "API Explorer",
                        "description": "Lightweight environment for API testing",
                        "base_image": "jupyter/minimal-notebook:latest"
                    }
                ]
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_environments",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list environments: {str(exc)}"
        )


@router.post("/sessions", response_model=Dict[str, any], status_code=201)
async def create_notebook_session(
    request: Request,
    session_data: SessionCreateRequest,
    principal: Dict[str, Any] | None = Depends(get_principal)
) -> Dict[str, any]:
    """Create a new notebook session."""
    start_time = time.perf_counter()

    try:
        if not principal:
            raise HTTPException(status_code=401, detail="Unauthorized")

        from ..services.developer_workspace_service import create_notebook_session

        # Get user and tenant from request (mock)
        user_id = principal.get("sub") or principal.get("email") or "unknown"
        tenant_id = principal.get("tenant") or "default"

        require_permission(principal, Permission.DEVELOPER_WORKSPACE_WRITE, tenant_id)

        # Create session
        session_id = await create_notebook_session(
            environment_id=session_data.environment_id,
            user_id=user_id,
            tenant_id=tenant_id
        )

        # Deploy template if specified
        if session_data.template_id:
            service = get_developer_workspace_service()
            await service.deploy_notebook_template(
                template_id=session_data.template_id,
                session_id=session_id,
                customizations=session_data.customizations
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="create_notebook_session",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="create_notebook_session",
                query_time_ms=query_time_ms
            ),
            "data": {
                "session_id": session_id,
                "status": "starting",
                "message": "Notebook session created successfully"
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="create_notebook_session",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create notebook session: {str(exc)}"
        )


@router.get("/sessions/{session_id}", response_model=SessionResponse)
async def get_session_status(
    request: Request,
    session_id: str,
    principal: Dict[str, Any] | None = Depends(get_principal)
) -> SessionResponse:
    """Get notebook session status."""
    start_time = time.perf_counter()

    try:
        if not principal:
            raise HTTPException(status_code=401, detail="Unauthorized")

        service = get_developer_workspace_service()
        session = await service.get_session_status(session_id)

        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        require_permission(principal, Permission.DEVELOPER_WORKSPACE_READ, session.tenant_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_session_status",
            query_time_ms=query_time_ms
        )

        return SessionResponse(
            session_id=session.session_id,
            environment_id=session.environment_id,
            user_id=session.user_id,
            tenant_id=session.tenant_id,
            status=session.status,
            pod_name=session.pod_name,
            pod_ip=session.pod_ip,
            notebook_url=session.notebook_url,
            start_time=session.start_time,
            last_activity=session.last_activity,
            resource_usage=session.resource_usage
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_session_status",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get session status: {str(exc)}"
        )


@router.get("/templates", response_model=List[TemplateResponse])
async def list_notebook_templates(
    request: Request,
    response: Response,
    category: Optional[str] = Query(None, description="Filter by template category")
) -> List[TemplateResponse]:
    """List available notebook templates."""
    start_time = time.perf_counter()

    try:
        service = get_developer_workspace_service()
        templates = await service.get_environment_templates(category)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        template_responses = [
            TemplateResponse(
                template_id=template.template_id,
                template_name=template.template_name,
                description=template.description,
                category=template.category,
                required_packages=template.required_packages,
                sample_queries=template.sample_queries,
                documentation_links=template.documentation_links,
                tags=template.tags
            )
            for template in templates
        ]

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="list_notebook_templates",
            query_time_ms=query_time_ms
        )

        return template_responses

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_notebook_templates",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list notebook templates: {str(exc)}"
        )


@router.get("/api-examples", response_model=Dict[str, any])
async def get_api_examples(
    request: Request,
    response: Response,
    category: str = Query("all", description="API example category")
) -> Dict[str, any]:
    """Get API usage examples and documentation."""
    start_time = time.perf_counter()

    try:
        from ..services.developer_workspace_service import get_api_documentation

        # Get API documentation
        examples = await get_api_documentation()

        # Filter by category if specified
        if category != "all":
            examples = {category: examples.get(category, {})}

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_api_examples",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": examples
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_api_examples",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get API examples: {str(exc)}"
        )


@router.get("/guide/{user_id}", response_model=DeveloperGuideResponse)
async def get_developer_guide(
    request: Request,
    user_id: str
) -> DeveloperGuideResponse:
    """Get personalized developer guide."""
    start_time = time.perf_counter()

    try:
        service = get_developer_workspace_service()

        # Generate developer guide
        guide_content = await service.create_developer_guide(user_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_developer_guide",
            query_time_ms=query_time_ms
        )

        return DeveloperGuideResponse(
            user_id=user_id,
            guide_content=guide_content,
            generated_at=datetime.utcnow()
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_developer_guide",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get developer guide: {str(exc)}"
        )


@router.get("/health")
async def get_developer_workspace_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get developer workspace service health status."""
    start_time = time.perf_counter()

    try:
        service = get_developer_workspace_service()
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_developer_workspace_health",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": health
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_developer_workspace_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get developer workspace health: {str(exc)}"
        )
