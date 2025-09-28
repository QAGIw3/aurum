"""v1 Model Registry API with RBAC, audit logging, and immutable versioning."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from ..auth import Permission, require_permission
from ..services.model_registry_service import (
    ModelConfig,
    ModelVersion,
    RegisteredModel,
    get_model_registry_service,
)
from ..telemetry.context import get_request_id
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v1/model-registry", tags=["model-registry"])


class AuditMetadataRequest(BaseModel):
    """Audit metadata accompanying mutating operations."""

    requested_by: Optional[str] = Field(None, description="User initiating the action")
    tenant_id: Optional[str] = Field(None, description="Tenant identifier")
    request_id: Optional[str] = Field(None, description="Client supplied correlation id")
    source: Optional[str] = Field(None, description="Originating system")
    notes: Optional[str] = Field(None, description="Additional context for audit pipeline")
    tags: Dict[str, Any] = Field(default_factory=dict, description="Free-form audit annotations")


class ModelCreateRequest(BaseModel):
    """Payload for registering a model container."""

    model_name: str = Field(..., description="Unique model identifier")
    model_type: str = Field(..., description="Model family or framework")
    description: str = Field("", description="Model description")
    owners: List[str] = Field(default_factory=list, description="Model owner roster")
    tags: Dict[str, str] = Field(default_factory=dict, description="Model tags")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    audit: Optional[AuditMetadataRequest] = Field(None, description="Audit metadata for the create request")


class ModelUpdateRequest(BaseModel):
    """Partial update payload for a registered model."""

    description: Optional[str] = Field(None, description="Updated model description")
    status: Optional[str] = Field(None, description="Updated lifecycle state (active, deprecated)")
    owners: Optional[List[str]] = Field(None, description="Replacement owner list")
    tags: Optional[Dict[str, str]] = Field(None, description="Tags to upsert")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Metadata entries to upsert")
    audit: Optional[AuditMetadataRequest] = Field(None, description="Audit metadata for the update")


class ModelVersionCreateRequest(BaseModel):
    """Payload for registering an immutable model version."""

    version_number: Optional[str] = Field(None, description="Semantic version label (defaults to next sequential version)")
    version_id: Optional[str] = Field(None, description="Optional external version identifier")
    description: str = Field("", description="Version description")
    config: Dict[str, Any] = Field(..., description="Training configuration payload")
    training_start_date: datetime = Field(..., description="Training start timestamp")
    training_end_date: datetime = Field(..., description="Training completion timestamp")
    model_path: Optional[str] = Field(None, description="Storage location for the serialized artefact")
    model_size_bytes: int = Field(0, ge=0, description="Model artifact size in bytes")
    performance_metrics: Dict[str, float] = Field(default_factory=dict, description="Evaluation metrics")
    feature_importance: Dict[str, float] = Field(default_factory=dict, description="Feature importance mapping")
    validation_results: Dict[str, Any] = Field(default_factory=dict, description="Validation outputs")
    status: str = Field("active", description="Version lifecycle state")
    created_by: Optional[str] = Field(None, description="Actor responsible for creating this version")
    tags: Dict[str, str] = Field(default_factory=dict, description="Version tags")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Supplementary metadata")
    audit: Optional[AuditMetadataRequest] = Field(None, description="Audit metadata for the version registration")


class RegisteredModelResponse(BaseModel):
    """Registered model presentation payload."""

    model_name: str
    model_type: str
    description: str
    status: str
    latest_version: Optional[str]
    champion_version_id: Optional[str]
    total_versions: int
    owners: List[str]
    tags: Dict[str, str]
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class ModelListResponse(BaseModel):
    """Paginated response for registered models."""

    data: List[RegisteredModelResponse]
    meta: Dict[str, Any]


class ModelDetailResponse(BaseModel):
    """Single model response wrapper."""

    data: RegisteredModelResponse
    meta: Dict[str, Any]


class ModelVersionResponse(BaseModel):
    """Model version presentation payload."""

    version_id: str
    model_name: str
    version_number: str
    description: str
    config: Dict[str, Any]
    training_start_date: datetime
    training_end_date: datetime
    model_path: Optional[str]
    model_size_bytes: int
    performance_metrics: Dict[str, float]
    feature_importance: Dict[str, float]
    validation_results: Dict[str, Any]
    status: str
    created_at: datetime
    created_by: str
    tags: Dict[str, str]
    metadata: Dict[str, Any]
    champion_score: Optional[float]


class ModelVersionListResponse(BaseModel):
    """Paginated response for model versions."""

    data: List[ModelVersionResponse]
    meta: Dict[str, Any]


class AuditRecordResponse(BaseModel):
    """Audit record presentation payload."""

    event_id: str
    action: str
    model_name: str
    reference: Dict[str, Any]
    audit: Dict[str, Any]
    timestamp: datetime


class AuditListResponse(BaseModel):
    """Audit log response wrapper."""

    data: List[AuditRecordResponse]
    meta: Dict[str, Any]


class DocEndpoint(BaseModel):
    """Documentation entry for an endpoint."""

    method: str
    path: str
    summary: str
    permission: str


class DocumentationResponse(BaseModel):
    """Structured documentation for the Model Registry v1 API."""

    version: str
    description: str
    endpoints: List[DocEndpoint]
    rbac: Dict[str, str]
    audit_events: List[str]
    meta: Dict[str, Any]


def _authorize(request: Request, permission: Permission) -> None:
    """Enforce RBAC for the incoming request."""

    principal = getattr(request.state, "principal", None)
    tenant = getattr(request.state, "tenant", None) or getattr(request.state, "tenant_id", None)
    require_permission(principal, permission, tenant)


def _to_model_response(model: RegisteredModel) -> RegisteredModelResponse:
    """Convert service model representation to API response."""

    return RegisteredModelResponse(
        model_name=model.model_name,
        model_type=model.model_type,
        description=model.description,
        status=model.status,
        latest_version=model.latest_version,
        champion_version_id=model.champion_version_id,
        total_versions=model.total_versions,
        owners=list(model.owners),
        tags=dict(model.tags),
        metadata=dict(model.metadata),
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


def _to_version_response(version: ModelVersion) -> ModelVersionResponse:
    """Convert a model version to API response."""

    return ModelVersionResponse(
        version_id=version.version_id,
        model_name=version.model_name,
        version_number=version.version_number,
        description=version.description,
        config=version.config.model_dump(),
        training_start_date=version.training_start_date,
        training_end_date=version.training_end_date,
        model_path=version.model_path,
        model_size_bytes=version.model_size_bytes,
        performance_metrics=dict(version.performance_metrics),
        feature_importance=dict(version.feature_importance),
        validation_results=dict(version.validation_results),
        status=version.status,
        created_at=version.created_at,
        created_by=version.created_by,
        tags=dict(version.tags),
        metadata=dict(version.metadata),
        champion_score=version.champion_score,
    )


def _to_audit_response(record) -> AuditRecordResponse:
    """Convert audit record to API response payload."""

    audit = record.audit.dict() if hasattr(record.audit, "dict") else dict(record.audit)
    return AuditRecordResponse(
        event_id=record.event_id,
        action=record.action,
        model_name=record.model_name,
        reference=dict(record.reference),
        audit=audit,
        timestamp=record.timestamp,
    )


def _meta(elapsed_ms: float, **extra: Any) -> Dict[str, Any]:
    """Standard metadata payload with duration and request identifier."""

    payload: Dict[str, Any] = {
        "request_id": get_request_id(),
        "elapsed_ms": round(elapsed_ms, 2),
    }
    payload.update({k: v for k, v in extra.items() if v is not None})
    return payload


@router.get("/docs", response_model=DocumentationResponse)
async def get_model_registry_docs(request: Request) -> DocumentationResponse:
    """Return inline documentation for the v1 model registry API."""

    _authorize(request, Permission.MODEL_REGISTRY_READ)
    start = time.perf_counter()

    endpoints = [
        DocEndpoint(method="GET", path="/v1/model-registry/models", summary="List registered models", permission=Permission.MODEL_REGISTRY_READ.value),
        DocEndpoint(method="POST", path="/v1/model-registry/models", summary="Register a model", permission=Permission.MODEL_REGISTRY_WRITE.value),
        DocEndpoint(method="PATCH", path="/v1/model-registry/models/{model_name}", summary="Update model metadata", permission=Permission.MODEL_REGISTRY_WRITE.value),
        DocEndpoint(method="DELETE", path="/v1/model-registry/models/{model_name}", summary="Archive a model", permission=Permission.MODEL_REGISTRY_WRITE.value),
        DocEndpoint(method="POST", path="/v1/model-registry/models/{model_name}/versions", summary="Register a model version", permission=Permission.MODEL_REGISTRY_WRITE.value),
        DocEndpoint(method="GET", path="/v1/model-registry/models/{model_name}/versions", summary="List model versions", permission=Permission.MODEL_REGISTRY_READ.value),
        DocEndpoint(method="GET", path="/v1/model-registry/audit", summary="List audit events", permission=Permission.MODEL_REGISTRY_READ.value),
    ]

    audit_actions = [
        "register_model",
        "update_model_metadata",
        "archive_model",
        "register_model_version",
        "promote_model",
        "compare_models",
    ]

    telemetry = get_telemetry_facade()
    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.docs", elapsed_ms=elapsed_ms)

    rbac_notes = {
        Permission.MODEL_REGISTRY_READ.value: "Allows read-only access to model metadata, versions, and audit logs.",
        Permission.MODEL_REGISTRY_WRITE.value: "Allows registering models, creating immutable versions, and updating lifecycle metadata.",
    }

    return DocumentationResponse(
        version="1.0",
        description="Model Registry v1.0 covering CRUD, immutable versioning, RBAC, and audit transparency.",
        endpoints=endpoints,
        rbac=rbac_notes,
        audit_events=audit_actions,
        meta=_meta(elapsed_ms),
    )


@router.get("/models", response_model=ModelListResponse)
async def list_registered_models(
    request: Request,
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by model status"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ModelListResponse:
    """List registered models with optional status filtering."""

    _authorize(request, Permission.MODEL_REGISTRY_READ)
    start = time.perf_counter()
    telemetry = get_telemetry_facade()

    service = get_model_registry_service()
    models = await service.list_models(status=status_filter, limit=limit, offset=offset)
    data = [_to_model_response(model) for model in models]

    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.list_models", elapsed_ms=elapsed_ms, count=len(data))

    return ModelListResponse(
        data=data,
        meta=_meta(elapsed_ms, count=len(data), limit=limit, offset=offset, status=status_filter),
    )


@router.get("/models/{model_name}", response_model=ModelDetailResponse)
async def get_registered_model(request: Request, model_name: str) -> ModelDetailResponse:
    """Retrieve metadata for a specific model."""

    _authorize(request, Permission.MODEL_REGISTRY_READ)
    start = time.perf_counter()
    telemetry = get_telemetry_facade()

    service = get_model_registry_service()
    model = service.get_model(model_name)
    if not model:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Model not found")

    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.get_model", elapsed_ms=elapsed_ms)

    return ModelDetailResponse(
        data=_to_model_response(model),
        meta=_meta(elapsed_ms),
    )


@router.post("/models", response_model=ModelDetailResponse, status_code=status.HTTP_201_CREATED)
async def register_model(request: Request, payload: ModelCreateRequest) -> ModelDetailResponse:
    """Register a new model container."""

    _authorize(request, Permission.MODEL_REGISTRY_WRITE)
    start = time.perf_counter()
    telemetry = get_telemetry_facade()

    service = get_model_registry_service()
    audit_payload = payload.audit.dict(exclude_none=True) if payload.audit else None

    model = await service.register_model(
        model_name=payload.model_name,
        model_type=payload.model_type,
        description=payload.description,
        owners=payload.owners,
        tags=payload.tags,
        metadata=payload.metadata,
        audit_metadata=audit_payload,
    )

    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.register_model", elapsed_ms=elapsed_ms)

    return ModelDetailResponse(
        data=_to_model_response(model),
        meta=_meta(elapsed_ms),
    )


@router.patch("/models/{model_name}", response_model=ModelDetailResponse)
async def update_registered_model(request: Request, model_name: str, payload: ModelUpdateRequest) -> ModelDetailResponse:
    """Update mutable metadata for a registered model."""

    _authorize(request, Permission.MODEL_REGISTRY_WRITE)
    start = time.perf_counter()
    telemetry = get_telemetry_facade()

    service = get_model_registry_service()
    audit_payload = payload.audit.dict(exclude_none=True) if payload.audit else None

    try:
        model = service.update_model_metadata(
            model_name=model_name,
            description=payload.description,
            status=payload.status,
            owners=payload.owners,
            tags=payload.tags,
            metadata=payload.metadata,
            audit_metadata=audit_payload,
        )
    except ValueError as exc:
        telemetry.record_error("model_registry.update_model", error=str(exc))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.update_model", elapsed_ms=elapsed_ms)

    return ModelDetailResponse(
        data=_to_model_response(model),
        meta=_meta(elapsed_ms),
    )


@router.delete("/models/{model_name}")
async def archive_model(request: Request, model_name: str, reason: Optional[str] = Query(None, description="Optional archive reason")) -> Dict[str, Any]:
    """Archive a model without deleting historical versions."""

    _authorize(request, Permission.MODEL_REGISTRY_WRITE)
    start = time.perf_counter()
    telemetry = get_telemetry_facade()

    service = get_model_registry_service()

    archived = service.archive_model(model_name, reason=reason)
    if not archived:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Model not found or already archived")

    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.archive_model", elapsed_ms=elapsed_ms)

    return {
        "message": "model archived",
        "model_name": model_name,
        "meta": _meta(elapsed_ms, reason=reason),
    }


@router.post("/models/{model_name}/versions", response_model=ModelVersionResponse, status_code=status.HTTP_201_CREATED)
async def register_model_version(request: Request, model_name: str, payload: ModelVersionCreateRequest) -> ModelVersionResponse:
    """Register an immutable model version."""

    _authorize(request, Permission.MODEL_REGISTRY_WRITE)
    start = time.perf_counter()
    telemetry = get_telemetry_facade()

    service = get_model_registry_service()
    audit_payload = payload.audit.dict(exclude_none=True) if payload.audit else None

    version_number = payload.version_number or service.get_next_version_number(model_name)
    version_id = payload.version_id or str(uuid4())

    try:
        config = ModelConfig(**payload.config)
    except Exception as exc:  # pragma: no cover - validation handled by pydantic
        telemetry.record_error("model_registry.version_config_invalid", error=str(exc))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid model configuration: {exc}") from exc

    creator = payload.created_by
    if not creator and audit_payload:
        creator = audit_payload.get("requested_by")
    if not creator:
        creator = "unknown"

    version = ModelVersion(
        version_id=version_id,
        model_name=model_name,
        version_number=version_number,
        description=payload.description,
        config=config,
        training_start_date=payload.training_start_date,
        training_end_date=payload.training_end_date,
        model_path=payload.model_path or f"models/{model_name}/{version_number}",
        model_size_bytes=payload.model_size_bytes,
        performance_metrics=payload.performance_metrics,
        feature_importance=payload.feature_importance,
        validation_results=payload.validation_results,
        status=payload.status,
        created_by=creator,
        tags=payload.tags,
        metadata=payload.metadata,
    )

    try:
        stored = await service.register_model_version(version, audit_metadata=audit_payload)
    except ValueError as exc:
        telemetry.record_error("model_registry.register_version", error=str(exc))
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.register_version", elapsed_ms=elapsed_ms)

    return _to_version_response(stored)


@router.get("/models/{model_name}/versions", response_model=ModelVersionListResponse)
async def list_model_versions(
    request: Request,
    model_name: str,
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by version status"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ModelVersionListResponse:
    """List versions of a registered model."""

    _authorize(request, Permission.MODEL_REGISTRY_READ)
    start = time.perf_counter()
    telemetry = get_telemetry_facade()

    service = get_model_registry_service()
    versions = await service.list_model_versions(model_name, status=status_filter, limit=limit, offset=offset)
    data = [_to_version_response(version) for version in versions]

    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.list_versions", elapsed_ms=elapsed_ms, count=len(data))

    return ModelVersionListResponse(
        data=data,
        meta=_meta(elapsed_ms, count=len(data), limit=limit, offset=offset, status=status_filter),
    )


@router.get("/models/{model_name}/versions/{version_number}", response_model=ModelVersionResponse)
async def get_model_version(request: Request, model_name: str, version_number: str) -> ModelVersionResponse:
    """Retrieve a specific model version."""

    _authorize(request, Permission.MODEL_REGISTRY_READ)
    start = time.perf_counter()
    telemetry = get_telemetry_facade()

    service = get_model_registry_service()
    version = service.get_model_version(model_name, version_number)
    if not version:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Model version not found")

    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.get_version", elapsed_ms=elapsed_ms)

    return _to_version_response(version)


@router.get("/audit", response_model=AuditListResponse)
async def list_audit_events(
    request: Request,
    action: Optional[str] = Query(None, description="Filter audit events by action"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> AuditListResponse:
    """List audit events recorded by the model registry."""

    _authorize(request, Permission.MODEL_REGISTRY_READ)
    start = time.perf_counter()
    telemetry = get_telemetry_facade()

    service = get_model_registry_service()
    events = service.get_audit_events(action=action, limit=limit, offset=offset)
    data = [_to_audit_response(event) for event in events]

    elapsed_ms = (time.perf_counter() - start) * 1000
    telemetry.record_success("model_registry.list_audit", elapsed_ms=elapsed_ms, count=len(data))

    return AuditListResponse(
        data=data,
        meta=_meta(elapsed_ms, count=len(data), limit=limit, offset=offset, action=action),
    )
