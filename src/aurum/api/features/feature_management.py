"""Feature flag management endpoints for administration and monitoring."""

from __future__ import annotations

import hashlib
import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Body
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id
from .feature_flags import (
    get_feature_manager,
    FeatureFlagStatus,
    UserSegment,
    ABTestConfiguration,
    FeatureFlagRule,
    FeatureFlag
)


# Pydantic Models for API requests and responses
class FeatureFlagCreateRequest(BaseModel):
    """Request model for creating a feature flag."""
    name: str = Field(..., description="Feature flag name")
    key: str = Field(..., description="Feature flag key")
    description: str = Field("", description="Feature description")
    default_value: bool = Field(False, description="Default value when disabled")
    status: str = Field("disabled", description="Initial status")
    tags: List[str] = Field([], description="Feature tags")


class FeatureFlagResponse(BaseModel):
    """Response model for feature flag data."""
    key: str
    name: str
    description: str
    status: str
    default_value: bool
    rules_count: int
    has_ab_test: bool
    created_at: str
    updated_at: str
    created_by: str
    tags: List[str]


class FeatureFlagListResponse(BaseModel):
    """Response model for feature flag list."""
    meta: Dict[str, Any]
    data: List[FeatureFlagResponse]


class FeatureFlagDetailResponse(BaseModel):
    """Response model for detailed feature flag data."""
    meta: Dict[str, Any]
    data: Dict[str, Any]


class FeatureRuleRequest(BaseModel):
    """Request model for creating a feature rule."""
    name: str = Field(..., description="Rule name")
    conditions: Dict[str, Any] = Field(default_factory=dict, description="Rule conditions")
    rollout_percentage: float = Field(100.0, description="Rollout percentage")
    user_segments: List[str] = Field([], description="User segments")
    required_flags: List[str] = Field([], description="Required flags")
    excluded_flags: List[str] = Field([], description="Excluded flags")


class ABTestRequest(BaseModel):
    """Request model for A/B test configuration."""
    variants: Dict[str, float] = Field(..., description="Variant name to percentage mapping")
    control_variant: str = Field("control", description="Control variant name")
    track_events: List[str] = Field([], description="Events to track")
    end_date: Optional[str] = Field(None, description="Test end date (ISO format)")


class FeatureFlagUpdateRequest(BaseModel):
    """Request model for updating feature flag status."""
    status: str = Field(..., description="New status")


class FeatureTagsUpdateRequest(BaseModel):
    """Request model for updating feature flag tags."""
    tags: List[str] = Field(..., description="New tags list")


class FeatureStatsResponse(BaseModel):
    """Response model for feature statistics."""
    meta: Dict[str, Any]
    data: Dict[str, Any]


class FeatureEvaluationRequest(BaseModel):
    """Request model for feature evaluation."""
    user_id: str = Field(..., description="User ID for evaluation")
    user_segment: str = Field("all_users", description="User segment")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")


class FeatureEvaluationResponse(BaseModel):
    """Response model for feature evaluation."""
    meta: Dict[str, Any]
    data: Dict[str, Any]


class ABTestListResponse(BaseModel):
    """Response model for A/B test list."""
    meta: Dict[str, Any]
    data: List[Dict[str, Any]]


class FeatureUsageResponse(BaseModel):
    """Response model for feature usage statistics."""
    meta: Dict[str, Any]
    data: Dict[str, Any]


router = APIRouter()


@router.post("/v1/admin/features", response_model=FeatureFlagDetailResponse)
async def create_feature_flag(
    request: Request,
    flag_data: FeatureFlagCreateRequest,
    principal=Depends(_routes._get_principal),
) -> Dict[str, str]:
    """Create a new feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        # Validate status
        try:
            flag_status = FeatureFlagStatus(flag_data.status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {flag_data.status}")

        # Create the flag
        created_by = principal.get("user_id", "system") if principal else "system"
        flag = await manager.create_flag(
            name=flag_data.name,
            key=flag_data.key,
            description=flag_data.description,
            default_value=flag_data.default_value,
            status=flag_status,
            created_by=created_by
        )

        # Set tags
        flag.tags = flag_data.tags
        await manager.set_flag(flag)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Feature flag '{flag_data.name}' created successfully",
            "feature_key": flag_data.key,
            "status": flag_status.value,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create feature flag: {str(exc)}"
        ) from exc


@router.get("/v1/admin/features", response_model=FeatureFlagListResponse)
async def list_feature_flags(
    request: Request,
    status: Optional[str] = Query(None, description="Filter by status"),
    tag: Optional[str] = Query(None, description="Filter by tag"),
    page: int = Query(1, description="Page number", ge=1),
    limit: int = Query(50, description="Items per page", ge=1, le=100),
    principal=Depends(_routes._get_principal),
) -> Dict[str, str]:
    """List all feature flags with optional filtering."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()
        flags = await manager.list_flags()

        # Apply filters
        if status:
            try:
                status_enum = FeatureFlagStatus(status)
                flags = [f for f in flags if f.status == status_enum]
            except ValueError:
                pass  # Invalid status, ignore filter

        if tag:
            flags = [f for f in flags if tag in f.tags]

        # Generate ETag from flag keys and last updated times
        etag = hashlib.md5(str(sorted([f.key + f.updated_at.isoformat() for f in flags])).encode()).hexdigest()

        # Check if client has current version
        if request.headers.get("If-None-Match") == etag:
            from fastapi import Response
            return Response(status_code=304)

        # Apply pagination
        total_flags = len(flags)
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_flags = flags[start_idx:end_idx]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        response_data = {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_flags": total_flags,
                "page": page,
                "limit": limit,
                "total_pages": (total_flags + limit - 1) // limit,
                "has_next": end_idx < total_flags,
                "has_prev": page > 1,
                "filters": {
                    "status": status,
                    "tag": tag,
                },
                "etag": etag
            },
            "data": [
                {
                    "key": flag.key,
                    "name": flag.name,
                    "description": flag.description,
                    "status": flag.status.value,
                    "default_value": flag.default_value,
                    "rules_count": len(flag.rules),
                    "has_ab_test": flag.ab_test_config is not None,
                    "created_at": flag.created_at.isoformat(),
                    "updated_at": flag.updated_at.isoformat(),
                    "created_by": flag.created_by,
                    "tags": flag.tags,
                }
                for flag in paginated_flags
            ]
        }

        # Set ETag header
        from fastapi import Response
        response = Response()
        response.headers["ETag"] = etag
        response.headers["Cache-Control"] = "private, max-age=300"  # 5 minutes

        return response_data

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list feature flags: {str(exc)}"
        ) from exc


@router.get("/v1/admin/features/{flag_key}")
async def get_feature_flag(
    request: Request,
    flag_key: str,
    principal=Depends(_routes._get_principal),
) -> Dict[str, str]:
    """Get details for a specific feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()
        flag = await manager.get_flag(flag_key)

        if not flag:
            raise HTTPException(status_code=404, detail=f"Feature flag '{flag_key}' not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                "key": flag.key,
                "name": flag.name,
                "description": flag.description,
                "status": flag.status.value,
                "default_value": flag.default_value,
                "rules": [
                    {
                        "name": rule.name,
                        "rollout_percentage": rule.rollout_percentage,
                        "user_segments": [s.value for s in rule.user_segments],
                        "required_flags": rule.required_flags,
                        "excluded_flags": rule.excluded_flags,
                    }
                    for rule in flag.rules
                ],
                "ab_test_config": {
                    "variants": flag.ab_test_config.variants if flag.ab_test_config else {},
                    "control_variant": flag.ab_test_config.control_variant if flag.ab_test_config else "control",
                    "track_events": flag.ab_test_config.track_events if flag.ab_test_config else [],
                    "end_date": flag.ab_test_config.end_date.isoformat() if flag.ab_test_config and flag.ab_test_config.end_date else None,
                } if flag.ab_test_config else None,
                "created_at": flag.created_at.isoformat(),
                "updated_at": flag.updated_at.isoformat(),
                "created_by": flag.created_by,
                "tags": flag.tags,
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get feature flag: {str(exc)}"
        ) from exc


@router.put("/v1/admin/features/{flag_key}/status")
async def update_feature_flag_status(
    request: Request,
    flag_key: str,
    status: str = Body(..., description="New status"),
    principal=Depends(_routes._get_principal),
) -> Dict[str, str]:
    """Update the status of a feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        # Validate status
        try:
            flag_status = FeatureFlagStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

        # Update status
        success = await manager.update_flag_status(flag_key, flag_status)
        if not success:
            raise HTTPException(status_code=404, detail=f"Feature flag '{flag_key}' not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Feature flag '{flag_key}' status updated to '{status}'",
            "feature_key": flag_key,
            "new_status": status,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update feature flag status: {str(exc)}"
        ) from exc


@router.post("/v1/admin/features/{flag_key}/rules")
async def add_feature_flag_rule(
    request: Request,
    flag_key: str,
    rule_data: Dict = Body(..., description="Rule configuration"),
    principal=Depends(_routes._get_principal),
) -> Dict[str, str]:
    """Add a rule to a feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        # Create rule
        rule = FeatureFlagRule(
            name=rule_data.get("name", "Unnamed Rule"),
            rollout_percentage=rule_data.get("rollout_percentage", 100.0),
            user_segments=[UserSegment(s) for s in rule_data.get("user_segments", [])],
            required_flags=rule_data.get("required_flags", []),
            excluded_flags=rule_data.get("excluded_flags", []),
        )

        # Add rule to flag
        success = await manager.add_rule(flag_key, rule)
        if not success:
            raise HTTPException(status_code=404, detail=f"Feature flag '{flag_key}' not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Rule '{rule.name}' added to feature flag '{flag_key}'",
            "feature_key": flag_key,
            "rule_name": rule.name,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add rule: {str(exc)}"
        ) from exc


@router.post("/v1/admin/features/{flag_key}/ab-test")
async def configure_ab_test(
    request: Request,
    flag_key: str,
    variants: Dict[str, float] = Body(..., description="Variant name to percentage mapping"),
    control_variant: str = Body("control", description="Control variant name"),
    track_events: List[str] = Body([], description="Events to track"),
    end_date: Optional[str] = Body(None, description="Test end date (ISO format)"),
    principal=Depends(_routes._get_principal),
) -> Dict[str, str]:
    """Configure A/B testing for a feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        # Parse end date
        end_date_dt = None
        if end_date:
            from datetime import datetime
            end_date_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))

        # Create A/B test configuration
        ab_config = ABTestConfiguration(
            variants=variants,
            control_variant=control_variant,
            track_events=track_events,
            end_date=end_date_dt,
        )

        # Set A/B test configuration
        success = await manager.set_ab_test(flag_key, ab_config)
        if not success:
            raise HTTPException(status_code=404, detail=f"Feature flag '{flag_key}' not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"A/B test configured for feature flag '{flag_key}'",
            "feature_key": flag_key,
            "variants": variants,
            "control_variant": control_variant,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to configure A/B test: {str(exc)}"
        ) from exc


@router.delete("/v1/admin/features/{flag_key}")
async def delete_feature_flag(
    request: Request,
    flag_key: str,
    principal=Depends(_routes._get_principal),
) -> Dict[str, str]:
    """Delete a feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        # Check if flag exists
        flag = await manager.get_flag(flag_key)
        if not flag:
            raise HTTPException(status_code=404, detail=f"Feature flag '{flag_key}' not found")

        # Delete the flag
        await manager.delete_flag(flag_key)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Feature flag '{flag_key}' deleted successfully",
            "feature_key": flag_key,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete feature flag: {str(exc)}"
        ) from exc


@router.post("/v1/admin/features/{flag_key}/tags")
async def update_feature_flag_tags(
    request: Request,
    flag_key: str,
    tags: List[str] = Body(..., description="New tags list"),
    principal=Depends(_routes._get_principal),
) -> Dict[str, str]:
    """Update tags for a feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        # Get the flag
        flag = await manager.get_flag(flag_key)
        if not flag:
            raise HTTPException(status_code=404, detail=f"Feature flag '{flag_key}' not found")

        # Update tags
        flag.tags = tags
        flag.updated_at = time.perf_counter()  # This would be datetime.utcnow()
        await manager.set_flag(flag)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Tags updated for feature flag '{flag_key}'",
            "feature_key": flag_key,
            "tags": tags,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update feature flag tags: {str(exc)}"
        ) from exc


@router.get("/v1/admin/features/stats")
async def get_feature_stats(
    request: Request,
    principal=Depends(_routes._get_principal),
) -> Dict[str, str]:
    """Get feature flag system statistics."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()
        stats = await manager.get_feature_stats()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": stats
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get feature stats: {str(exc)}"
        ) from exc


@router.get("/v1/admin/features/evaluate")
async def evaluate_feature_flags(
    request: Request,
    principal=Depends(_routes._get_principal),
    user_id: str = Query(..., description="User ID for evaluation"),
    user_segment: str = Query("all_users", description="User segment"),
    tenant_id: Optional[str] = Query(None, description="Tenant ID"),
) -> Dict[str, str]:
    """Evaluate all feature flags for a specific user context."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        # Create user context
        user_context = {
            "user_id": user_id,
            "user_segment": user_segment,
        }
        if tenant_id:
            user_context["tenant_id"] = tenant_id

        # Evaluate all flags
        flag_evaluations = await manager.get_all_flags_for_user(user_context)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "user_context": {
                    "user_id": user_id,
                    "user_segment": user_segment,
                    "tenant_id": tenant_id,
                }
            },
            "data": {
                "flag_evaluations": flag_evaluations,
                "total_flags": len(flag_evaluations),
                "enabled_flags": sum(1 for v in flag_evaluations.values() if v),
                "ab_test_participation": sum(
                    1 for v in flag_evaluations.values()
                    if isinstance(v, str) and v.startswith("variant")
                ),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to evaluate feature flags: {str(exc)}"
        ) from exc


@router.get("/v1/admin/features/ab-tests")
async def get_ab_tests(
    request: Request,
    principal=Depends(_routes._get_principal),
    active_only: bool = Query(True, description="Show only active A/B tests"),
) -> Dict[str, str]:
    """Get all A/B tests with their status."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()
        flags = await manager.list_flags()

        # Filter A/B tests
        ab_tests = []
        for flag in flags:
            if flag.ab_test_config:
                from datetime import datetime
                now = datetime.utcnow()

                is_active = True
                if flag.ab_test_config.end_date and flag.ab_test_config.end_date < now:
                    is_active = False

                if not active_only or is_active:
                    ab_tests.append({
                        "feature_key": flag.key,
                        "feature_name": flag.name,
                        "variants": flag.ab_test_config.variants,
                        "control_variant": flag.ab_test_config.control_variant,
                        "track_events": flag.ab_test_config.track_events,
                        "end_date": flag.ab_test_config.end_date.isoformat() if flag.ab_test_config.end_date else None,
                        "is_active": is_active,
                        "created_at": flag.created_at.isoformat(),
                    })

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_ab_tests": len(ab_tests),
                "active_ab_tests": sum(1 for t in ab_tests if t["is_active"]),
            },
            "data": ab_tests
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get A/B tests: {str(exc)}"
        ) from exc


@router.get("/v1/admin/features/usage")
async def get_feature_usage(
    request: Request,
    principal=Depends(_routes._get_principal),
    hours: int = Query(24, description="Time range in hours"),
) -> Dict[str, str]:
    """Get feature flag usage statistics."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()
        flags = await manager.list_flags()

        # In a real implementation, this would query actual usage data
        # For now, return mock usage data
        usage_data = []
        for flag in flags:
            usage_data.append({
                "feature_key": flag.key,
                "feature_name": flag.name,
                "total_evaluations": 0,  # Would be tracked in production
                "enabled_evaluations": 0,
                "disabled_evaluations": 0,
                "ab_test_participations": 0,
                "last_evaluated": None,
                "error_count": 0,
            })

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "time_range_hours": hours,
            },
            "data": {
                "usage_stats": usage_data,
                "total_features": len(usage_data),
                "summary": {
                    "total_evaluations": sum(u["total_evaluations"] for u in usage_data),
                    "average_evaluations_per_feature": 0,
                }
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get feature usage: {str(exc)}"
        ) from exc
