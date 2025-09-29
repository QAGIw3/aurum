"""Feature flag management endpoints for administration and monitoring."""

from __future__ import annotations

import hashlib
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Body, Depends
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id
from ..auth import Permission, require_permissions
from .. import routes as _routes
from . import (
    get_feature_manager,
    FeatureFlagStatus,
    UserSegment,
    ABTestConfiguration,
    FeatureFlagRule,
    FeatureFlag,
    RolloutPlan,
    RolloutStrategy,
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


# Rollout Plan Models
class RolloutPlanCreateRequest(BaseModel):
    """Request model for creating a rollout plan."""
    strategy: str = Field(..., description="Rollout strategy (percentage, gradual, targeted, scheduled, dependent)")
    name: str = Field("", description="Rollout plan name")
    description: str = Field("", description="Rollout plan description")

    # For percentage-based rollouts
    percentage: float = Field(100.0, description="Percentage of users to rollout to (0-100)")

    # For gradual rollouts
    start_percentage: float = Field(0.0, description="Starting percentage for gradual rollout")
    end_percentage: float = Field(100.0, description="Target percentage for gradual rollout")
    step_percentage: float = Field(10.0, description="Step size for each interval")
    step_interval_hours: int = Field(24, description="Hours between steps")

    # For scheduled rollouts
    start_time: Optional[str] = Field(None, description="ISO datetime when rollout starts")
    end_time: Optional[str] = Field(None, description="ISO datetime when rollout ends")

    # For targeted rollouts
    user_segments: List[str] = Field([], description="User segments to target")
    required_flags: List[str] = Field([], description="Flags that must be enabled")
    excluded_flags: List[str] = Field([], description="Flags that must be disabled")


class RolloutPlanResponse(BaseModel):
    """Response model for rollout plan data."""
    flag_key: str
    strategy: str
    name: str
    description: str
    percentage: float
    start_percentage: float
    end_percentage: float
    step_percentage: float
    step_interval_hours: int
    start_time: Optional[str]
    end_time: Optional[str]
    user_segments: List[str]
    required_flags: List[str]
    excluded_flags: List[str]
    created_at: str
    updated_at: str
    created_by: str
    effective_percentage: float
    is_active: bool


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


router = APIRouter(
    dependencies=[Depends(require_permissions(Permission.FEATURE_FLAGS_MANAGE, tenant_scoped=False))]
)


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


# Rollout Plan Endpoints
@router.post("/v1/admin/features/{flag_key}/rollout", response_model=Dict[str, Any])
async def create_rollout_plan(
    flag_key: str,
    rollout_plan: RolloutPlanCreateRequest,
    request: Request,
    principal=Depends(_routes._get_principal),
) -> Dict[str, Any]:
    """Create a rollout plan for a feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        # Validate strategy
        try:
            strategy = RolloutStrategy(rollout_plan.strategy)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid rollout strategy: {rollout_plan.strategy}"
            )

        # Validate user segments
        user_segments = []
        for segment_str in rollout_plan.user_segments:
            try:
                user_segments.append(UserSegment(segment_str))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid user segment: {segment_str}"
                )

        # Parse datetime strings
        start_time_dt = None
        end_time_dt = None
        if rollout_plan.start_time:
            try:
                start_time_dt = datetime.fromisoformat(rollout_plan.start_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid start_time format. Use ISO datetime format."
                )

        if rollout_plan.end_time:
            try:
                end_time_dt = datetime.fromisoformat(rollout_plan.end_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid end_time format. Use ISO datetime format."
                )

        # Create rollout plan
        success = await manager.create_rollout_plan(
            flag_key=flag_key,
            strategy=strategy.value,
            name=rollout_plan.name,
            description=rollout_plan.description,
            percentage=rollout_plan.percentage,
            start_percentage=rollout_plan.start_percentage,
            end_percentage=rollout_plan.end_percentage,
            step_percentage=rollout_plan.step_percentage,
            step_interval_hours=rollout_plan.step_interval_hours,
            start_time=start_time_dt,
            end_time=end_time_dt,
            user_segments=user_segments,
            required_flags=rollout_plan.required_flags,
            excluded_flags=rollout_plan.excluded_flags,
            created_by=principal.user_id or "admin"
        )

        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to create rollout plan"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                "flag_key": flag_key,
                "rollout_plan_created": True,
                "strategy": rollout_plan.strategy,
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create rollout plan: {str(exc)}"
        ) from exc


@router.get("/v1/admin/features/{flag_key}/rollout", response_model=Dict[str, Any])
async def get_rollout_plan(
    flag_key: str,
    request: Request,
    principal=Depends(_routes._get_principal),
) -> Dict[str, Any]:
    """Get the rollout plan for a feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        plan_data = await manager.get_rollout_plan(flag_key)
        if not plan_data:
            raise HTTPException(
                status_code=404,
                detail=f"No rollout plan found for flag: {flag_key}"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": plan_data
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get rollout plan: {str(exc)}"
        ) from exc


@router.put("/v1/admin/features/{flag_key}/rollout", response_model=Dict[str, Any])
async def update_rollout_plan(
    flag_key: str,
    updates: Dict[str, Any],
    request: Request,
    principal=Depends(_routes._get_principal),
) -> Dict[str, Any]:
    """Update a rollout plan for a feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        # Validate user segments if provided
        if "user_segments" in updates:
            user_segments = []
            for segment_str in updates["user_segments"]:
                try:
                    user_segments.append(UserSegment(segment_str))
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid user segment: {segment_str}"
                    )
            updates["user_segments"] = user_segments

        # Validate datetime strings if provided
        if "start_time" in updates and updates["start_time"]:
            try:
                updates["start_time"] = datetime.fromisoformat(updates["start_time"].replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid start_time format. Use ISO datetime format."
                )

        if "end_time" in updates and updates["end_time"]:
            try:
                updates["end_time"] = datetime.fromisoformat(updates["end_time"].replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid end_time format. Use ISO datetime format."
                )

        success = await manager.update_rollout_plan(flag_key, **updates)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"No rollout plan found for flag: {flag_key}"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                "flag_key": flag_key,
                "rollout_plan_updated": True,
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update rollout plan: {str(exc)}"
        ) from exc


@router.delete("/v1/admin/features/{flag_key}/rollout", response_model=Dict[str, Any])
async def delete_rollout_plan(
    flag_key: str,
    request: Request,
    principal=Depends(_routes._get_principal),
) -> Dict[str, Any]:
    """Delete the rollout plan for a feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        success = await manager.delete_rollout_plan(flag_key)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"No rollout plan found for flag: {flag_key}"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                "flag_key": flag_key,
                "rollout_plan_deleted": True,
            }
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete rollout plan: {str(exc)}"
        ) from exc


# Analytics Endpoints
@router.get("/v1/admin/features/analytics", response_model=Dict[str, Any])
async def get_all_flags_analytics(
    request: Request,
    principal=Depends(_routes._get_principal),
    hours: int = Query(24, description="Time range in hours", ge=1, le=168),
) -> Dict[str, Any]:
    """Get analytics for all feature flags."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        analytics_data = await manager.get_all_flags_analytics(hours)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": analytics_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get feature analytics: {str(exc)}"
        ) from exc


@router.get("/v1/admin/features/{flag_key}/analytics", response_model=Dict[str, Any])
async def get_flag_analytics(
    flag_key: str,
    request: Request,
    principal=Depends(_routes._get_principal),
    hours: int = Query(24, description="Time range in hours", ge=1, le=168),
) -> Dict[str, Any]:
    """Get analytics for a specific feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        analytics_data = await manager.get_flag_analytics(flag_key, hours)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": analytics_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get flag analytics: {str(exc)}"
        ) from exc


@router.get("/v1/admin/features/{flag_key}/ab-analytics", response_model=Dict[str, Any])
async def get_ab_test_analytics(
    flag_key: str,
    request: Request,
    principal=Depends(_routes._get_principal),
    hours: int = Query(24, description="Time range in hours", ge=1, le=168),
) -> Dict[str, Any]:
    """Get A/B test analytics for a specific feature flag."""
    start_time = time.perf_counter()

    try:
        _routes._require_admin(principal)
        manager = get_feature_manager()

        analytics_data = await manager.get_ab_test_analytics(flag_key, hours)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": analytics_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get A/B test analytics: {str(exc)}"
        ) from exc
