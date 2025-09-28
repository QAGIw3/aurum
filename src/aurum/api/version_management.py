"""API version management endpoints for administrators."""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from ..telemetry.context import get_request_id
from .versioning import (
    get_version_manager,
    VersionStatus,
    DeprecationInfo,
    deprecation_warning,
)


router = APIRouter()


@router.get("/v1/admin/versions")
async def list_versions(
    request: Request,
    status: Optional[str] = Query(None, description="Filter by status"),
) -> Dict[str, List[Dict[str, str]]]:
    """List all API versions with their status."""
    start_time = time.perf_counter()

    try:
        version_manager = get_version_manager()
        versions = await version_manager.list_versions()

        # Filter by status if provided
        if status:
            try:
                status_enum = VersionStatus(status)
                versions = [v for v in versions if v.status == status_enum]
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid status: {status}"
                )

        # Convert to response format
        versions_data = []
        for version in versions:
            version_data = {
                "version": version.version,
                "status": version.status.value,
                "supported_features": version.supported_features,
                "created_at": version.created_at.isoformat(),
            }

            if version.deprecation_info:
                version_data["deprecation_info"] = {
                    "deprecated_in": version.deprecation_info.deprecated_in,
                    "sunset_on": version.deprecation_info.sunset_on,
                    "removed_in": version.deprecation_info.removed_in,
                    "migration_guide": version.deprecation_info.migration_guide,
                    "alternative_endpoints": version.deprecation_info.alternative_endpoints,
                }

            versions_data.append(version_data)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_versions": len(versions_data),
            },
            "data": versions_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list versions: {str(exc)}"
        ) from exc


@router.get("/v1/admin/versions/{version}")
async def get_version_info(
    request: Request,
    version: str,
) -> Dict[str, str]:
    """Get detailed information about a specific version."""
    start_time = time.perf_counter()

    try:
        version_manager = get_version_manager()
        api_version = await version_manager.get_version(version)

        if not api_version:
            raise HTTPException(
                status_code=404,
                detail=f"Version {version} not found"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = {
            "version": api_version.version,
            "status": api_version.status.value,
            "supported_features": api_version.supported_features,
            "created_at": api_version.created_at.isoformat(),
            "is_supported": api_version.is_supported(),
            "is_deprecated": api_version.is_deprecated(),
            "will_be_removed": api_version.will_be_removed(),
        }

        if api_version.deprecation_info:
            result["deprecation_info"] = {
                "deprecated_in": api_version.deprecation_info.deprecated_in,
                "sunset_on": api_version.deprecation_info.sunset_on,
                "removed_in": api_version.deprecation_info.removed_in,
                "migration_guide": api_version.deprecation_info.migration_guide,
                "alternative_endpoints": api_version.deprecation_info.alternative_endpoints,
            }

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": result
        }

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get version info: {str(exc)}"
        ) from exc


@router.post("/v1/admin/versions/{version}/deprecate")
async def deprecate_version(
    request: Request,
    version: str,
    sunset_on: Optional[str] = None,
    removed_in: Optional[str] = None,
    migration_guide: str = "",
) -> Dict[str, str]:
    """Mark a version as deprecated."""
    start_time = time.perf_counter()

    try:
        version_manager = get_version_manager()

        # Check if version exists
        api_version = await version_manager.get_version(version)
        if not api_version:
            raise HTTPException(
                status_code=404,
                detail=f"Version {version} not found"
            )

        # Deprecate the version
        await version_manager.deprecate_version(
            version=version,
            sunset_on=sunset_on,
            removed_in=removed_in,
            migration_guide=migration_guide
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Version {version} marked as deprecated",
            "version": version,
            "new_status": "deprecated",
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
            detail=f"Failed to deprecate version: {str(exc)}"
        ) from exc


@router.post("/v1/admin/versions/{version}/retire")
async def retire_version(
    request: Request,
    version: str,
) -> Dict[str, str]:
    """Mark a version as retired (no longer supported)."""
    start_time = time.perf_counter()

    try:
        version_manager = get_version_manager()

        # Check if version exists
        api_version = await version_manager.get_version(version)
        if not api_version:
            raise HTTPException(
                status_code=404,
                detail=f"Version {version} not found"
            )

        # Check if version is deprecated first
        if api_version.status != VersionStatus.DEPRECATED:
            raise HTTPException(
                status_code=400,
                detail=f"Version {version} must be deprecated before retiring"
            )

        # Retire the version
        await version_manager.retire_version(version)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Version {version} marked as retired",
            "version": version,
            "new_status": "retired",
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
            detail=f"Failed to retire version: {str(exc)}"
        ) from exc


@router.post("/v1/admin/versions/{version}/aliases/{alias}")
async def add_version_alias(
    request: Request,
    version: str,
    alias: str,
) -> Dict[str, str]:
    """Add an alias for a version."""
    start_time = time.perf_counter()

    try:
        version_manager = get_version_manager()

        # Check if version exists
        api_version = await version_manager.get_version(version)
        if not api_version:
            raise HTTPException(
                status_code=404,
                detail=f"Version {version} not found"
            )

        # Add alias
        await version_manager.add_version_alias(alias, version)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Alias '{alias}' added for version {version}",
            "version": version,
            "alias": alias,
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
            detail=f"Failed to add version alias: {str(exc)}"
        ) from exc


@router.get("/v1/admin/versions/{version}/usage")
async def get_version_usage(
    request: Request,
    version: str,
    hours: int = Query(24, description="Time period in hours"),
) -> Dict[str, str]:
    """Get usage statistics for a version."""
    start_time = time.perf_counter()

    try:
        # In a real implementation, this would query metrics/logs
        # For now, return placeholder data
        usage_data = {
            "version": version,
            "period_hours": hours,
            "requests_total": 0,
            "requests_per_hour": 0,
            "average_response_time": 0.0,
            "error_rate": 0.0,
            "top_endpoints": [],
            "last_used": None,
        }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": usage_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get version usage: {str(exc)}"
        ) from exc


@router.get("/v1/admin/versions/migration-paths")
async def get_migration_paths(
    request: Request,
    from_version: Optional[str] = Query(None, description="Source version"),
    to_version: Optional[str] = Query(None, description="Target version"),
) -> Dict[str, str]:
    """Get available migration paths between versions."""
    start_time = time.perf_counter()

    try:
        version_manager = get_version_manager()
        versions = await version_manager.list_versions()

        # Build migration paths
        active_versions = [v for v in versions if v.is_supported()]
        migration_paths = []

        for from_v in active_versions:
            for to_v in active_versions:
                if from_v.version != to_v.version:
                    migration_paths.append({
                        "from_version": from_v.version,
                        "to_version": to_v.version,
                        "compatibility": "full" if to_v.version > from_v.version else "partial",
                        "breaking_changes": from_v.version == "1.0" and to_v.version == "2.0",
                        "migration_guide": f"Migrate from {from_v.version} to {to_v.version}"
                    })

        # Filter if specific versions requested
        if from_version:
            migration_paths = [p for p in migration_paths if p["from_version"] == from_version]
        if to_version:
            migration_paths = [p for p in migration_paths if p["to_version"] == to_version]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_paths": len(migration_paths),
            },
            "data": migration_paths
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get migration paths: {str(exc)}"
        ) from exc


@router.post("/v1/admin/versions/{version}/features")
async def update_version_features(
    request: Request,
    version: str,
    features: List[str],
) -> Dict[str, str]:
    """Update supported features for a version."""
    start_time = time.perf_counter()

    try:
        version_manager = get_version_manager()

        # Check if version exists
        api_version = await version_manager.get_version(version)
        if not api_version:
            raise HTTPException(
                status_code=404,
                detail=f"Version {version} not found"
            )

        # Update features
        api_version.supported_features = features

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Features updated for version {version}",
            "version": version,
            "features": features,
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
            detail=f"Failed to update version features: {str(exc)}"
        ) from exc


@router.get("/v1/admin/versions/deprecation-schedule")
async def get_deprecation_schedule(
    request: Request,
) -> Dict[str, str]:
    """Get deprecation schedule for all versions."""
    start_time = time.perf_counter()

    try:
        version_manager = get_version_manager()
        versions = await version_manager.list_versions()

        schedule = {
            "deprecated_versions": [],
            "versions_to_deprecate": [],
            "versions_to_retire": [],
        }

        for version in versions:
            if version.is_deprecated():
                schedule["deprecated_versions"].append({
                    "version": version.version,
                    "sunset_on": version.deprecation_info.sunset_on if version.deprecation_info else None,
                    "removed_in": version.deprecation_info.removed_in if version.deprecation_info else None,
                })
            elif version.status == VersionStatus.SUNSET:
                schedule["versions_to_retire"].append({
                    "version": version.version,
                    "remove_after": version.deprecation_info.removed_in if version.deprecation_info else None,
                })

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": schedule
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get deprecation schedule: {str(exc)}"
        ) from exc


@router.post("/v1/admin/versions/bulk-deprecate")
async def bulk_deprecate_versions(
    request: Request,
    versions: List[str],
    sunset_on: Optional[str] = None,
    migration_guide: str = "",
) -> Dict[str, str]:
    """Deprecate multiple versions at once."""
    start_time = time.perf_counter()

    try:
        version_manager = get_version_manager()

        deprecated = []
        errors = []

        for version in versions:
            try:
                api_version = await version_manager.get_version(version)
                if api_version:
                    await version_manager.deprecate_version(
                        version=version,
                        sunset_on=sunset_on,
                        migration_guide=migration_guide
                    )
                    deprecated.append(version)
                else:
                    errors.append(f"Version {version} not found")
            except Exception as exc:
                errors.append(f"Failed to deprecate {version}: {str(exc)}")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = {
            "message": f"Bulk deprecation completed",
            "deprecated": deprecated,
            "errors": errors,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

        if errors:
            result["message"] += " with some errors"

        return result

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to bulk deprecate versions: {str(exc)}"
        ) from exc


@router.get("/v1/admin/versions/analytics")
async def get_version_analytics(
    request: Request,
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Get analytics about API version usage and migration patterns."""

    _require_admin(principal)

    start_time = time.perf_counter()
    user_id = get_user_id()
    request_id = get_request_id()

    try:
        version_manager = get_version_manager()
        analytics = await version_manager.get_version_analytics()

        log_structured(
            "info",
            "version_analytics_retrieved",
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {
                "request_id": request_id,
                "query_time_ms": int((time.perf_counter() - start_time) * 1000),
            },
            "data": analytics
        }
    except Exception as exc:
        log_structured(
            "error",
            "version_analytics_failed",
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Failed to retrieve version analytics: {str(exc)}")


@router.post("/v1/admin/versions/migrate/{from_version}")
async def initiate_version_migration(
    from_version: str,
    to_version: str = Query(..., description="Target version for migration"),
    principal: dict = Depends(_get_principal),
) -> Dict[str, Any]:
    """Initiate migration from one API version to another."""

    _require_admin(principal)

    start_time = time.perf_counter()
    user_id = get_user_id()
    request_id = get_request_id()

    try:
        version_manager = get_version_manager()

        # Validate versions
        source_version = await version_manager.get_version(from_version)
        target_version = await version_manager.get_version(to_version)

        if not source_version:
            raise HTTPException(status_code=404, detail=f"Source version {from_version} not found")
        if not target_version:
            raise HTTPException(status_code=404, detail=f"Target version {to_version} not found")

        # Check if migration is valid
        if source_version.status == VersionStatus.RETIRED:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot migrate from retired version {from_version}"
            )

        # Create migration plan
        migration_plan = {
            "source_version": from_version,
            "target_version": to_version,
            "migration_type": "gradual" if source_version.status == VersionStatus.DEPRECATED else "immediate",
            "estimated_impact": await _estimate_migration_impact(version_manager, from_version),
            "recommended_actions": await _get_migration_actions(version_manager, from_version, to_version),
        }

        log_structured(
            "info",
            "version_migration_initiated",
            from_version=from_version,
            to_version=to_version,
            user_id=user_id,
            request_id=request_id,
        )

        return {
            "meta": {
                "request_id": request_id,
                "query_time_ms": int((time.perf_counter() - start_time) * 1000),
            },
            "data": migration_plan
        }
    except HTTPException:
        raise
    except Exception as exc:
        log_structured(
            "error",
            "version_migration_failed",
            from_version=from_version,
            to_version=to_version,
            error=str(exc),
            user_id=user_id,
            request_id=request_id,
        )
        raise HTTPException(status_code=500, detail=f"Failed to initiate migration: {str(exc)}")


async def _estimate_migration_impact(version_manager: VersionManager, version: str) -> Dict[str, Any]:
    """Estimate the impact of migrating from a specific version."""
    analytics = await version_manager.get_version_analytics()

    if version not in analytics["version_breakdown"]:
        return {"impact_level": "unknown", "affected_endpoints": 0}

    version_data = analytics["version_breakdown"][version]

    impact_level = "low" if version_data["percentage"] < 5 else "medium" if version_data["percentage"] < 20 else "high"

    return {
        "impact_level": impact_level,
        "affected_endpoints": version_data["endpoints"],
        "traffic_percentage": version_data["percentage"],
        "recommendation": f"Consider {impact_level}-impact migration strategy"
    }


async def _get_migration_actions(
    version_manager: VersionManager,
    from_version: str,
    to_version: str
) -> List[str]:
    """Get recommended actions for version migration."""
    actions = [
        f"Update documentation to highlight {to_version} as the preferred version",
        "Add deprecation warnings to {from_version} endpoints",
        "Monitor migration progress through version analytics",
    ]

    # Add version-specific actions
    source_version = await version_manager.get_version(from_version)
    if source_version and source_version.deprecation_info:
        if source_version.deprecation_info.migration_guide:
            actions.append(f"Provide migration guide: {source_version.deprecation_info.migration_guide}")

    return actions
