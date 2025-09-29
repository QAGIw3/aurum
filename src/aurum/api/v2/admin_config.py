"""
Admin API endpoints for configuration management.

Provides endpoints for:
- Getting effective configuration
- Configuration diffing and comparison
- Configuration backup and restore
- Configuration change history
- Schema export
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from aurum.config.change_tracking import get_change_tracker, ChangeType, ChangeSource
from aurum.config.dynamic_config import DynamicConfigService
from aurum.config.validation import get_schema_registry, export_all_schemas
from aurum.security.rbac import require_permissions, Permission

router = APIRouter(prefix="/v2/admin/config", tags=["admin-config"])


class ConfigResponse(BaseModel):
    """Response model for configuration data."""
    config: Dict[str, Any]
    version: int
    timestamp: float
    content_hash: str


class ConfigVersionResponse(BaseModel):
    """Response model for configuration version."""
    version: int
    timestamp: float
    content_hash: str
    change_id: str
    compressed_size: int
    metadata: Dict[str, Any]


class ConfigChangeResponse(BaseModel):
    """Response model for configuration change."""
    change_id: str
    timestamp: float
    change_type: str
    source: str
    actor: str
    namespace: Optional[str]
    reason: str
    correlation_id: Optional[str]
    metadata: Dict[str, Any]


class ConfigDiffResponse(BaseModel):
    """Response model for configuration diff."""
    from_version: int
    to_version: int
    diff: Dict[str, Any]


@router.get("/effective", response_model=ConfigResponse)
async def get_effective_config(
    environment: str = Query("development", description="Environment name"),
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """Get the effective configuration for an environment."""
    try:
        service = DynamicConfigService(environment=environment)
        config = service.get()
        snapshot = service.get_snapshot()

        if not snapshot:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Configuration not available"
            )

        return ConfigResponse(
            config=config,
            version=snapshot.version,
            timestamp=snapshot.timestamp,
            content_hash=snapshot.content_hash
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get effective configuration: {str(e)}"
        )


@router.get("/versions", response_model=List[ConfigVersionResponse])
async def list_config_versions(
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of versions to return"),
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """List configuration versions."""
    try:
        tracker = get_change_tracker()
        versions = tracker.list_versions(limit=limit)

        return [
            ConfigVersionResponse(
                version=v.version,
                timestamp=v.timestamp,
                content_hash=v.content_hash,
                change_id=v.change_id,
                compressed_size=v.compressed_size,
                metadata=v.metadata
            )
            for v in versions
        ]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list configuration versions: {str(e)}"
        )


@router.get("/versions/{version}", response_model=ConfigResponse)
async def get_config_version(
    version: int,
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """Get a specific configuration version."""
    try:
        tracker = get_change_tracker()
        config_version = tracker.get_version(version)

        if not config_version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Configuration version {version} not found"
            )

        return ConfigResponse(
            config=config_version.config,
            version=config_version.version,
            timestamp=config_version.timestamp,
            content_hash=config_version.content_hash
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get configuration version: {str(e)}"
        )


@router.get("/changes", response_model=List[ConfigChangeResponse])
async def list_config_changes(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of changes to return"),
    namespace: Optional[str] = Query(None, description="Filter by namespace"),
    actor: Optional[str] = Query(None, description="Filter by actor"),
    since_timestamp: Optional[float] = Query(None, description="Filter changes since timestamp"),
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """List configuration changes with optional filtering."""
    try:
        tracker = get_change_tracker()
        changes = tracker.get_change_history(
            limit=limit,
            namespace=namespace,
            actor=actor,
            since_timestamp=since_timestamp
        )

        return [
            ConfigChangeResponse(
                change_id=c.change_id,
                timestamp=c.timestamp,
                change_type=c.change_type.value,
                source=c.source.value,
                actor=c.actor,
                namespace=c.namespace,
                reason=c.reason,
                correlation_id=c.correlation_id,
                metadata=c.metadata
            )
            for c in changes
        ]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list configuration changes: {str(e)}"
        )


@router.get("/diff", response_model=ConfigDiffResponse)
async def get_config_diff(
    from_version: int = Query(..., description="From version"),
    to_version: int = Query(..., description="To version"),
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """Get diff between two configuration versions."""
    try:
        tracker = get_change_tracker()
        diff = tracker.compare_versions(from_version, to_version)

        return ConfigDiffResponse(
            from_version=from_version,
            to_version=to_version,
            diff=diff
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get configuration diff: {str(e)}"
        )


@router.post("/backup")
async def backup_config(
    reason: str = Query(..., description="Reason for backup"),
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """Backup the current configuration."""
    try:
        tracker = get_change_tracker()
        service = DynamicConfigService()
        config = service.get()

        change_id = await tracker.backup_current_config(config, reason)

        return {
            "message": "Configuration backed up successfully",
            "change_id": change_id,
            "version": tracker.get_latest_version().version
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to backup configuration: {str(e)}"
        )


@router.post("/restore")
async def restore_config(
    version: int = Query(..., description="Version to restore"),
    reason: str = Query("", description="Reason for restore"),
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """Restore configuration to a specific version."""
    try:
        tracker = get_change_tracker()

        # Get target version info for confirmation
        target_version = tracker.get_version(version)
        if not target_version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Configuration version {version} not found"
            )

        # Perform restoration (this would normally require additional confirmation)
        change_id = await tracker.restore_version(version, "admin_api", reason)

        return {
            "message": "Configuration restored successfully",
            "change_id": change_id,
            "restored_version": version,
            "new_version": tracker.get_latest_version().version
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to restore configuration: {str(e)}"
        )


@router.get("/schemas")
async def export_schemas(
    output_format: str = Query("json", regex="^(json|yaml)$", description="Output format"),
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """Export all configuration schemas."""
    try:
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as temp_dir:
            # Export schemas to temporary directory
            export_all_schemas(temp_dir)

            # Read and return all schema files
            schemas = {}
            for schema_file in os.listdir(temp_dir):
                if schema_file.endswith('.json'):
                    schema_path = os.path.join(temp_dir, schema_file)
                    with open(schema_path, 'r') as f:
                        import json
                        schemas[schema_file] = json.load(f)

            return {
                "message": "Schemas exported successfully",
                "schemas": schemas,
                "count": len(schemas)
            }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to export schemas: {str(e)}"
        )


@router.post("/overrides")
async def set_ephemeral_override(
    key: str = Query(..., description="Override key"),
    value: Dict[str, Any] = Query(..., description="Override value"),
    ttl_seconds: Optional[int] = Query(None, description="TTL in seconds"),
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """Set an ephemeral configuration override."""
    try:
        service = DynamicConfigService()

        # Set the override
        service.set_ephemeral_override(key, value, ttl_seconds)

        return {
            "message": "Ephemeral override set successfully",
            "key": key,
            "ttl_seconds": ttl_seconds
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to set ephemeral override: {str(e)}"
        )


@router.delete("/overrides/{key}")
async def remove_ephemeral_override(
    key: str,
    _permissions: None = Depends(require_permissions(Permission.CONFIG_MANAGE))
):
    """Remove an ephemeral configuration override."""
    try:
        service = DynamicConfigService()
        service.remove_ephemeral_override(key)

        return {
            "message": "Ephemeral override removed successfully",
            "key": key
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to remove ephemeral override: {str(e)}"
        )
