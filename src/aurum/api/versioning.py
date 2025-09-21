"""API versioning system with backward compatibility and migration support."""

from __future__ import annotations

import asyncio
import inspect
import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..telemetry.context import get_request_id


class VersionStatus(Enum):
    """Status of an API version."""
    ACTIVE = "active"           # Currently supported
    DEPRECATED = "deprecated"   # Still supported but deprecated
    SUNSET = "sunset"          # Will be removed soon
    RETIRED = "retired"        # No longer supported


class DeprecationInfo(BaseModel):
    """Information about API deprecation."""
    deprecated_in: str
    sunset_on: Optional[str] = None
    removed_in: Optional[str] = None
    migration_guide: str = ""
    alternative_endpoints: List[str] = []


class APIVersion:
    """Represents a specific version of the API."""

    def __init__(
        self,
        version: str,
        status: VersionStatus = VersionStatus.ACTIVE,
        deprecation_info: Optional[DeprecationInfo] = None,
        supported_features: Optional[List[str]] = None
    ):
        self.version = self._normalize_version(version)
        self.status = status
        self.deprecation_info = deprecation_info
        self.supported_features = supported_features or []
        self.created_at = datetime.utcnow()

    def _normalize_version(self, version: str) -> str:
        """Normalize version string (e.g., 'v1', '1.0', '1.2.3')."""
        # Remove leading 'v' if present
        normalized = version.lower().lstrip('v')

        # Ensure it's a valid version format
        if not re.match(r'^(\d+)(\.(\d+)(\.(\d+))?)?$', normalized):
            raise ValueError(f"Invalid version format: {version}")

        return normalized

    def is_supported(self) -> bool:
        """Check if this version is still supported."""
        return self.status in [VersionStatus.ACTIVE, VersionStatus.DEPRECATED]

    def is_deprecated(self) -> bool:
        """Check if this version is deprecated."""
        return self.status == VersionStatus.DEPRECATED

    def will_be_removed(self) -> bool:
        """Check if this version will be removed."""
        return self.status in [VersionStatus.SUNSET, VersionStatus.RETIRED]

    def get_deprecation_headers(self) -> Dict[str, str]:
        """Get HTTP headers for deprecation warnings."""
        if not self.is_deprecated():
            return {}

        headers = {
            "X-API-Version": self.version,
            "X-API-Deprecation": "true",
            "X-API-Deprecation-Info": f"Version {self.version} is deprecated"
        }

        if self.deprecation_info:
            if self.deprecation_info.sunset_on:
                headers["X-API-Sunset"] = self.deprecation_info.sunset_on
            if self.deprecation_info.removed_in:
                headers["X-API-Removed"] = self.deprecation_info.removed_in
            if self.deprecation_info.migration_guide:
                headers["X-API-Migration-Guide"] = self.deprecation_info.migration_guide

        return headers


class VersionManager:
    """Manages API versions and their lifecycle."""

    def __init__(self):
        self._versions: Dict[str, APIVersion] = {}
        self._default_version = "1.0"
        self._version_aliases: Dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def register_version(
        self,
        version: str,
        status: VersionStatus = VersionStatus.ACTIVE,
        deprecation_info: Optional[DeprecationInfo] = None,
        supported_features: Optional[List[str]] = None
    ) -> APIVersion:
        """Register a new API version."""
        api_version = APIVersion(
            version=version,
            status=status,
            deprecation_info=deprecation_info,
            supported_features=supported_features
        )

        async with self._lock:
            self._versions[api_version.version] = api_version

        return api_version

    async def get_version(self, version: str) -> Optional[APIVersion]:
        """Get information about a specific version."""
        # Handle aliases
        actual_version = self._version_aliases.get(version, version)

        async with self._lock:
            return self._versions.get(actual_version)

    async def list_versions(self) -> List[APIVersion]:
        """List all registered versions."""
        async with self._lock:
            return list(self._versions.values())

    async def set_default_version(self, version: str) -> None:
        """Set the default API version."""
        if version not in self._versions:
            raise ValueError(f"Version {version} is not registered")

        async with self._lock:
            self._default_version = version

    async def get_default_version(self) -> str:
        """Get the default API version."""
        return self._default_version

    async def add_version_alias(self, alias: str, version: str) -> None:
        """Add an alias for a version."""
        if version not in self._versions:
            raise ValueError(f"Version {version} is not registered")

        async with self._lock:
            self._version_aliases[alias] = version

    async def deprecate_version(
        self,
        version: str,
        sunset_on: Optional[str] = None,
        removed_in: Optional[str] = None,
        migration_guide: str = ""
    ) -> None:
        """Mark a version as deprecated."""
        api_version = await self.get_version(version)
        if not api_version:
            raise ValueError(f"Version {version} is not registered")

        deprecation_info = DeprecationInfo(
            deprecated_in=datetime.utcnow().isoformat(),
            sunset_on=sunset_on,
            removed_in=removed_in,
            migration_guide=migration_guide
        )

        async with self._lock:
            api_version.status = VersionStatus.DEPRECATED
            api_version.deprecation_info = deprecation_info

    async def retire_version(self, version: str) -> None:
        """Mark a version as retired (no longer supported)."""
        api_version = await self.get_version(version)
        if not api_version:
            raise ValueError(f"Version {version} is not registered")

        async with self._lock:
            api_version.status = VersionStatus.RETIRED

    async def resolve_version(
        self,
        requested_version: Optional[str] = None,
        accept_header: Optional[str] = None
    ) -> Tuple[str, APIVersion]:
        """Resolve which version to use based on request."""
        # Check Accept header first
        if accept_header:
            version = self._parse_accept_header(accept_header)
            if version:
                api_version = await self.get_version(version)
                if api_version and api_version.is_supported():
                    return version, api_version

        # Check requested version
        if requested_version:
            api_version = await self.get_version(requested_version)
            if api_version and api_version.is_supported():
                return requested_version, api_version

        # Fall back to default version
        default_version = await self.get_default_version()
        api_version = await self.get_version(default_version)
        if api_version:
            return default_version, api_version

        # This should not happen if versions are properly configured
        raise HTTPException(
            status_code=500,
            detail="No supported API version available"
        )

    def _parse_accept_header(self, accept_header: str) -> Optional[str]:
        """Parse Accept header to extract version."""
        # Example: application/vnd.aurum.v1+json
        pattern = r'application/vnd\.aurum\.v(\d+(?:\.\d+)*)\+json'
        match = re.search(pattern, accept_header)
        if match:
            return match.group(1)
        return None


class VersionedRouter:
    """Router that supports multiple API versions."""

    def __init__(self, version_manager: VersionManager):
        self.version_manager = version_manager
        self._routers: Dict[str, APIRouter] = {}
        self._middleware = []

    def register_router(
        self,
        version: str,
        router: APIRouter,
        status: VersionStatus = VersionStatus.ACTIVE
    ) -> None:
        """Register a router for a specific version."""
        self._routers[version] = router

        # Register version with manager
        asyncio.create_task(
            self.version_manager.register_version(version, status)
        )

    def get_router(self, version: str) -> Optional[APIRouter]:
        """Get router for a specific version."""
        return self._routers.get(version)

    def list_routers(self) -> Dict[str, APIRouter]:
        """List all registered routers."""
        return self._routers.copy()

    async def get_version_response(
        self,
        request: Request,
        requested_version: Optional[str] = None
    ) -> Response:
        """Get version information response."""
        version_str, api_version = await self.version_manager.resolve_version(
            requested_version,
            request.headers.get("accept")
        )

        return JSONResponse(
            content={
                "version": version_str,
                "status": api_version.status.value,
                "supported_features": api_version.supported_features,
                "deprecation_info": api_version.deprecation_info.dict() if api_version.deprecation_info else None,
                "request_id": get_request_id(),
            },
            headers=api_version.get_deprecation_headers()
        )


def create_versioned_app(
    title: str = "Aurum API",
    description: str = "Aurum Market Intelligence Platform API",
    version: str = "1.0.0"
) -> Tuple[VersionManager, VersionedRouter]:
    """Create a versioned FastAPI application."""
    version_manager = VersionManager()
    versioned_router = VersionedRouter(version_manager)

    # Create main app router
    from fastapi import FastAPI
    app = FastAPI(
        title=title,
        description=description,
        version=version
    )

    # Add version endpoint
    @app.get("/version")
    async def get_version(request: Request):
        return await versioned_router.get_version_response(request)

    # Add health check endpoint
    @app.get("/health")
    async def health_check(request: Request):
        return {
            "status": "healthy",
            "version": await version_manager.get_default_version(),
            "request_id": get_request_id(),
        }

    # Store versioned router on app
    app.state.versioned_router = versioned_router
    app.state.version_manager = version_manager

    return version_manager, versioned_router


def version_header_middleware(version_manager: VersionManager):
    """Middleware to handle API versioning headers."""
    async def middleware(request: Request, call_next):
        # Extract version from headers or path
        version = None

        # Check Accept header
        accept_header = request.headers.get("accept")
        if accept_header:
            version = version_manager._parse_accept_header(accept_header)

        # Check custom version header
        if not version:
            version = request.headers.get("x-api-version")

        # Store version in request state
        request.state.api_version = version or await version_manager.get_default_version()

        response = await call_next(request)

        # Add version headers to response
        if version:
            api_version = await version_manager.get_version(version)
            if api_version:
                deprecation_headers = api_version.get_deprecation_headers()
                for header, value in deprecation_headers.items():
                    response.headers[header] = value

        return response

    return middleware


def create_v1_router() -> APIRouter:
    """Create v1 API router with original endpoints."""
    router = APIRouter(prefix="/v1", tags=["v1"])

    # This would include all the original v1 endpoints
    # For now, we'll add a placeholder
    @router.get("/status")
    async def v1_status():
        return {
            "version": "v1",
            "status": "active",
            "message": "V1 API is active"
        }

    return router


def create_v2_router() -> APIRouter:
    """Create v2 API router with enhanced features."""
    router = APIRouter(prefix="/v2", tags=["v2"])

    @router.get("/status")
    async def v2_status():
        return {
            "version": "v2",
            "status": "active",
            "message": "V2 API with enhanced features",
            "features": ["async_support", "improved_error_handling", "advanced_caching"]
        }

    return router


async def migrate_v1_to_v2(v1_data: Dict[str, Any]) -> Dict[str, Any]:
    """Migrate data from v1 to v2 format."""
    # This would contain migration logic
    # For now, just return the data as-is
    return v1_data


def deprecation_warning(version: str, alternative: str = "") -> str:
    """Generate deprecation warning message."""
    message = f"API version {version} is deprecated"
    if alternative:
        message += f". Please use {alternative} instead."
    return message


# Global version manager
_version_manager = VersionManager()
_versioned_router = VersionedRouter(_version_manager)


def get_version_manager() -> VersionManager:
    """Get the global version manager."""
    return _version_manager


def get_versioned_router() -> VersionedRouter:
    """Get the global versioned router."""
    return _versioned_router
