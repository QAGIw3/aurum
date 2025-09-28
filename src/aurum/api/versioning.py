"""API versioning system with backward compatibility and migration support."""

from __future__ import annotations

import asyncio
import inspect
import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..telemetry.context import get_request_id
from ..telemetry import get_logger
from ..observability.metrics import get_metrics_client


class VersionStatus(Enum):
    """Status of an API version."""
    ACTIVE = "active"           # Currently supported
    DEPRECATED = "deprecated"   # Still supported but deprecated
    SUNSET = "sunset"          # Will be removed soon
    RETIRED = "retired"        # No longer supported


class ContentNegotiationStrategy(Enum):
    """Strategies for API version content negotiation."""
    HEADER_BASED = "header"        # Use Accept-Version header
    PATH_BASED = "path"           # Use /v1/, /v2/ path prefix
    QUERY_BASED = "query"         # Use ?version=v1 query parameter
    AUTO_NEGOTIATION = "auto"      # Try header, then path, then default to latest


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
    """Manages API versions and their lifecycle with telemetry tracking."""

    def __init__(self):
        self._versions: Dict[str, APIVersion] = {}
        self._default_version = "1.0.0"
        self._version_aliases: Dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._usage_stats: Dict[str, Dict[str, int]] = {}
        self._logger = get_logger(__name__)
        self._feature_frozen_versions: Set[str] = set()
        self._negotiation_strategy = ContentNegotiationStrategy.AUTO_NEGOTIATION

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

    def set_negotiation_strategy(self, strategy: ContentNegotiationStrategy) -> None:
        """Set the content negotiation strategy."""
        self._negotiation_strategy = strategy

    async def negotiate_version(
        self,
        request: Request,
        path_version: Optional[str] = None
    ) -> Tuple[APIVersion, str]:
        """Negotiate API version based on request headers, path, and strategy.

        Returns:
            Tuple of (selected_version, negotiation_method)
        """
        # Try different negotiation methods based on strategy
        if self._negotiation_strategy == ContentNegotiationStrategy.HEADER_BASED:
            return await self._negotiate_by_header(request)
        elif self._negotiation_strategy == ContentNegotiationStrategy.PATH_BASED:
            return await self._negotiate_by_path(path_version)
        elif self._negotiation_strategy == ContentNegotiationStrategy.QUERY_BASED:
            return await self._negotiate_by_query(request)
        else:  # AUTO_NEGOTIATION
            return await self._auto_negotiate(request, path_version)

    async def _negotiate_by_header(self, request: Request) -> Tuple[APIVersion, str]:
        """Negotiate version using Accept-Version header."""
        accept_version = request.headers.get("Accept-Version")
        if accept_version:
            version = await self.get_version(accept_version)
            if version and version.is_supported():
                return version, "header"

        # Fall back to default
        default_version = await self.get_version(self._default_version)
        if not default_version:
            raise HTTPException(status_code=406, detail="No acceptable API version found")

        return default_version, "default"

    async def _negotiate_by_path(self, path_version: Optional[str]) -> Tuple[APIVersion, str]:
        """Negotiate version using path prefix."""
        if path_version:
            version = await self.get_version(path_version)
            if version and version.is_supported():
                return version, "path"

        # Fall back to default
        default_version = await self.get_version(self._default_version)
        if not default_version:
            raise HTTPException(status_code=406, detail="No acceptable API version found")

        return default_version, "default"

    async def _negotiate_by_query(self, request: Request) -> Tuple[APIVersion, str]:
        """Negotiate version using query parameter."""
        version_param = request.query_params.get("version")
        if version_param:
            version = await self.get_version(version_param)
            if version and version.is_supported():
                return version, "query"

        # Fall back to default
        default_version = await self.get_version(self._default_version)
        if not default_version:
            raise HTTPException(status_code=406, detail="No acceptable API version found")

        return default_version, "default"

    async def _auto_negotiate(
        self,
        request: Request,
        path_version: Optional[str]
    ) -> Tuple[APIVersion, str]:
        """Auto-negotiate version using multiple strategies."""
        # 1. Try Accept-Version header first
        accept_version = request.headers.get("Accept-Version")
        if accept_version:
            version = await self.get_version(accept_version)
            if version and version.is_supported():
                return version, "header"

        # 2. Try path-based version
        if path_version:
            version = await self.get_version(path_version)
            if version and version.is_supported():
                return version, "path"

        # 3. Try query parameter
        version_param = request.query_params.get("version")
        if version_param:
            version = await self.get_version(version_param)
            if version and version.is_supported():
                return version, "query"

        # 4. Fall back to default version
        default_version = await self.get_version(self._default_version)
        if not default_version:
            raise HTTPException(status_code=406, detail="No acceptable API version found")

        return default_version, "default"

    async def get_default_version(self) -> str:
        """Get the default API version."""
        return self._default_version

    async def add_version_alias(self, alias: str, version: str) -> None:
        """Add an alias for a version."""
        if version not in self._versions:
            raise ValueError(f"Version {version} is not registered")

        async with self._lock:
            self._version_aliases[alias] = version

    async def record_version_usage(self, version: str, endpoint: str, method: str) -> None:
        """Record API version usage for analytics."""
        async with self._lock:
            if version not in self._usage_stats:
                self._usage_stats[version] = {}

            endpoint_key = f"{method}:{endpoint}"
            self._usage_stats[version][endpoint_key] = self._usage_stats[version].get(endpoint_key, 0) + 1

        # Record metrics
        metrics_client = get_metrics_client()
        if metrics_client:
            try:
                metrics_client.counter(
                    "aurum_api_version_usage_total",
                    1,
                    tags={"version": version, "endpoint": endpoint, "method": method}
                )
            except Exception:
                pass  # Metrics recording shouldn't break functionality

    async def get_version_analytics(self) -> Dict[str, Any]:
        """Get analytics about API version usage."""
        async with self._lock:
            total_requests = sum(
                sum(endpoints.values()) for endpoints in self._usage_stats.values()
            )

            version_breakdown = {}
            for version, endpoints in self._usage_stats.items():
                version_total = sum(endpoints.values())
                version_breakdown[version] = {
                    "total_requests": version_total,
                    "percentage": (version_total / total_requests * 100) if total_requests > 0 else 0,
                    "endpoints": len(endpoints),
                    "top_endpoints": sorted(
                        endpoints.items(),
                        key=lambda x: x[1],
                        reverse=True
                    )[:5]  # Top 5 endpoints
                }

            return {
                "total_requests": total_requests,
                "version_breakdown": version_breakdown,
                "deprecated_versions_in_use": [
                    v for v in self._usage_stats.keys()
                    if self._versions.get(v, APIVersion("0.0.0")).is_deprecated()
                ],
                "migration_recommendations": self._generate_migration_recommendations(version_breakdown)
            }

    def _generate_migration_recommendations(self, version_breakdown: Dict[str, Any]) -> List[str]:
        """Generate migration recommendations based on usage patterns."""
        recommendations = []

        for version, data in version_breakdown.items():
            if data["percentage"] > 10:  # Significant usage
                version_obj = self._versions.get(version)
                if version_obj and version_obj.is_deprecated():
                    recommendations.append(
                        f"Consider migrating {version} users ({data['percentage']:.1f}% of traffic) "
                        f"to {self._default_version} before sunset"
                    )

        return recommendations


class VersionNegotiationMiddleware:
    """Middleware for automatic API version negotiation."""

    def __init__(self, app, version_manager: VersionManager):
        self.app = app
        self.version_manager = version_manager

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Extract version information from request
        path = scope.get("path", "")
        query_string = scope.get("query_string", b"").decode()
        headers = dict(scope.get("headers", []))

        # Parse path for version prefix (e.g., /v1/endpoint)
        path_version = self._extract_path_version(path)

        # Parse query parameters
        query_params = {}
        if query_string:
            for param in query_string.split("&"):
                if "=" in param:
                    key, value = param.split("=", 1)
                    query_params[key] = value

        # Create request-like object for version negotiation
        class MockRequest:
            def __init__(self, headers, query_params):
                self.headers = headers
                self.query_params = query_params

        mock_request = MockRequest(headers, query_params)

        try:
            # Negotiate version
            selected_version, negotiation_method = await self.version_manager.negotiate_version(
                mock_request, path_version
            )

            # Add version information to scope
            scope["state"] = scope.get("state", {})
            scope["state"]["api_version"] = selected_version
            scope["state"]["version_negotiation_method"] = negotiation_method

            # Record usage
            await self.version_manager.record_version_usage(
                selected_version.version, path, scope.get("method", "GET")
            )

        except HTTPException:
            # Version negotiation failed, let the app handle it
            pass

        await self.app(scope, receive, send)

    def _extract_path_version(self, path: str) -> Optional[str]:
        """Extract version from path prefix (e.g., /v1/endpoint -> v1)."""
        if path.startswith("/v"):
            # Look for version pattern like /v1/, /v2/, etc.
            parts = path.split("/")
            if len(parts) > 1 and parts[1].startswith("v") and len(parts[1]) <= 8:
                version_part = parts[1]
                if version_part[1:].replace(".", "").isdigit():
                    return version_part
        return None


def create_version_negotiation_middleware(app, version_manager: VersionManager):
    """Create version negotiation middleware."""
    return VersionNegotiationMiddleware(app, version_manager)

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

    async def freeze_features(self, version: str) -> None:
        """Mark a version as feature-frozen (no new features, only bug fixes)."""
        api_version = await self.get_version(version)
        if not api_version:
            raise ValueError(f"Version {version} is not registered")

        async with self._lock:
            self._feature_frozen_versions.add(version)
            self._logger.warning(f"Version {version} is now feature-frozen")

    async def is_feature_frozen(self, version: str) -> bool:
        """Check if a version is feature-frozen."""
        async with self._lock:
            return version in self._feature_frozen_versions

    async def track_usage(self, version: str, endpoint: str, method: str) -> None:
        """Track API usage for telemetry and analytics."""
        async with self._lock:
            if version not in self._usage_stats:
                self._usage_stats[version] = {}
            if endpoint not in self._usage_stats[version]:
                self._usage_stats[version][endpoint] = {}

            key = f"{method}:{endpoint}"
            self._usage_stats[version][endpoint][key] = self._usage_stats[version][endpoint].get(key, 0) + 1

    async def get_usage_stats(self, version: Optional[str] = None) -> Dict[str, Any]:
        """Get usage statistics for all versions or a specific version."""
        async with self._lock:
            if version:
                return self._usage_stats.get(version, {})
            return self._usage_stats.copy()

    async def get_deprecation_report(self) -> Dict[str, Any]:
        """Generate a deprecation report with usage statistics."""
        async with self._lock:
            report = {
                "feature_frozen_versions": list(self._feature_frozen_versions),
                "deprecated_versions": [],
                "usage_by_version": {},
                "migration_recommendations": {}
            }

            for ver_str, api_version in self._versions.items():
                if api_version.is_deprecated():
                    report["deprecated_versions"].append({
                        "version": ver_str,
                        "status": api_version.status.value,
                        "deprecation_info": api_version.deprecation_info.model_dump() if api_version.deprecation_info else None,
                        "usage": self._usage_stats.get(ver_str, {})
                    })

                report["usage_by_version"][ver_str] = self._usage_stats.get(ver_str, {})

            return report

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
                "deprecation_info": api_version.deprecation_info.model_dump() if api_version.deprecation_info else None,
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

    # Store versioned router on app
    app.state.versioned_router = versioned_router
    app.state.version_manager = version_manager

    return version_manager, versioned_router


def version_header_middleware(version_manager: VersionManager):
    """Middleware to handle API versioning headers with telemetry tracking."""
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

        # Track usage
        await version_manager.track_usage(
            version=request.state.api_version,
            endpoint=request.url.path,
            method=request.method
        )

        response = await call_next(request)

        # Add version headers to response
        if version:
            api_version = await version_manager.get_version(version)
            if api_version:
                deprecation_headers = api_version.get_deprecation_headers()
                for header, value in deprecation_headers.items():
                    response.headers[header] = value

        # Add version info to response headers
        response.headers["X-API-Version"] = request.state.api_version

        # Check for feature freeze warnings
        if await version_manager.is_feature_frozen(request.state.api_version):
            response.headers["X-API-Feature-Freeze"] = "true"
            response.headers["X-API-Feature-Freeze-Info"] = f"Version {request.state.api_version} is feature-frozen. Please migrate to latest version."

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
