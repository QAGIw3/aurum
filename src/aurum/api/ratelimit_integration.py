"""Integration layer for enhanced tenant-aware rate limiting.

This module provides a seamless integration between the existing rate limiting
system and the enhanced tenant-aware rate limiting with graceful degradation.
"""

from __future__ import annotations

from typing import Optional

from aurum.core import AurumSettings
from starlette.requests import Request

from .config import CacheConfig
from .ratelimit import RateLimitConfig
from .ratelimit_enhanced import (
    TenantRateLimitManager,
    TenantRateLimitMiddleware,
    get_rate_limit_manager,
    ratelimit_admin_router,
)


class IntegratedRateLimitMiddleware:
    """Integrated rate limiting middleware that uses enhanced features when available."""

    def __init__(self, app, cache_cfg: CacheConfig, rl_cfg: RateLimitConfig):
        self.app = app
        self.cache_cfg = cache_cfg
        self.rl_cfg = rl_cfg
        self._enhanced_middleware = None
        self._legacy_middleware = None

        # Try to initialize enhanced middleware
        try:
            self._enhanced_middleware = TenantRateLimitMiddleware(app, cache_cfg)
            print("✅ Enhanced tenant-aware rate limiting enabled")
        except Exception as e:
            print(f"⚠️ Enhanced rate limiting unavailable: {e}, falling back to legacy")

        # Fallback to legacy middleware if needed
        if self._enhanced_middleware is None:
            from .ratelimit import RateLimitMiddleware
            self._legacy_middleware = RateLimitMiddleware(app, cache_cfg, rl_cfg)
            print("✅ Legacy rate limiting middleware active")

    async def __call__(self, scope, receive, send):
        if self._enhanced_middleware:
            await self._enhanced_middleware(scope, receive, send)
        elif self._legacy_middleware:
            await self._legacy_middleware(scope, receive, send)
        else:
            # No rate limiting - pass through
            await self.app(scope, receive, send)


def get_tenant_id_from_request(request: Request) -> Optional[str]:
    """Extract tenant ID from request using enhanced logic."""
    # Check headers
    tenant_headers = ["X-Tenant-ID", "X-Aurum-Tenant"]
    for header in tenant_headers:
        tenant_id = request.headers.get(header)
        if tenant_id:
            return tenant_id

    # Check principal context
    try:
        principal = getattr(request.state, "principal", None)
        if principal and isinstance(principal, dict):
            return principal.get("tenant")
    except Exception:
        pass

    # Check URL path for tenant information
    path_parts = request.url.path.strip("/").split("/")
    if len(path_parts) > 1 and path_parts[0] in ["api", "v1"]:
        # Look for tenant in path like /api/{tenant_id}/...
        if len(path_parts) > 1:
            potential_tenant = path_parts[1]
            if len(potential_tenant) > 3:  # Basic validation
                return potential_tenant

    return None


def configure_enhanced_rate_limiting(settings: AurumSettings):
    """Configure enhanced rate limiting for the application."""
    from .ratelimit_config import EnhancedRateLimitConfig

    # Create enhanced configuration
    enhanced_config = EnhancedRateLimitConfig.from_settings(settings)

    # Configure tenant-specific quotas based on settings
    if hasattr(settings, 'api') and hasattr(settings.api, 'rate_limit'):
        rl_settings = settings.api.rate_limit

        # Set tenant overrides from settings
        for tenant_override in getattr(rl_settings, 'tenant_overrides', []):
            tenant_id = tenant_override.get('tenant_id')
            if tenant_id:
                from .ratelimit_config import TenantQuotaConfig
                quota_config = TenantQuotaConfig(
                    requests_per_second=tenant_override.get('requests_per_second', 10),
                    burst_size=tenant_override.get('burst_size', 20),
                    max_concurrent_requests=tenant_override.get('max_concurrent_requests', 100),
                    priority_boost=tenant_override.get('priority_boost', 1.0),
                )
                enhanced_config.tenant_overrides[tenant_id] = quota_config

    return enhanced_config


def create_rate_limit_middleware(app, settings: AurumSettings):
    """Create the appropriate rate limiting middleware based on configuration."""
    cache_cfg = CacheConfig.from_settings(settings)

    # Try enhanced rate limiting first
    try:
        enhanced_config = configure_enhanced_rate_limiting(settings)
        return TenantRateLimitMiddleware(app, cache_cfg)
    except Exception as e:
        print(f"Enhanced rate limiting unavailable: {e}, using legacy")

    # Fall back to legacy rate limiting
    rl_config = RateLimitConfig.from_settings(settings)
    from .ratelimit import RateLimitMiddleware
    return RateLimitMiddleware(app, cache_cfg, rl_config)


# Export admin router for integration
__all__ = [
    "IntegratedRateLimitMiddleware",
    "get_tenant_id_from_request",
    "configure_enhanced_rate_limiting",
    "create_rate_limit_middleware",
    "ratelimit_admin_router",
]
