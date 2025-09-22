"""Configuration models for enhanced tenant-aware rate limiting."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple, List


@dataclass
class TenantQuotaConfig:
    """Configuration for tenant quotas."""
    requests_per_second: int
    burst_size: int
    max_concurrent_requests: int = 100
    priority_boost: float = 1.0

    def to_quota(self) -> TenantQuota:
        """Convert to TenantQuota object."""
        from .ratelimit_enhanced import TenantQuota
        return TenantQuota(
            requests_per_second=self.requests_per_second,
            burst_size=self.burst_size,
            max_concurrent_requests=self.max_concurrent_requests,
            priority_boost=self.priority_boost,
        )


@dataclass
class EnhancedRateLimitConfig:
    """Enhanced configuration for tenant-aware rate limiting."""

    # Default quotas per tier
    default_quotas: Dict[str, TenantQuotaConfig] = field(default_factory=lambda: {
        "free": TenantQuotaConfig(5, 10, 50, 0.5),
        "basic": TenantQuotaConfig(20, 40, 100, 0.8),
        "premium": TenantQuotaConfig(100, 200, 500, 1.0),
        "enterprise": TenantQuotaConfig(500, 1000, 1000, 1.2),
    })

    # Tenant-specific overrides
    tenant_overrides: Dict[str, TenantQuotaConfig] = field(default_factory=dict)

    # Path-specific overrides (for backward compatibility)
    path_overrides: Dict[str, Tuple[int, int]] = field(default_factory=dict)

    # Circuit breaker configuration
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_reset_timeout: int = 60

    # System load configuration
    load_update_interval: int = 10

    # Graceful degradation settings
    enable_graceful_degradation: bool = True
    memory_fallback_window_size: int = 1000

    # Tenant identification
    tenant_id_headers: List[str] = field(default_factory=lambda: [
        "X-Tenant-ID",
        "X-Aurum-Tenant",
    ])

    # Whitelist settings
    whitelisted_tenants: List[str] = field(default_factory=list)
    whitelisted_paths: List[str] = field(default_factory=list)

    def get_tenant_quota_config(self, tenant_id: str) -> TenantQuotaConfig:
        """Get quota configuration for a tenant."""
        if tenant_id in self.tenant_overrides:
            return self.tenant_overrides[tenant_id]
        return self._get_tier_quota_config(tenant_id)

    def _get_tier_quota_config(self, tenant_id: str) -> TenantQuotaConfig:
        """Get quota configuration based on tenant tier."""
        # Simple tier detection based on tenant_id pattern
        if tenant_id.startswith("enterprise-"):
            tier = "enterprise"
        elif tenant_id.startswith("premium-"):
            tier = "premium"
        elif tenant_id.startswith("basic-"):
            tier = "basic"
        else:
            tier = "free"

        return self.default_quotas.get(tier, self.default_quotas["free"])

    @classmethod
    def from_settings(cls, settings) -> "EnhancedRateLimitConfig":
        """Create configuration from AurumSettings."""
        # This would integrate with the existing AurumSettings
        # For now, return a basic configuration
        return cls()

    def to_legacy_config(self) -> Dict[str, Any]:
        """Convert to legacy RateLimitConfig format for backward compatibility."""
        return {
            "rps": 10,
            "burst": 20,
            "overrides": self.path_overrides,
            "tenant_overrides": {
                tenant_id: (quota.requests_per_second, quota.burst_size)
                for tenant_id, quota in self.tenant_overrides.items()
            },
            "identifier_header": self.tenant_id_headers[0] if self.tenant_id_headers else None,
            "whitelist": tuple(self.whitelisted_tenants + self.whitelisted_paths),
        }
