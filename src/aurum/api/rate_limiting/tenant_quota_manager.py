"""Per-tenant quota management with configuration-based limits and Retry-After headers."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from pydantic import BaseModel

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger
from ..core.settings import get_settings


@dataclass
class QuotaLimit:
    """Individual quota limit configuration."""
    requests_per_second: int = 10
    burst_size: int = 20
    concurrency_limit: int = 50
    daily_quota: Optional[int] = None  # Total requests per day
    monthly_quota: Optional[int] = None  # Total requests per month


@dataclass
class TenantQuotaConfig:
    """Per-tenant quota configuration."""
    tenant_id: str
    limits: QuotaLimit
    priority: str = "normal"  # normal, high, low
    enabled: bool = True


@dataclass
class QuotaUsage:
    """Track quota usage for a tenant."""
    tenant_id: str
    current_requests: int = 0
    burst_tokens: float = 0.0
    daily_count: int = 0
    monthly_count: int = 0
    concurrent_requests: int = 0
    last_reset_daily: float = 0.0
    last_reset_monthly: float = 0.0
    last_request_time: float = 0.0
    reset_time: float = 0.0  # When rate limit resets


class APIQuotaExceeded(Exception):
    """Exception raised when API quota is exceeded."""
    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


class TenantQuotaManager:
    """Manage per-tenant quotas with comprehensive tracking and enforcement."""

    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or Path("config/data_source_quotas.json")
        self.metrics = get_metrics_client()
        self.logger = get_logger(__name__)

        # Configuration
        self._config: Dict[str, TenantQuotaConfig] = {}
        self._usage: Dict[str, QuotaUsage] = {}

        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._config_reload_task: Optional[asyncio.Task] = None

        # Settings
        self.settings = get_settings()

    async def initialize(self):
        """Initialize the quota manager."""
        await self._load_config()
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_usage())
        self._config_reload_task = asyncio.create_task(self._reload_config_periodically())

    async def close(self):
        """Close the quota manager."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        if self._config_reload_task:
            self._config_reload_task.cancel()
            try:
                await self._config_reload_task
            except asyncio.CancelledError:
                pass

    async def check_and_increment(
        self,
        tenant_id: str,
        resource: str = "api"
    ) -> Tuple[bool, Optional[float]]:
        """Check if request is allowed and increment counters."""
        if not tenant_id:
            return True, None  # No tenant ID means no limits

        # Get or create tenant configuration
        config = self._get_tenant_config(tenant_id)
        if not config or not config.enabled:
            return True, None  # No limits for this tenant

        # Get usage tracker
        usage = self._get_usage_tracker(tenant_id, config)

        # Check rate limits
        rate_limit_exceeded, retry_after = self._check_rate_limits(usage, config)
        if rate_limit_exceeded:
            return False, retry_after

        # Check concurrency limits
        concurrency_exceeded, retry_after = self._check_concurrency_limits(usage, config)
        if concurrency_exceeded:
            return False, retry_after

        # Check daily/monthly quotas
        quota_exceeded, retry_after = self._check_quotas(usage, config)
        if quota_exceeded:
            return False, retry_after

        # Increment counters
        self._increment_usage(usage, config)

        # Update metrics
        self._update_metrics(tenant_id, usage, config)

        return True, None

    def _get_tenant_config(self, tenant_id: str) -> Optional[TenantQuotaConfig]:
        """Get tenant configuration."""
        return self._config.get(tenant_id)

    def _get_usage_tracker(self, tenant_id: str, config: TenantQuotaConfig) -> QuotaUsage:
        """Get or create usage tracker for tenant."""
        if tenant_id not in self._usage:
            self._usage[tenant_id] = QuotaUsage(
                tenant_id=tenant_id,
                burst_tokens=config.limits.burst_size,
                last_reset_daily=time.time(),
                last_reset_monthly=time.time()
            )

        return self._usage[tenant_id]

    def _check_rate_limits(self, usage: QuotaUsage, config: TenantQuotaConfig) -> Tuple[bool, Optional[float]]:
        """Check rate limits using token bucket algorithm."""
        current_time = time.time()

        # Reset burst tokens based on time elapsed
        time_elapsed = current_time - usage.last_request_time
        tokens_to_add = time_elapsed * config.limits.requests_per_second
        usage.burst_tokens = min(
            config.limits.burst_size,
            usage.burst_tokens + tokens_to_add
        )

        # Check if request is allowed
        if usage.burst_tokens >= 1.0:
            usage.burst_tokens -= 1.0
            usage.last_request_time = current_time
            return False, None
        else:
            # Calculate retry after time
            retry_after = 1.0 / config.limits.requests_per_second
            return True, retry_after

    def _check_concurrency_limits(self, usage: QuotaUsage, config: TenantQuotaConfig) -> Tuple[bool, Optional[float]]:
        """Check concurrency limits."""
        if usage.concurrent_requests >= config.limits.concurrency_limit:
            # Estimate retry time based on current load
            avg_processing_time = 1.0  # seconds, could be made configurable
            retry_after = avg_processing_time * (usage.concurrent_requests - config.limits.concurrency_limit + 1)
            return True, min(retry_after, 300)  # Max 5 minutes

        usage.concurrent_requests += 1
        return False, None

    def decrement_concurrency(self, tenant_id: str):
        """Decrement concurrency counter when request completes."""
        usage = self._usage.get(tenant_id)
        if usage and usage.concurrent_requests > 0:
            usage.concurrent_requests -= 1

    def _check_quotas(self, usage: QuotaUsage, config: TenantQuotaConfig) -> Tuple[bool, Optional[float]]:
        """Check daily and monthly quotas."""
        current_time = time.time()
        limits = config.limits

        # Check daily quota
        if limits.daily_quota and usage.daily_count >= limits.daily_quota:
            # Daily quota exceeded - retry after midnight
            tomorrow = current_time + 86400  # 24 hours
            midnight = tomorrow - (tomorrow % 86400)
            retry_after = midnight - current_time
            return True, retry_after

        # Check monthly quota
        if limits.monthly_quota and usage.monthly_count >= limits.monthly_quota:
            # Monthly quota exceeded - retry after month end
            # This is a simplified calculation
            days_in_month = 30  # Could be more accurate
            month_end = current_time + (days_in_month * 86400)
            retry_after = month_end - current_time
            return True, retry_after

        return False, None

    def _increment_usage(self, usage: QuotaUsage, config: TenantQuotaConfig):
        """Increment usage counters."""
        current_time = time.time()
        limits = config.limits

        # Increment daily/monthly counts
        if limits.daily_quota:
            usage.daily_count += 1

        if limits.monthly_quota:
            usage.monthly_count += 1

        usage.last_request_time = current_time

        # Reset daily quota if needed
        if (current_time - usage.last_reset_daily) >= 86400:  # 24 hours
            usage.daily_count = 0
            usage.last_reset_daily = current_time

        # Reset monthly quota if needed (simplified)
        if (current_time - usage.last_reset_monthly) >= 2592000:  # 30 days
            usage.monthly_count = 0
            usage.last_reset_monthly = current_time

    def _update_metrics(self, tenant_id: str, usage: QuotaUsage, config: TenantQuotaConfig):
        """Update Prometheus metrics."""
        limits = config.limits

        self.metrics.gauge(f"tenant_quota_current_requests_{tenant_id}", usage.current_requests)
        self.metrics.gauge(f"tenant_quota_burst_tokens_{tenant_id}", usage.burst_tokens)
        self.metrics.gauge(f"tenant_quota_concurrent_{tenant_id}", usage.concurrent_requests)
        self.metrics.gauge(f"tenant_quota_daily_count_{tenant_id}", usage.daily_count)
        self.metrics.gauge(f"tenant_quota_monthly_count_{tenant_id}", usage.monthly_count)

        if limits.daily_quota:
            self.metrics.gauge(f"tenant_quota_daily_utilization_{tenant_id}",
                              usage.daily_count / limits.daily_quota)

        if limits.monthly_quota:
            self.metrics.gauge(f"tenant_quota_monthly_utilization_{tenant_id}",
                              usage.monthly_count / limits.monthly_quota)

    async def _load_config(self):
        """Load quota configuration from file."""
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r') as f:
                    config_data = json.load(f)

                self._config = {}
                for tenant_data in config_data.get('tenants', []):
                    tenant_id = tenant_data['tenant_id']
                    limits_data = tenant_data.get('limits', {})

                    self._config[tenant_id] = TenantQuotaConfig(
                        tenant_id=tenant_id,
                        limits=QuotaLimit(
                            requests_per_second=limits_data.get('requests_per_second', 10),
                            burst_size=limits_data.get('burst_size', 20),
                            concurrency_limit=limits_data.get('concurrency_limit', 50),
                            daily_quota=limits_data.get('daily_quota'),
                            monthly_quota=limits_data.get('monthly_quota')
                        ),
                        priority=tenant_data.get('priority', 'normal'),
                        enabled=tenant_data.get('enabled', True)
                    )

                self.logger.info(f"Loaded quota configuration for {len(self._config)} tenants")
            else:
                self.logger.warning(f"Quota configuration file not found: {self.config_path}")

        except Exception as e:
            self.logger.error(f"Error loading quota configuration: {e}")

    async def _reload_config_periodically(self):
        """Reload configuration periodically."""
        while True:
            try:
                await asyncio.sleep(300)  # Reload every 5 minutes
                await self._load_config()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error reloading quota configuration: {e}")

    async def _cleanup_expired_usage(self):
        """Clean up expired usage data."""
        while True:
            try:
                await asyncio.sleep(3600)  # Clean up every hour

                current_time = time.time()
                expired_tenants = []

                for tenant_id, usage in self._usage.items():
                    # Clean up if no activity for 24 hours
                    if current_time - usage.last_request_time > 86400:
                        expired_tenants.append(tenant_id)

                for tenant_id in expired_tenants:
                    del self._usage[tenant_id]

                if expired_tenants:
                    self.logger.info(f"Cleaned up usage data for {len(expired_tenants)} tenants")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error cleaning up usage data: {e}")

    def get_tenant_usage(self, tenant_id: str) -> Optional[QuotaUsage]:
        """Get usage information for a tenant."""
        return self._usage.get(tenant_id)

    def get_tenant_config_info(self, tenant_id: str) -> Optional[TenantQuotaConfig]:
        """Get configuration information for a tenant."""
        return self._config.get(tenant_id)

    def get_all_tenant_usage(self) -> Dict[str, QuotaUsage]:
        """Get usage information for all tenants."""
        return self._usage.copy()

    def get_quota_status(self, tenant_id: str) -> Dict[str, Any]:
        """Get comprehensive quota status for a tenant."""
        usage = self._usage.get(tenant_id)
        config = self._config.get(tenant_id)

        if not usage or not config:
            return {"error": "Tenant not found"}

        return {
            "tenant_id": tenant_id,
            "current_requests": usage.current_requests,
            "burst_tokens": usage.burst_tokens,
            "concurrent_requests": usage.concurrent_requests,
            "daily_count": usage.daily_count,
            "monthly_count": usage.monthly_count,
            "rate_limit_rps": config.limits.requests_per_second,
            "burst_size": config.limits.burst_size,
            "concurrency_limit": config.limits.concurrency_limit,
            "daily_quota": config.limits.daily_quota,
            "monthly_quota": config.limits.monthly_quota,
            "priority": config.priority,
            "enabled": config.enabled,
            "daily_utilization": usage.daily_count / config.limits.daily_quota if config.limits.daily_quota else 0,
            "monthly_utilization": usage.monthly_count / config.limits.monthly_quota if config.limits.monthly_quota else 0
        }


# Global quota manager instance
_quota_manager: Optional[TenantQuotaManager] = None


async def get_tenant_quota_manager() -> TenantQuotaManager:
    """Get the global tenant quota manager."""
    global _quota_manager
    if _quota_manager is None:
        _quota_manager = TenantQuotaManager()
        await _quota_manager.initialize()
    return _quota_manager


def create_quota_config_file_example():
    """Create an example quota configuration file."""
    example_config = {
        "tenants": [
            {
                "tenant_id": "premium_customer_1",
                "limits": {
                    "requests_per_second": 100,
                    "burst_size": 200,
                    "concurrency_limit": 500,
                    "daily_quota": 100000,
                    "monthly_quota": 3000000
                },
                "priority": "high",
                "enabled": True
            },
            {
                "tenant_id": "standard_customer_1",
                "limits": {
                    "requests_per_second": 10,
                    "burst_size": 20,
                    "concurrency_limit": 50,
                    "daily_quota": 10000,
                    "monthly_quota": 300000
                },
                "priority": "normal",
                "enabled": True
            },
            {
                "tenant_id": "trial_customer_1",
                "limits": {
                    "requests_per_second": 1,
                    "burst_size": 5,
                    "concurrency_limit": 10,
                    "daily_quota": 100,
                    "monthly_quota": 1000
                },
                "priority": "low",
                "enabled": True
            }
        ]
    }

    return json.dumps(example_config, indent=2)


__all__ = [
    "APIQuotaExceeded",
    "TenantQuotaManager",
    "QuotaLimit",
    "TenantQuotaConfig",
    "QuotaUsage",
    "get_tenant_quota_manager",
    "create_quota_config_file_example",
]
