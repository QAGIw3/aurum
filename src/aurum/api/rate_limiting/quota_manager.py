"""API quota management and rate limiting system.

This module contains two related systems:

- External API QuotaManager: tracks usage for third‑party APIs (EIA, FRED, NOAA, World Bank)
  keyed by provider API keys. It provides coarse rate checks and simulated requests for
  integration testing.

- TenantQuotaManager: enforces per‑tenant quotas for the Aurum API itself using Redis.
  It supports per‑tenant requests/second (RPS) with burst via a sliding window over 1s
  (backed by a Redis ZSET) and per‑tenant daily caps via Redis counters with day‑scoped
  keys. This manager is lightweight and stateless beyond Redis keys.

Both managers are safe to import even if Redis is unavailable; the tenant manager
will no‑op if no client is provided.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import redis
from redis.exceptions import ConnectionError, TimeoutError

from ..config import settings
from ..observability.telemetry import get_api_telemetry


class APIQuotaExceeded(Exception):
    """Exception raised when API quota is exceeded."""
    pass


class APIRateLimit:
    """Rate limit configuration for an API."""

    def __init__(
        self,
        requests_per_minute: int = 60,
        requests_per_hour: int = 1000,
        requests_per_day: int = 10000,
        burst_limit: int = 10
    ):
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.requests_per_day = requests_per_day
        self.burst_limit = burst_limit

        # Calculate time windows
        self.minute_window = 60
        self.hour_window = 3600
        self.day_window = 86400


class APIKey:
    """API key configuration with usage tracking."""

    def __init__(
        self,
        key: str,
        name: str,
        quota: APIRateLimit,
        priority: int = 1,
        is_active: bool = True
    ):
        self.key = key
        self.name = name
        self.quota = quota
        self.priority = priority
        self.is_active = is_active
        self.created_at = datetime.now(timezone.utc)
        self.last_used_at: Optional[datetime] = None
        self.total_requests = 0
        self.failed_requests = 0


class APIEndpoint:
    """API endpoint configuration."""

    def __init__(
        self,
        name: str,
        base_url: str,
        auth_header: str = "Authorization",
        auth_prefix: str = "Bearer",
        rate_limit: APIRateLimit = None,
        timeout: float = 30.0,
        retry_attempts: int = 3,
        backoff_factor: float = 2.0
    ):
        self.name = name
        self.base_url = base_url
        self.auth_header = auth_header
        self.auth_prefix = auth_prefix
        self.rate_limit = rate_limit or APIRateLimit()
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.backoff_factor = backoff_factor


class QuotaManager:
    """Manages API quotas and rate limiting across multiple services."""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """Initialize quota manager."""
        self.redis_client = redis_client or redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )

        self.telemetry = get_api_telemetry()

        # API configurations
        self.endpoints: Dict[str, APIEndpoint] = {}
        self.api_keys: Dict[str, List[APIKey]] = {}

        # Initialize with default API configurations
        self._init_default_apis()

        # Start quota monitoring
        self._start_monitoring()

    def _init_default_apis(self):
        """Initialize default API configurations with actual documented limits.

        EIA: No clear documented requests per second/minute/day rate-limit found.
            Rows per response: default max 5,000 rows for JSON, or 300 if using XML.

        FRED: Community sources say ~120 requests per minute.
            Official docs state there is rate limiting (HTTP 429) but no numeric limit published.

        NOAA: Access token limited to 5 requests per second; 10,000 requests per day.

        World Bank: Max 15,000 data points per call (including nulls) for SDMX endpoints.
                   For Indicators API: max 60 indicators per request.
                   No published requests per time period limit found.
        """

        # EIA API configuration
        # Note: No clear documented requests per second/minute/day rate-limit found
        # Rows per response: default max 5,000 rows for JSON, or 300 if using XML
        eia_rate_limit = APIRateLimit(
            requests_per_minute=60,    # Conservative estimate based on community sources
            requests_per_hour=600,     # Conservative estimate
            requests_per_day=5000,     # Based on row limits and typical usage patterns
            burst_limit=5
        )

        self.endpoints["eia"] = APIEndpoint(
            name="EIA",
            base_url="https://api.eia.gov/v2",
            auth_header="X-API-Key",
            auth_prefix="",
            rate_limit=eia_rate_limit,
            timeout=30.0,
            retry_attempts=3,
            backoff_factor=1.5
        )

        # FRED API configuration
        # Community sources say ~120 requests per minute
        # Official docs state there is rate limiting (HTTP 429) but no numeric limit published
        fred_rate_limit = APIRateLimit(
            requests_per_minute=120,   # Based on community reports
            requests_per_hour=7200,    # 120 * 60 minutes
            requests_per_day=100000,   # Conservative estimate based on community usage
            burst_limit=10
        )

        self.endpoints["fred"] = APIEndpoint(
            name="FRED",
            base_url="https://api.stlouisfed.org/fred",
            auth_header="api_key",
            auth_prefix="",
            rate_limit=fred_rate_limit,
            timeout=30.0,
            retry_attempts=2,
            backoff_factor=2.0
        )

        # NOAA API configuration
        # Access token limited to 5 requests per second; 10,000 requests per day
        noaa_rate_limit = APIRateLimit(
            requests_per_minute=300,   # 5 requests per second * 60 seconds
            requests_per_hour=1800,    # 300 * 6 (conservative)
            requests_per_day=10000,    # Documented limit
            burst_limit=2
        )

        self.endpoints["noaa"] = APIEndpoint(
            name="NOAA",
            base_url="https://api.weather.gov",
            auth_header="User-Agent",
            auth_prefix="Aurum-Data-Platform/1.0",
            rate_limit=noaa_rate_limit,
            timeout=45.0,
            retry_attempts=3,
            backoff_factor=3.0  # Longer backoff for NOAA
        )

        # World Bank API configuration
        # Max 15,000 data points per call (including nulls) for SDMX endpoints
        # For Indicators API: max 60 indicators per request
        # No published requests per time period limit found
        wb_rate_limit = APIRateLimit(
            requests_per_minute=60,    # Conservative estimate
            requests_per_hour=1000,    # Conservative estimate
            requests_per_day=10000,    # Conservative estimate
            burst_limit=10
        )

        self.endpoints["world_bank"] = APIEndpoint(
            name="World Bank",
            base_url="https://api.worldbank.org/v2",
            auth_header="",  # No auth required
            auth_prefix="",
            rate_limit=wb_rate_limit,
            timeout=30.0,
            retry_attempts=3,
            backoff_factor=1.5
        )

    def add_api_key(self, api_name: str, key: str, name: str, priority: int = 1):
        """Add an API key for a service."""
        if api_name not in self.endpoints:
            raise ValueError(f"Unknown API: {api_name}")

        if api_name not in self.api_keys:
            self.api_keys[api_name] = []

        endpoint = self.endpoints[api_name]
        api_key = APIKey(
            key=key,
            name=name,
            quota=endpoint.rate_limit,
            priority=priority
        )

        self.api_keys[api_name].append(api_key)
        self.api_keys[api_name].sort(key=lambda k: k.priority, reverse=True)  # Higher priority first

    def get_best_api_key(self, api_name: str) -> Optional[APIKey]:
        """Get the best available API key for a service."""
        if api_name not in self.api_keys:
            return None

        # Find active keys sorted by priority
        active_keys = [key for key in self.api_keys[api_name] if key.is_active]
        if not active_keys:
            return None

        # Get the highest priority key
        return active_keys[0]

    async def check_quota_and_make_request(
        self,
        api_name: str,
        endpoint: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """Check quota and make API request with rate limiting."""
        if api_name not in self.endpoints:
            raise ValueError(f"Unknown API: {api_name}")

        endpoint_config = self.endpoints[api_name]

        # Get the best API key
        api_key = self.get_best_api_key(api_name)
        if not api_key:
            raise APIQuotaExceeded(f"No active API keys available for {api_name}")

        # Check if we're within rate limits
        await self._check_rate_limit(api_name, api_key)

        # Make the request
        try:
            result = await self._make_request(
                api_name=api_name,
                endpoint=endpoint,
                method=method,
                headers=headers,
                data=data,
                timeout=timeout or endpoint_config.timeout,
                api_key=api_key
            )

            # Update usage statistics
            await self._update_usage_stats(api_name, api_key, success=True)

            return result

        except Exception as e:
            # Update failed request count
            await self._update_usage_stats(api_name, api_key, success=False)
            raise

    async def _check_rate_limit(self, api_name: str, api_key: APIKey):
        """Check if request is within rate limits."""
        now = datetime.now(timezone.utc)
        current_minute = int(now.timestamp() / 60)
        current_hour = int(now.timestamp() / 3600)
        current_day = now.date()

        # Check Redis for current usage
        try:
            # Minute limit
            minute_key = f"quota:{api_name}:{api_key.key}:minute:{current_minute}"
            minute_count = self.redis_client.get(minute_key) or 0

            # Hour limit
            hour_key = f"quota:{api_name}:{api_key.key}:hour:{current_hour}"
            hour_count = self.redis_client.get(hour_key) or 0

            # Day limit
            day_key = f"quota:{api_name}:{api_key.key}:day:{current_day.isoformat()}"
            day_count = self.redis_client.get(day_key) or 0

            # Check limits
            if int(minute_count) >= api_key.quota.requests_per_minute:
                raise APIQuotaExceeded(f"Minute quota exceeded for {api_name}")

            if int(hour_count) >= api_key.quota.requests_per_hour:
                raise APIQuotaExceeded(f"Hour quota exceeded for {api_name}")

            if int(day_count) >= api_key.quota.requests_per_day:
                raise APIQuotaExceeded(f"Day quota exceeded for {api_name}")

        except (ConnectionError, TimeoutError):
            # If Redis is unavailable, allow request but log warning
            logging.warning(f"Redis unavailable, skipping quota check for {api_name}")

    async def _make_request(
        self,
        api_name: str,
        endpoint: str,
        method: str,
        headers: Optional[Dict[str, str]],
        data: Optional[Dict[str, Any]],
        timeout: float,
        api_key: APIKey
    ) -> Dict[str, Any]:
        """Make HTTP request with proper headers and authentication."""
        endpoint_config = self.endpoints[api_name]

        # Prepare headers
        request_headers = headers.copy() if headers else {}
        request_headers.update({
            endpoint_config.auth_header: f"{endpoint_config.auth_prefix} {api_key.key}".strip(),
            "User-Agent": f"Aurum-Data-Platform/1.0 ({api_name})"
        })

        # Simulate API call with telemetry
        with self.telemetry.trace_api_call(
            api_name=api_name,
            endpoint=f"{endpoint_config.base_url}{endpoint}",
            method=method
        ) as span:

            # Simulate network delay and response
            await asyncio.sleep(0.1)  # Simulate network latency

            # Simulate different response patterns based on actual limits
            if api_name == "eia":
                response_status = 200
                response_data = {"response": {"data": []}}
                quota_remaining = 55  # Conservative estimate for EIA
            elif api_name == "fred":
                response_status = 200
                response_data = {"observations": []}
                quota_remaining = 115  # Based on 120/min limit
            elif api_name == "noaa":
                response_status = 200
                response_data = {"features": []}
                quota_remaining = 295  # Based on 300/min limit
            elif api_name == "world_bank":
                response_status = 200
                response_data = {"data": []}
                quota_remaining = 55  # Conservative estimate
            else:
                response_status = 200
                response_data = {}
                quota_remaining = 50

            # Record telemetry
            self.telemetry.record_api_metrics(
                span=span,
                response_status=response_status,
                response_size=len(str(response_data)),
                api_key_used=True,
                quota_exceeded=False
            )

            span.set_attribute("api.quota_remaining", quota_remaining)

            return {
                "status_code": response_status,
                "data": response_data,
                "headers": {"X-RateLimit-Remaining": str(quota_remaining)},
                "quota_remaining": quota_remaining
            }

    async def _update_usage_stats(
        self,
        api_name: str,
        api_key: APIKey,
        success: bool
    ):
        """Update usage statistics for the API key."""
        now = datetime.now(timezone.utc)
        current_minute = int(now.timestamp() / 60)
        current_hour = int(now.timestamp() / 3600)
        current_day = now.date()

        api_key.last_used_at = now
        api_key.total_requests += 1

        if not success:
            api_key.failed_requests += 1

        try:
            # Update Redis counters
            minute_key = f"quota:{api_name}:{api_key.key}:minute:{current_minute}"
            hour_key = f"quota:{api_name}:{api_key.key}:hour:{current_hour}"
            day_key = f"quota:{api_name}:{api_key.key}:day:{current_day.isoformat()}"

            # Use pipelines for atomic operations
            pipe = self.redis_client.pipeline()
            pipe.incr(minute_key)
            pipe.expire(minute_key, 120)  # 2 minutes
            pipe.incr(hour_key)
            pipe.expire(hour_key, 7200)   # 2 hours
            pipe.incr(day_key)
            pipe.expire(day_key, 172800)  # 2 days
            pipe.execute()

        except (ConnectionError, TimeoutError):
            # If Redis is unavailable, log but don't fail
            logging.warning(f"Redis unavailable, quota tracking may be inaccurate for {api_name}")

    def get_quota_status(self, api_name: str) -> Dict[str, Any]:
        """Get current quota status for an API."""
        if api_name not in self.api_keys:
            return {"status": "no_keys"}

        keys = self.api_keys[api_name]
        if not keys:
            return {"status": "no_keys"}

        # Get best key
        best_key = self.get_best_api_key(api_name)
        if not best_key:
            return {"status": "no_active_keys"}

        # Calculate current usage
        now = datetime.now(timezone.utc)
        current_minute = int(now.timestamp() / 60)
        current_hour = int(now.timestamp() / 3600)
        current_day = now.date()

        try:
            minute_usage = int(self.redis_client.get(f"quota:{api_name}:{best_key.key}:minute:{current_minute}") or 0)
            hour_usage = int(self.redis_client.get(f"quota:{api_name}:{best_key.key}:hour:{current_hour}") or 0)
            day_usage = int(self.redis_client.get(f"quota:{api_name}:{best_key.key}:day:{current_day.isoformat()}") or 0)
        except (ConnectionError, TimeoutError):
            minute_usage = hour_usage = day_usage = 0

        return {
            "status": "active",
            "api_key": best_key.name,
            "current_usage": {
                "minute": minute_usage,
                "hour": hour_usage,
                "day": day_usage
            },
            "limits": {
                "minute": best_key.quota.requests_per_minute,
                "hour": best_key.quota.requests_per_hour,
                "day": best_key.quota.requests_per_day
            },
            "remaining": {
                "minute": max(0, best_key.quota.requests_per_minute - minute_usage),
                "hour": max(0, best_key.quota.requests_per_hour - hour_usage),
                "day": max(0, best_key.quota.requests_per_day - day_usage)
            }
        }

    async def _start_monitoring(self):
        """Start background monitoring of API quotas."""
        asyncio.create_task(self._monitor_quotas())

    async def _monitor_quotas(self):
        """Background task to monitor API quota usage."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                for api_name in self.api_keys:
                    status = self.get_quota_status(api_name)

                    if status["status"] == "active":
                        remaining = status["remaining"]

                        # Log warnings for low quotas based on actual limits
                        if api_name == "eia":
                            if remaining["minute"] < 10:
                                logging.warning(f"Low minute quota for {api_name}: {remaining['minute']} remaining")
                        elif api_name == "fred":
                            if remaining["minute"] < 20:
                                logging.warning(f"Low minute quota for {api_name}: {remaining['minute']} remaining")
                        elif api_name == "noaa":
                            if remaining["minute"] < 50:
                                logging.warning(f"Low minute quota for {api_name}: {remaining['minute']} remaining")
                        elif api_name == "world_bank":
                            if remaining["minute"] < 10:
                                logging.warning(f"Low minute quota for {api_name}: {remaining['minute']} remaining")

                        # General hour warnings
                        if remaining["hour"] < 100:
                            logging.warning(f"Low hour quota for {api_name}: {remaining['hour']} remaining")

                        # General day warnings
                        if remaining["day"] < 1000:
                            logging.warning(f"Low day quota for {api_name}: {remaining['day']} remaining")

            except Exception as e:
                logging.error(f"Error in quota monitoring: {e}")

    def get_all_quota_status(self) -> Dict[str, Dict[str, Any]]:
        """Get quota status for all APIs."""
        return {api_name: self.get_quota_status(api_name) for api_name in self.endpoints}


class QuotaAwareRequestScheduler:
    """Schedules API requests with quota awareness."""

    def __init__(self, quota_manager: QuotaManager):
        self.quota_manager = quota_manager
        self.request_queue: asyncio.Queue = asyncio.Queue()
        self.is_processing = False

    async def schedule_request(
        self,
        api_name: str,
        endpoint: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        priority: int = 1
    ) -> Dict[str, Any]:
        """Schedule an API request with quota awareness."""
        request = {
            "api_name": api_name,
            "endpoint": endpoint,
            "method": method,
            "headers": headers,
            "data": data,
            "priority": priority,
            "timestamp": time.time()
        }

        await self.request_queue.put(request)

        # Start processing if not already started
        if not self.is_processing:
            self.is_processing = True
            asyncio.create_task(self._process_requests())

        # Wait for the request to complete
        # In practice, this would use a callback or event system
        # For now, we'll simulate processing
        await asyncio.sleep(0.1)

        return {"status": "queued", "message": "Request queued for processing"}

    async def _process_requests(self):
        """Process queued requests with quota awareness."""
        while True:
            try:
                # Get next request
                request = await self.request_queue.get()

                api_name = request["api_name"]

                # Check quota before making request
                try:
                    result = await self.quota_manager.check_quota_and_make_request(
                        api_name=api_name,
                        endpoint=request["endpoint"],
                        method=request["method"],
                        headers=request["headers"],
                        data=request["data"]
                    )

                    logging.info(f"Successfully processed {api_name} request: {request['endpoint']}")

                except APIQuotaExceeded as e:
                    logging.warning(f"Quota exceeded for {api_name}: {e}")
                    # Re-queue with lower priority or delay
                    request["priority"] = max(0, request["priority"] - 1)
                    await asyncio.sleep(5)  # Wait 5 seconds before retry
                    await self.request_queue.put(request)

                except Exception as e:
                    logging.error(f"Error processing {api_name} request: {e}")
                    # Re-queue with exponential backoff
                    await asyncio.sleep(10)
                    await self.request_queue.put(request)

                # Mark request as done
                self.request_queue.task_done()

            except Exception as e:
                logging.error(f"Error in request processing: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on errors

    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status."""
        return {
            "queue_size": self.request_queue.qsize(),
            "is_processing": self.is_processing
        }


# Global quota manager instance
_quota_manager = None


def get_quota_manager() -> QuotaManager:
    """Get or create the global quota manager."""
    global _quota_manager

    if _quota_manager is None:
        _quota_manager = QuotaManager()

    return _quota_manager


def get_request_scheduler() -> QuotaAwareRequestScheduler:
    """Get the global request scheduler."""
    return QuotaAwareRequestScheduler(get_quota_manager())


# Convenience functions for common APIs
async def call_eia_api(endpoint: str, **kwargs) -> Dict[str, Any]:
    """Make a rate-limited call to the EIA API."""
    quota_manager = get_quota_manager()
    return await quota_manager.check_quota_and_make_request("eia", endpoint, **kwargs)


async def call_fred_api(endpoint: str, **kwargs) -> Dict[str, Any]:
    """Make a rate-limited call to the FRED API."""
    quota_manager = get_quota_manager()
    return await quota_manager.check_quota_and_make_request("fred", endpoint, **kwargs)


async def call_noaa_api(endpoint: str, **kwargs) -> Dict[str, Any]:
    """Make a rate-limited call to the NOAA API."""
    quota_manager = get_quota_manager()
    return await quota_manager.check_quota_and_make_request("noaa", endpoint, **kwargs)


async def call_world_bank_api(endpoint: str, **kwargs) -> Dict[str, Any]:
    """Make a rate-limited call to the World Bank API."""
    quota_manager = get_quota_manager()
    return await quota_manager.check_quota_and_make_request("world_bank", endpoint, **kwargs)


# =====================
# Tenant quota manager
# =====================

class TenantQuotaManager:
    """Enforce per‑tenant RPS, burst and daily caps using Redis.

    Implementation details:
    - RPS/burst uses a 1s sliding window backed by a Redis ZSET where each request
      is added as a unique member with a millisecond score. Old entries are evicted
      on each check. This approximates a token bucket and allows short bursts.
    - Daily caps use a per‑tenant counter key for the UTC day. We increment on every
      allowed request and set the key to expire at the next UTC midnight. If an
      increment would exceed the cap we atomically decrement back and deny.
    """

    def __init__(self, redis_client: Optional[redis.Redis], namespace: str = "quota") -> None:
        self._redis = redis_client
        self._ns = namespace.rstrip(":")

    # ---- Key helpers ----
    def _key_rps(self, tenant_id: str) -> str:
        return f"{self._ns}:tenant:{tenant_id}:rps"

    def _key_day(self, tenant_id: str, day_str: str) -> str:
        return f"{self._ns}:tenant:{tenant_id}:day:{day_str}"

    def _key_cfg(self, tenant_id: str) -> str:
        return f"{self._ns}:tenant:{tenant_id}:config"

    # ---- Public API ----
    def get_tenant_config(self, tenant_id: str, default_rps: int, default_burst: int, default_daily: int) -> Dict[str, int]:
        """Return effective config for a tenant, falling back to provided defaults.

        Values can be overridden by a Redis hash at ``{ns}:tenant:{id}:config`` with fields
        ``rps``, ``burst``, and ``daily_cap``.
        """
        if not self._redis:
            return {"rps": default_rps, "burst": default_burst, "daily_cap": default_daily}
        try:
            values = self._redis.hgetall(self._key_cfg(tenant_id)) or {}
        except Exception:
            values = {}
        rps = int(values.get("rps", default_rps) or default_rps)
        burst = int(values.get("burst", default_burst) or default_burst)
        daily = int(values.get("daily_cap", default_daily) or default_daily)
        return {"rps": rps, "burst": burst, "daily_cap": daily}

    def get_day_usage(self, tenant_id: str, now: Optional[datetime] = None) -> Tuple[int, int]:
        """Return (used_today, seconds_until_reset) for the tenant in UTC."""
        if now is None:
            now = datetime.now(timezone.utc)
        day_str = now.strftime("%Y%m%d")
        if not self._redis:
            # No Redis available: report zero usage and next reset at next midnight
            tomorrow = (now + timedelta(days=1)).date()
            midnight_next = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)
            return 0, int((midnight_next - now).total_seconds())
        try:
            count = int(self._redis.get(self._key_day(tenant_id, day_str)) or 0)
        except Exception:
            count = 0
        # Compute seconds until next UTC midnight
        tomorrow = (now + timedelta(days=1)).date()
        midnight_next = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)
        return count, int((midnight_next - now).total_seconds())

    def get_rps_usage(self, tenant_id: str, window_ms: int = 1000, now_ms: Optional[int] = None) -> int:
        """Return current request count in the 1s sliding window for the tenant."""
        if not self._redis:
            return 0
        now_ms = now_ms or int(time.time() * 1000)
        key = self._key_rps(tenant_id)
        try:
            pipe = self._redis.pipeline()
            pipe.zremrangebyscore(key, 0, now_ms - window_ms)
            pipe.zcard(key)
            _, count = pipe.execute()
            return int(count)
        except Exception:
            return 0

    def check_and_consume(
        self,
        tenant_id: str,
        rps: int,
        burst: int,
        daily_cap: int,
        now: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Check quotas and consume a unit if allowed.

        Returns a dict with keys:
        - allowed: bool
        - reason: "ok" | "rps_exceeded" | "daily_exceeded" | "unavailable"
        - rps_remaining: int, rps_reset: int (seconds)
        - daily_remaining: int, daily_reset: int (seconds)
        """
        if not self._redis:
            return {
                "allowed": True,
                "reason": "unavailable",
                "rps_remaining": max(0, rps + burst - 1),
                "rps_reset": 1,
                "daily_remaining": max(0, daily_cap - 1),
                "daily_reset": 60,
            }

        now = now or datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        window_ms = 1000
        key_rps = self._key_rps(tenant_id)
        # Sliding window check
        try:
            member = f"{now_ms}:{tenant_id}:{uuid.uuid4().hex}"
            pipe = self._redis.pipeline()
            pipe.zremrangebyscore(key_rps, 0, now_ms - window_ms)
            pipe.zadd(key_rps, {member: now_ms})
            pipe.zcard(key_rps)
            pipe.zrange(key_rps, 0, 0, withscores=True)
            pipe.pexpire(key_rps, window_ms)
            _, _, current_count, earliest, _ = pipe.execute()
            current = int(current_count)
            limit_total = int(rps + burst)
            rps_allowed = current <= limit_total
            # Compute reset seconds for window
            oldest_score = None
            if earliest:
                try:
                    oldest_score = float(earliest[0][1])
                except (ValueError, TypeError, IndexError):
                    oldest_score = None
            rps_reset_seconds = 1
            if oldest_score is not None:
                reset_window = ((oldest_score + window_ms) - now_ms) / 1000.0
                rps_reset_seconds = max(0, math.ceil(reset_window))
            rps_remaining = max(0, limit_total - current)
        except Exception:
            # Fail open but report unavailable
            return {
                "allowed": True,
                "reason": "unavailable",
                "rps_remaining": max(0, rps + burst - 1),
                "rps_reset": 1,
                "daily_remaining": max(0, daily_cap - 1),
                "daily_reset": 60,
            }

        if not rps_allowed:
            used_today, day_reset = self.get_day_usage(tenant_id, now)
            return {
                "allowed": False,
                "reason": "rps_exceeded",
                "rps_remaining": rps_remaining,
                "rps_reset": rps_reset_seconds,
                "daily_remaining": max(0, daily_cap - used_today),
                "daily_reset": day_reset,
            }

        # Daily cap enforcement with atomic increment and optional rollback
        day_str = now.strftime("%Y%m%d")
        key_day = self._key_day(tenant_id, day_str)
        try:
            new_value = int(self._redis.incr(key_day))
            if new_value == 1:
                # First request today: expire at next UTC midnight
                tomorrow = (now + timedelta(days=1)).date()
                midnight_next = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc)
                self._redis.expireat(key_day, int(midnight_next.timestamp()))
            if new_value > daily_cap:
                # Roll back and deny
                try:
                    self._redis.decr(key_day)
                except Exception:
                    pass
                used_today = new_value - 1
                _, day_reset = self.get_day_usage(tenant_id, now)
                return {
                    "allowed": False,
                    "reason": "daily_exceeded",
                    "rps_remaining": rps_remaining,
                    "rps_reset": rps_reset_seconds,
                    "daily_remaining": max(0, daily_cap - used_today),
                    "daily_reset": day_reset,
                }
        except Exception:
            # Fail open on Redis issues, but report unavailable
            return {
                "allowed": True,
                "reason": "unavailable",
                "rps_remaining": rps_remaining,
                "rps_reset": rps_reset_seconds,
                "daily_remaining": max(0, daily_cap - 1),
                "daily_reset": 60,
            }

        # Allowed and accounted for
        used_today, day_reset = self.get_day_usage(tenant_id, now)
        return {
            "allowed": True,
            "reason": "ok",
            "rps_remaining": rps_remaining,
            "rps_reset": rps_reset_seconds,
            "daily_remaining": max(0, daily_cap - used_today),
            "daily_reset": day_reset,
        }
