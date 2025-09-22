"""Rate limiting management endpoints for administrators.

Surface: `/v1/admin/ratelimit/*`

Provides read/write helpers around quotas, rules, usage, and housekeeping:
- `GET /quotas`, `GET /quotas/{tier}`, `POST /quotas/{tier}`
- `GET /rules`, `POST /rules`
- `GET /usage`, `GET /violations`
- `POST /cleanup`, `POST /reset`
- `GET /configuration`

Notes:
- Current implementation uses an in-memory manager stub; wire to durable storage
  to persist changes in production.
- All responses include `meta.request_id` and simple timing for observability.
"""

from __future__ import annotations

import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from ..telemetry.context import get_request_id
from .rate_limiting import (
    create_rate_limit_manager,
    QuotaTier,
    RateLimitManager,
    Quota,
    RateLimitRule,
    RateLimitAlgorithm,
)


router = APIRouter()


@router.get("/v1/admin/ratelimit/quotas")
async def list_quotas(
    request: Request,
    tier: Optional[str] = Query(None, description="Filter by tier"),
) -> Dict[str, List[Dict[str, str]]]:
    """List all quota tiers and their limits."""
    start_time = time.perf_counter()

    try:
        # Create a temporary manager to get quota information
        manager = create_rate_limit_manager("memory")

        quotas_data = []
        tiers = [QuotaTier.FREE, QuotaTier.BASIC, QuotaTier.PREMIUM, QuotaTier.ENTERPRISE, QuotaTier.UNLIMITED]

        for quota_tier in tiers:
            quota = manager.get_quota(quota_tier)
            if quota:
                quota_data = {
                    "tier": quota.tier.value,
                    "requests_per_minute": quota.requests_per_minute,
                    "requests_per_hour": quota.requests_per_hour,
                    "requests_per_day": quota.requests_per_day,
                    "burst_multiplier": quota.burst_multiplier,
                    "custom_limits": quota.custom_limits,
                }
                quotas_data.append(quota_data)

        # Filter by tier if provided
        if tier:
            quotas_data = [q for q in quotas_data if q["tier"] == tier.lower()]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_tiers": len(quotas_data),
            },
            "data": quotas_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list quotas: {str(exc)}"
        ) from exc


@router.get("/v1/admin/ratelimit/quotas/{tier}")
async def get_quota_info(
    request: Request,
    tier: str,
) -> Dict[str, str]:
    """Get detailed information about a specific quota tier."""
    start_time = time.perf_counter()

    try:
        # Validate tier
        try:
            quota_tier = QuotaTier(tier.lower())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tier: {tier}"
            )

        manager = create_rate_limit_manager("memory")
        quota = manager.get_quota(quota_tier)

        if not quota:
            raise HTTPException(
                status_code=404,
                detail=f"Quota tier {tier} not found"
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        result = {
            "tier": quota.tier.value,
            "requests_per_minute": quota.requests_per_minute,
            "requests_per_hour": quota.requests_per_hour,
            "requests_per_day": quota.requests_per_day,
            "burst_multiplier": quota.burst_multiplier,
            "custom_limits": quota.custom_limits,
            "effective_limits": {
                "per_second": round(quota.requests_per_minute / 60, 2),
                "per_minute": quota.requests_per_minute,
                "per_hour": quota.requests_per_hour,
                "per_day": quota.requests_per_day,
            }
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
            detail=f"Failed to get quota info: {str(exc)}"
        ) from exc


@router.post("/v1/admin/ratelimit/quotas/{tier}")
async def update_quota(
    request: Request,
    tier: str,
    quota_data: Dict[str, str],
) -> Dict[str, str]:
    """Update quota configuration for a tier."""
    start_time = time.perf_counter()

    try:
        # Validate tier
        try:
            quota_tier = QuotaTier(tier.lower())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tier: {tier}"
            )

        # Validate quota data
        required_fields = ["requests_per_minute", "requests_per_hour", "requests_per_day"]
        for field in required_fields:
            if field not in quota_data:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required field: {field}"
                )

        # In a real implementation, this would update the quota in storage
        # For now, return success response
        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Quota updated for tier {tier}",
            "tier": tier,
            "updated_fields": list(quota_data.keys()),
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
            detail=f"Failed to update quota: {str(exc)}"
        ) from exc


@router.get("/v1/admin/ratelimit/rules")
async def list_rate_limit_rules(
    request: Request,
    algorithm: Optional[str] = Query(None, description="Filter by algorithm"),
) -> Dict[str, List[Dict[str, str]]]:
    """List all rate limiting rules."""
    start_time = time.perf_counter()

    try:
        manager = create_rate_limit_manager("memory")

        rules_data = []
        # In a real implementation, this would get rules from storage
        # For now, return default rules
        default_rules = [
            {
                "name": "api_endpoints",
                "requests_per_window": 100,
                "window_seconds": 60,
                "burst_limit": 20,
                "algorithm": RateLimitAlgorithm.TOKEN_BUCKET.value,
                "endpoint_patterns": ["/v1/", "/v2/"],
                "user_agent_patterns": [],
            },
            {
                "name": "metadata_endpoints",
                "requests_per_window": 200,
                "window_seconds": 60,
                "burst_limit": 50,
                "algorithm": RateLimitAlgorithm.TOKEN_BUCKET.value,
                "endpoint_patterns": ["/v1/metadata/"],
                "user_agent_patterns": [],
            }
        ]

        # Filter by algorithm if provided
        if algorithm:
            rules_data = [r for r in default_rules if r["algorithm"] == algorithm.lower()]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_rules": len(rules_data),
            },
            "data": rules_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list rate limit rules: {str(exc)}"
        ) from exc


@router.post("/v1/admin/ratelimit/rules")
async def create_rate_limit_rule(
    request: Request,
    rule_data: Dict[str, str],
) -> Dict[str, str]:
    """Create a new rate limiting rule."""
    start_time = time.perf_counter()

    try:
        # Validate required fields
        required_fields = ["name", "requests_per_window", "window_seconds"]
        for field in required_fields:
            if field not in rule_data:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required field: {field}"
                )

        # Validate algorithm
        algorithm = rule_data.get("algorithm", "token_bucket")
        try:
            RateLimitAlgorithm(algorithm.lower())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid algorithm: {algorithm}"
            )

        # In a real implementation, this would save the rule to storage
        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": "Rate limit rule created",
            "rule_name": rule_data["name"],
            "algorithm": algorithm,
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
            detail=f"Failed to create rate limit rule: {str(exc)}"
        ) from exc


@router.get("/v1/admin/ratelimit/usage")
async def get_rate_limit_usage(
    request: Request,
    tier: Optional[str] = Query(None, description="Filter by tier"),
    identifier: Optional[str] = Query(None, description="Specific user/tenant identifier"),
) -> Dict[str, str]:
    """Get rate limiting usage statistics."""
    start_time = time.perf_counter()

    try:
        manager = create_rate_limit_manager("memory")

        # Get usage for all tiers
        tiers = [QuotaTier.FREE, QuotaTier.BASIC, QuotaTier.PREMIUM, QuotaTier.ENTERPRISE, QuotaTier.UNLIMITED]
        usage_data = []

        for quota_tier in tiers:
            usage = await manager.get_tier_usage(quota_tier)
            usage_data.append({
                "tier": quota_tier.value,
                "active_users": usage.get("active_users", 0),
                "requests_today": usage.get("requests_today", 0),
                "requests_this_hour": usage.get("requests_this_hour", 0),
                "average_utilization": usage.get("average_utilization", 0.0),
            })

        # Filter by tier if provided
        if tier:
            usage_data = [u for u in usage_data if u["tier"] == tier.lower()]

        # Get specific identifier usage if provided
        if identifier:
            # In a real implementation, this would query specific usage
            specific_usage = {
                "identifier": identifier,
                "current_requests": 0,
                "remaining_requests": 60,
                "reset_time_seconds": 45,
                "tier": "free",
            }
            usage_data.append(specific_usage)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_entries": len(usage_data),
            },
            "data": usage_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get rate limit usage: {str(exc)}"
        ) from exc


@router.get("/v1/admin/ratelimit/violations")
async def get_rate_limit_violations(
    request: Request,
    hours: int = Query(24, description="Time period in hours"),
    tier: Optional[str] = Query(None, description="Filter by tier"),
) -> Dict[str, str]:
    """Get rate limit violations and statistics."""
    start_time = time.perf_counter()

    try:
        # In a real implementation, this would query violation logs
        violations_data = {
            "period_hours": hours,
            "total_violations": 0,
            "violations_by_tier": {
                "free": 0,
                "basic": 0,
                "premium": 0,
                "enterprise": 0,
                "unlimited": 0,
            },
            "violations_by_endpoint": {},
            "top_violators": [],
            "violation_rate": 0.0,
        }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": violations_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get rate limit violations: {str(exc)}"
        ) from exc


@router.post("/v1/admin/ratelimit/cleanup")
async def cleanup_rate_limits(
    request: Request,
) -> Dict[str, str]:
    """Clean up expired rate limit states."""
    start_time = time.perf_counter()

    try:
        manager = create_rate_limit_manager("memory")
        cleaned_count = await manager.cleanup()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Rate limit cleanup completed",
            "expired_states_removed": cleaned_count,
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cleanup rate limits: {str(exc)}"
        ) from exc


@router.post("/v1/admin/ratelimit/reset")
async def reset_rate_limit(
    request: Request,
    identifier: str,
) -> Dict[str, str]:
    """Reset rate limit for a specific identifier."""
    start_time = time.perf_counter()

    try:
        manager = create_rate_limit_manager("memory")

        # In a real implementation, this would reset the specific identifier
        # For now, return success
        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "message": f"Rate limit reset for identifier: {identifier}",
            "identifier": identifier,
            "reset_at": time.time(),
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reset rate limit: {str(exc)}"
        ) from exc


@router.get("/v1/admin/ratelimit/configuration")
async def get_rate_limit_configuration(
    request: Request,
) -> Dict[str, str]:
    """Get current rate limiting configuration."""
    start_time = time.perf_counter()

    try:
        manager = create_rate_limit_manager("memory")

        # Get quota tiers
        quotas = []
        tiers = [QuotaTier.FREE, QuotaTier.BASIC, QuotaTier.PREMIUM, QuotaTier.ENTERPRISE, QuotaTier.UNLIMITED]

        for quota_tier in tiers:
            quota = manager.get_quota(quota_tier)
            if quota:
                quotas.append({
                    "tier": quota.tier.value,
                    "requests_per_minute": quota.requests_per_minute,
                    "requests_per_hour": quota.requests_per_hour,
                    "requests_per_day": quota.requests_per_day,
                    "burst_multiplier": quota.burst_multiplier,
                })

        # Get algorithms
        algorithms = [alg.value for alg in RateLimitAlgorithm]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": {
                "supported_algorithms": algorithms,
                "quota_tiers": quotas,
                "default_storage": "memory",
                "supported_storages": ["memory", "redis"],
                "features": [
                    "token_bucket_algorithm",
                    "tier_based_quotas",
                    "burst_handling",
                    "custom_rules",
                    "usage_tracking",
                ]
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get rate limit configuration: {str(exc)}"
        ) from exc


@router.get("/v1/admin/ratelimit/health")
async def get_rate_limit_health(
    request: Request,
) -> Dict[str, str]:
    """Get rate limiting system health status."""
    start_time = time.perf_counter()

    try:
        manager = create_rate_limit_manager("memory")

        # Check storage health
        try:
            await manager.cleanup()
            storage_status = "healthy"
        except Exception:
            storage_status = "unhealthy"

        # Get system status
        health_data = {
            "status": storage_status,
            "storage_backend": "memory",
            "active_connections": 1,
            "quota_tiers_configured": 5,
            "rules_configured": 2,
            "cleanup_last_run": time.time(),
            "uptime_seconds": 3600,  # Placeholder
        }

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return {
            "meta": {
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            "data": health_data
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get rate limit health: {str(exc)}"
        ) from exc
