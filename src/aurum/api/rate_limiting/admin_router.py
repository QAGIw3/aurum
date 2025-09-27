"""Admin router for rate limiting management.

Provides REST endpoints for:
- Viewing rate limiting rules and statistics
- Creating, updating, and deleting rules
- Monitoring rate limiting performance
- Managing adaptive limits
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Depends
from pydantic import BaseModel, Field

from ..telemetry.context import get_request_id
from .consolidated_policy_engine import (
    ConsolidatedRateLimiter,
    RateLimitRule,
    RateLimitStats,
    RateLimitResult,
    RateLimitAlgorithmType,
    RateLimitScope
)


router = APIRouter(prefix="/v1/admin/rate-limiting", tags=["rate-limiting-admin"])


class CreateRuleRequest(BaseModel):
    """Request to create a new rate limiting rule."""

    name: str = Field(..., description="Rule name")
    algorithm: RateLimitAlgorithmType
    scope: RateLimitScope
    limit: int = Field(..., gt=0, description="Requests per window")
    window_seconds: int = Field(..., gt=0, description="Window size in seconds")
    burst_limit: Optional[int] = Field(None, gt=0, description="Burst limit")
    priority: int = Field(100, ge=1, description="Rule priority (lower = higher priority)")
    enabled: bool = True
    conditions: Dict[str, str] = Field(default_factory=dict)
    metadata: Dict[str, str] = Field(default_factory=dict)


class UpdateRuleRequest(BaseModel):
    """Request to update an existing rate limiting rule."""

    algorithm: Optional[RateLimitAlgorithmType] = None
    scope: Optional[RateLimitScope] = None
    limit: Optional[int] = Field(None, gt=0)
    window_seconds: Optional[int] = Field(None, gt=0)
    burst_limit: Optional[int] = Field(None, gt=0)
    priority: Optional[int] = Field(None, ge=1)
    enabled: Optional[bool] = None
    conditions: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, str]] = None


class RuleResponse(BaseModel):
    """Response containing rule information."""

    name: str
    algorithm: RateLimitAlgorithmType
    scope: RateLimitScope
    limit: int
    window_seconds: int
    burst_limit: Optional[int]
    priority: int
    enabled: bool
    conditions: Dict[str, str]
    metadata: Dict[str, str]
    created_at: datetime
    updated_at: datetime


class StatsResponse(BaseModel):
    """Response containing rate limiting statistics."""

    total_requests: int
    allowed_requests: int
    blocked_requests: int
    success_rate: float
    avg_response_time: float
    error_count: int
    last_reset: datetime


class HealthResponse(BaseModel):
    """Response containing rate limiting health information."""

    is_healthy: bool
    backends_connected: List[str]
    redis_available: bool
    memory_usage_mb: Optional[float]
    error_rate: float
    last_health_check: Optional[datetime]


# Global rate limiter instance (would be injected in real app)
_rate_limiter: Optional[ConsolidatedRateLimiter] = None


def get_rate_limiter() -> ConsolidatedRateLimiter:
    """Get the global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        from .consolidated_policy_engine import get_unified_rate_limiter
        _rate_limiter = get_unified_rate_limiter()
    return _rate_limiter


@router.get("/rules", response_model=List[RuleResponse])
async def list_rules(
    scope: Optional[RateLimitScope] = Query(None, description="Filter by scope"),
    enabled: Optional[bool] = Query(None, description="Filter by enabled status")
) -> List[RuleResponse]:
    """List all rate limiting rules with optional filtering.

    Args:
        scope: Filter by rate limit scope
        enabled: Filter by enabled status

    Returns:
        List of rate limiting rules
    """
    rate_limiter = get_rate_limiter()

    rules = list(rate_limiter.rules.values())

    if scope:
        rules = [r for r in rules if r.scope == scope]

    if enabled is not None:
        rules = [r for r in rules if r.enabled == enabled]

    # Convert to response format
    responses = []
    for rule in rules:
        response = RuleResponse(
            name=rule.name,
            algorithm=rule.algorithm,
            scope=rule.scope,
            limit=rule.limit,
            window_seconds=rule.window_seconds,
            burst_limit=rule.burst_limit,
            priority=rule.priority,
            enabled=rule.enabled,
            conditions=rule.conditions,
            metadata=rule.metadata,
            created_at=datetime.utcnow(),  # Would track in real implementation
            updated_at=datetime.utcnow()
        )
        responses.append(response)

    return responses


@router.post("/rules", response_model=RuleResponse, status_code=201)
async def create_rule(request: CreateRuleRequest) -> RuleResponse:
    """Create a new rate limiting rule.

    Args:
        request: Rule creation request

    Returns:
        Created rule information
    """
    rate_limiter = get_rate_limiter()

    # Check if rule already exists
    if request.name in rate_limiter.rules:
        raise HTTPException(
            status_code=409,
            detail=f"Rule '{request.name}' already exists"
        )

    # Create new rule
    rule = RateLimitRule(
        name=request.name,
        algorithm=request.algorithm,
        scope=request.scope,
        limit=request.limit,
        window_seconds=request.window_seconds,
        burst_limit=request.burst_limit,
        priority=request.priority,
        enabled=request.enabled,
        conditions=request.conditions,
        metadata=request.metadata
    )

    # Add to rate limiter
    rate_limiter.add_rule(rule)

    # Return response
    return RuleResponse(
        name=rule.name,
        algorithm=rule.algorithm,
        scope=rule.scope,
        limit=rule.limit,
        window_seconds=rule.window_seconds,
        burst_limit=rule.burst_limit,
        priority=rule.priority,
        enabled=rule.enabled,
        conditions=rule.conditions,
        metadata=rule.metadata,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@router.get("/rules/{rule_name}", response_model=RuleResponse)
async def get_rule(rule_name: str) -> RuleResponse:
    """Get a specific rate limiting rule.

    Args:
        rule_name: Name of the rule

    Returns:
        Rule information
    """
    rate_limiter = get_rate_limiter()

    if rule_name not in rate_limiter.rules:
        raise HTTPException(
            status_code=404,
            detail=f"Rule '{rule_name}' not found"
        )

    rule = rate_limiter.rules[rule_name]

    return RuleResponse(
        name=rule.name,
        algorithm=rule.algorithm,
        scope=rule.scope,
        limit=rule.limit,
        window_seconds=rule.window_seconds,
        burst_limit=rule.burst_limit,
        priority=rule.priority,
        enabled=rule.enabled,
        conditions=rule.conditions,
        metadata=rule.metadata,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@router.put("/rules/{rule_name}", response_model=RuleResponse)
async def update_rule(rule_name: str, request: UpdateRuleRequest) -> RuleResponse:
    """Update an existing rate limiting rule.

    Args:
        rule_name: Name of the rule to update
        request: Update request

    Returns:
        Updated rule information
    """
    rate_limiter = get_rate_limiter()

    if rule_name not in rate_limiter.rules:
        raise HTTPException(
            status_code=404,
            detail=f"Rule '{rule_name}' not found"
        )

    rule = rate_limiter.rules[rule_name]

    # Update fields
    if request.algorithm is not None:
        rule.algorithm = request.algorithm
    if request.scope is not None:
        rule.scope = request.scope
    if request.limit is not None:
        rule.limit = request.limit
    if request.window_seconds is not None:
        rule.window_seconds = request.window_seconds
    if request.burst_limit is not None:
        rule.burst_limit = request.burst_limit
    if request.priority is not None:
        rule.priority = request.priority
    if request.enabled is not None:
        rule.enabled = request.enabled
    if request.conditions is not None:
        rule.conditions = request.conditions
    if request.metadata is not None:
        rule.metadata = request.metadata

    # Recreate algorithm with updated rule
    algorithm_class = rate_limiter._get_algorithm_class(rule.algorithm)
    rate_limiter.algorithms[rule_name] = algorithm_class(rule, rate_limiter.redis_client)

    return RuleResponse(
        name=rule.name,
        algorithm=rule.algorithm,
        scope=rule.scope,
        limit=rule.limit,
        window_seconds=rule.window_seconds,
        burst_limit=rule.burst_limit,
        priority=rule.priority,
        enabled=rule.enabled,
        conditions=rule.conditions,
        metadata=rule.metadata,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )


@router.delete("/rules/{rule_name}")
async def delete_rule(rule_name: str) -> dict:
    """Delete a rate limiting rule.

    Args:
        rule_name: Name of the rule to delete

    Returns:
        Deletion confirmation
    """
    rate_limiter = get_rate_limiter()

    if rule_name not in rate_limiter.rules:
        raise HTTPException(
            status_code=404,
            detail=f"Rule '{rule_name}' not found"
        )

    # Remove from all data structures
    del rate_limiter.rules[rule_name]
    del rate_limiter.algorithms[rule_name]

    # Remove from scope index
    for scope_rules in rate_limiter.scope_rules.values():
        if rule_name in scope_rules:
            scope_rules.remove(rule_name)

    return {"message": f"Rule '{rule_name}' deleted successfully"}


@router.get("/stats", response_model=StatsResponse)
async def get_stats() -> StatsResponse:
    """Get rate limiting statistics.

    Returns:
        Current statistics
    """
    rate_limiter = get_rate_limiter()
    stats = await rate_limiter.get_stats()

    return StatsResponse(
        total_requests=stats.total_requests,
        allowed_requests=stats.allowed_requests,
        blocked_requests=stats.blocked_requests,
        success_rate=stats.success_rate,
        avg_response_time=stats.avg_response_time,
        error_count=stats.error_count,
        last_reset=stats.last_reset
    )


@router.post("/stats/reset")
async def reset_stats() -> dict:
    """Reset rate limiting statistics.

    Returns:
        Reset confirmation
    """
    rate_limiter = get_rate_limiter()
    await rate_limiter.reset_stats()

    return {"message": "Statistics reset successfully"}


@router.get("/health", response_model=HealthResponse)
async def get_health() -> HealthResponse:
    """Get rate limiting health information.

    Returns:
        Health status
    """
    rate_limiter = get_rate_limiter()
    health = await rate_limiter.get_health()

    # Check backend connectivity
    backends_connected = []
    if rate_limiter.redis_client:
        try:
            await rate_limiter.redis_client.ping()
            backends_connected.append("redis")
        except Exception:
            pass

    backends_connected.append("memory")

    return HealthResponse(
        is_healthy=health.is_healthy,
        backends_connected=backends_connected,
        redis_available="redis" in backends_connected,
        memory_usage_mb=health.memory_usage_mb,
        error_rate=health.error_rate,
        last_health_check=health.last_health_check
    )


@router.get("/test", response_model=Dict[str, int])
async def test_rate_limits(
    request: Request,
    endpoint: Optional[str] = Query(None, description="Endpoint to test")
) -> Dict[str, int]:
    """Test rate limits for the current request.

    This endpoint can be used to test rate limiting behavior
    without making actual API calls.

    Args:
        request: Current request
        endpoint: Optional endpoint identifier

    Returns:
        Remaining limits for each rule
    """
    rate_limiter = get_rate_limiter()
    remaining = await rate_limiter.get_remaining_limits(request, endpoint)

    return remaining


@router.get("/scopes")
async def get_scopes() -> Dict[str, List[str]]:
    """Get available rate limiting scopes and their rules.

    Returns:
        Dictionary mapping scopes to rule names
    """
    rate_limiter = get_rate_limiter()

    return {
        scope.value: rule_names
        for scope, rule_names in rate_limiter.scope_rules.items()
    }
