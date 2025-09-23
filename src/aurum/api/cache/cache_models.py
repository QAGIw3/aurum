"""Pydantic models for cache analytics and management."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from .models import AurumBaseModel


class NamespaceAnalytics(AurumBaseModel):
    """Analytics data for a cache namespace."""

    namespace: str = Field(..., description="Namespace name")
    hits: int = Field(..., description="Cache hits")
    misses: int = Field(..., description="Cache misses")
    hit_rate: float = Field(..., description="Hit rate percentage")
    total_requests: int = Field(..., description="Total requests")
    average_response_time: float = Field(..., description="Average response time in seconds")
    memory_usage_bytes: int = Field(..., description="Memory usage in bytes")


class CacheAnalyticsData(AurumBaseModel):
    """Cache analytics data."""

    namespaces: List[NamespaceAnalytics] = Field(..., description="Per-namespace analytics")
    summary: Dict[str, Any] = Field(..., description="Summary statistics")
    ttl_overrides: Dict[str, int] = Field(
        default_factory=dict,
        description="Per-query TTL overrides keyed by query hash",
    )


class CacheAnalyticsResponse(AurumBaseModel):
    """Response model for cache analytics."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: CacheAnalyticsData = Field(..., description="Analytics data")


class CacheStatsData(AurumBaseModel):
    """Cache statistics data."""

    warming_status: str = Field(..., description="Cache warming status")
    strategy: str = Field(..., description="Cache strategy")
    namespaces: Dict[str, Any] = Field(..., description="Namespace statistics")
    distributed_nodes: int = Field(..., description="Number of distributed nodes")
    warmup_tasks: int = Field(..., description="Number of warmup tasks")
    distributed_info: Optional[Dict[str, Any]] = Field(None, description="Distributed cache info")


class CacheStatsResponse(AurumBaseModel):
    """Response model for cache statistics."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: CacheStatsData = Field(..., description="Statistics data")


class CacheWarmingStatusData(AurumBaseModel):
    """Cache warming service status."""

    warming_status: str = Field(..., description="Current warming status")
    active_tasks: int = Field(..., description="Number of active tasks")
    service_running: bool = Field(..., description="Whether service is running")


class CacheWarmingStatusResponse(AurumBaseModel):
    """Response model for cache warming status."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: CacheWarmingStatusData = Field(..., description="Status data")


class CacheStrategyRequest(AurumBaseModel):
    """Request model for setting cache strategy."""

    strategy: str = Field(..., description="Cache strategy (lru, lfu, ttl, adaptive)")
    namespace: Optional[str] = Field(None, description="Namespace to apply strategy to")


class CacheWarmingTaskRequest(AurumBaseModel):
    """Request model for cache warming tasks."""

    name: str = Field(..., description="Task name")
    key_pattern: str = Field(..., description="Key pattern to warm")
    schedule_interval: int = Field(300, description="Schedule interval in seconds")
    priority: int = Field(1, description="Task priority")
    enabled: bool = Field(True, description="Whether task is enabled")


class CacheRecommendationsData(AurumBaseModel):
    """Cache performance recommendations."""

    recommendations: List[str] = Field(..., description="List of recommendations")
    total_recommendations: int = Field(..., description="Total number of recommendations")


class CacheRecommendationsResponse(AurumBaseModel):
    """Response model for cache recommendations."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: CacheRecommendationsData = Field(..., description="Recommendations data")


class CacheHealthData(AurumBaseModel):
    """Cache health status."""

    status: str = Field(..., description="Overall health status")
    issues: List[str] = Field(..., description="List of health issues")


class CacheHealthResponse(AurumBaseModel):
    """Response model for cache health."""

    meta: Dict[str, Any] = Field(..., description="Response metadata")
    data: CacheHealthData = Field(..., description="Health data")


class CachePurgeRequest(AurumBaseModel):
    """Request payload for targeted cache invalidation."""

    pattern: Optional[str] = Field(None, description="Named golden query pattern to invalidate")
    query: Optional[str] = Field(None, description="Exact SQL query to invalidate from the cache")
    tenant_id: Optional[str] = Field(None, description="Tenant scope for the invalidation")

    @model_validator(mode="after")
    def _ensure_target(cls, values: "CachePurgeRequest") -> "CachePurgeRequest":
        if not values.pattern and not values.query:
            raise ValueError("Provide either a pattern or query to purge")
        return values


class CacheTTLOverrideRequest(AurumBaseModel):
    """Request payload for managing per-query TTL overrides."""

    query: str = Field(..., description="Exact SQL query to override the TTL for")
    ttl_seconds: Optional[int] = Field(
        None,
        ge=1,
        le=604800,
        description="TTL override in seconds (1 second to 7 days)",
    )
    clear: bool = Field(False, description="Remove the override instead of setting it")

    @model_validator(mode="after")
    def _validate_state(cls, values: "CacheTTLOverrideRequest") -> "CacheTTLOverrideRequest":
        if not values.clear and values.ttl_seconds is None:
            raise ValueError("ttl_seconds is required when clear is false")
        return values


# Legacy models for backward compatibility
CacheAnalytics = CacheAnalyticsResponse
CacheStats = CacheStatsResponse
CacheWarmingStatus = CacheWarmingStatusResponse
CacheRecommendations = CacheRecommendationsResponse
CacheHealth = CacheHealthResponse
