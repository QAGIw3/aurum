"""Unified rate limiting system consolidating all rate limiting implementations.

This module provides a single entry point for rate limiting with:
- Multiple algorithm support (token bucket, sliding window, fixed window)
- Tenant-aware resource controls
- API governance controls
- Unified configuration and monitoring
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from pathlib import Path

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger
from .tenant_quota_manager import TenantQuotaManager, QuotaLimit, TenantQuotaConfig
from .sliding_window import RateLimitMiddleware
from ..telemetry.context import extract_tenant_id_from_headers, get_request_id


class RateLimitAlgorithmType(str, Enum):
    """Available rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window" 
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


class RateLimitScope(str, Enum):  
    """Rate limit enforcement scope."""
    GLOBAL = "global"
    TENANT = "tenant"
    IP = "ip"
    ENDPOINT = "endpoint"
    USER = "user"


@dataclass
class RateLimitPolicy:
    """Unified rate limit policy configuration."""
    name: str
    algorithm: RateLimitAlgorithmType = RateLimitAlgorithmType.TOKEN_BUCKET
    scope: RateLimitScope = RateLimitScope.GLOBAL
    requests_per_second: int = 10
    burst_size: int = 20
    window_seconds: int = 60
    enabled: bool = True
    
    # Endpoint matching
    endpoint_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    
    # Priority and enforcement
    priority: int = 100  # Higher priority = applied first
    strict_enforcement: bool = True
    
    # Resource controls
    max_concurrency: Optional[int] = None
    queue_timeout_seconds: Optional[float] = None


@dataclass
class RateLimitResult:
    """Result of rate limit check."""
    allowed: bool
    retry_after: Optional[float] = None
    remaining: Optional[int] = None
    reset_time: Optional[float] = None
    policy_name: Optional[str] = None
    algorithm_used: Optional[str] = None


class RateLimitAlgorithm(ABC):
    """Abstract base for rate limiting algorithms."""
    
    @abstractmethod
    async def check_limit(
        self, 
        key: str, 
        policy: RateLimitPolicy,
        current_time: Optional[float] = None
    ) -> RateLimitResult:
        """Check if request is within rate limit."""
        pass
    
    @abstractmethod
    async def reset_limits(self, key: str) -> bool:
        """Reset rate limits for a key."""
        pass


class TokenBucketAlgorithm(RateLimitAlgorithm):
    """Token bucket rate limiting algorithm."""
    
    def __init__(self):
        self._buckets: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    async def check_limit(
        self, 
        key: str, 
        policy: RateLimitPolicy,
        current_time: Optional[float] = None
    ) -> RateLimitResult:
        """Check token bucket rate limit."""
        if current_time is None:
            current_time = time.time()
        
        async with self._lock:
            bucket = self._buckets.get(key)
            
            if bucket is None:
                # Initialize new bucket
                bucket = {
                    'tokens': policy.burst_size,
                    'last_refill': current_time,
                    'capacity': policy.burst_size,
                    'refill_rate': policy.requests_per_second
                }
                self._buckets[key] = bucket
            
            # Refill tokens based on time elapsed
            time_elapsed = current_time - bucket['last_refill']
            tokens_to_add = time_elapsed * bucket['refill_rate']
            bucket['tokens'] = min(bucket['capacity'], bucket['tokens'] + tokens_to_add)
            bucket['last_refill'] = current_time
            
            # Check if request can be allowed
            if bucket['tokens'] >= 1.0:
                bucket['tokens'] -= 1.0
                return RateLimitResult(
                    allowed=True,
                    remaining=int(bucket['tokens']),
                    reset_time=current_time + (bucket['capacity'] - bucket['tokens']) / bucket['refill_rate'],
                    policy_name=policy.name,
                    algorithm_used="token_bucket"
                )
            else:
                # Calculate retry after time
                retry_after = (1.0 - bucket['tokens']) / bucket['refill_rate']
                return RateLimitResult(
                    allowed=False,
                    retry_after=retry_after,
                    remaining=0,
                    reset_time=current_time + retry_after,
                    policy_name=policy.name,
                    algorithm_used="token_bucket"
                )
    
    async def reset_limits(self, key: str) -> bool:
        """Reset token bucket for a key."""
        async with self._lock:
            if key in self._buckets:
                del self._buckets[key]
                return True
            return False


class SlidingWindowAlgorithm(RateLimitAlgorithm):
    """Sliding window rate limiting algorithm."""
    
    def __init__(self):
        self._windows: Dict[str, List[float]] = {}
        self._lock = asyncio.Lock()
    
    async def check_limit(
        self, 
        key: str, 
        policy: RateLimitPolicy,
        current_time: Optional[float] = None
    ) -> RateLimitResult:
        """Check sliding window rate limit."""
        if current_time is None:
            current_time = time.time()
        
        async with self._lock:
            window = self._windows.get(key, [])
            window_start = current_time - policy.window_seconds
            
            # Remove expired timestamps
            window = [t for t in window if t > window_start]
            
            if len(window) < policy.requests_per_second * policy.window_seconds / 60:
                # Allow request
                window.append(current_time)
                self._windows[key] = window
                
                remaining = int(policy.requests_per_second * policy.window_seconds / 60) - len(window)
                return RateLimitResult(
                    allowed=True,
                    remaining=remaining,
                    reset_time=current_time + policy.window_seconds,
                    policy_name=policy.name,
                    algorithm_used="sliding_window"
                )
            else:
                # Calculate retry after
                oldest_request = min(window) if window else current_time
                retry_after = oldest_request + policy.window_seconds - current_time
                
                return RateLimitResult(
                    allowed=False,
                    retry_after=max(0, retry_after),
                    remaining=0,
                    reset_time=oldest_request + policy.window_seconds,
                    policy_name=policy.name,
                    algorithm_used="sliding_window"
                )
    
    async def reset_limits(self, key: str) -> bool:
        """Reset sliding window for a key."""
        async with self._lock:
            if key in self._windows:
                del self._windows[key]
                return True
            return False


class UnifiedRateLimiter:
    """Unified rate limiter consolidating all rate limiting implementations."""
    
    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or Path("config/rate_limit_policies.json")
        self.logger = get_logger("rate_limiter.unified")
        self.metrics = get_metrics_client()
        
        # Algorithm implementations
        self.algorithms = {
            RateLimitAlgorithmType.TOKEN_BUCKET: TokenBucketAlgorithm(),
            RateLimitAlgorithmType.SLIDING_WINDOW: SlidingWindowAlgorithm(),
        }
        
        # Rate limit policies
        self.policies: List[RateLimitPolicy] = []
        self.default_policy = RateLimitPolicy(
            name="default",
            requests_per_second=10,
            burst_size=20
        )
        
        # Tenant quota manager for tenant-aware controls
        self.tenant_manager = TenantQuotaManager(config_path)
        
        # Statistics
        self._stats = {
            "requests_processed": 0,
            "requests_allowed": 0, 
            "requests_denied": 0,
            "policies_applied": 0,
            "algorithms_used": {},
        }
    
    async def initialize(self):
        """Initialize the unified rate limiter."""
        await self._load_policies()
        await self.tenant_manager.initialize()
        self.logger.info("Unified rate limiter initialized", policies=len(self.policies))
    
    async def close(self):
        """Close the unified rate limiter."""
        await self.tenant_manager.close()
    
    async def _load_policies(self):
        """Load rate limiting policies from configuration."""
        try:
            if self.config_path.exists():
                import json
                with open(self.config_path) as f:
                    config = json.load(f)
                
                self.policies = []
                for policy_config in config.get("policies", []):
                    policy = RateLimitPolicy(**policy_config)
                    self.policies.append(policy)
                
                # Sort by priority (higher first)
                self.policies.sort(key=lambda p: p.priority, reverse=True)
            else:
                # Use default policy
                self.policies = [self.default_policy]
                
        except Exception as e:
            self.logger.warning("Failed to load rate limit policies", error=str(e))
            self.policies = [self.default_policy]
    
    def _match_policy(self, request: Request, tenant_id: Optional[str] = None) -> RateLimitPolicy:
        """Find the matching rate limit policy for a request."""
        path = request.url.path
        
        for policy in self.policies:
            if not policy.enabled:
                continue
                
            # Check endpoint patterns
            if policy.endpoint_patterns:
                if not any(pattern in path for pattern in policy.endpoint_patterns):
                    continue
            
            # Check exclude patterns
            if policy.exclude_patterns:
                if any(pattern in path for pattern in policy.exclude_patterns):
                    continue
            
            return policy
        
        return self.default_policy
    
    def _generate_rate_limit_key(
        self, 
        request: Request, 
        policy: RateLimitPolicy, 
        tenant_id: Optional[str] = None
    ) -> str:
        """Generate rate limiting key based on scope."""
        if policy.scope == RateLimitScope.GLOBAL:
            return f"global:{policy.name}"
        elif policy.scope == RateLimitScope.TENANT and tenant_id:
            return f"tenant:{tenant_id}:{policy.name}"
        elif policy.scope == RateLimitScope.IP:
            client_ip = request.client.host if request.client else "unknown"
            return f"ip:{client_ip}:{policy.name}"
        elif policy.scope == RateLimitScope.ENDPOINT:
            return f"endpoint:{request.url.path}:{policy.name}"
        else:
            # Fallback to global
            return f"global:{policy.name}"
    
    async def check_rate_limit(
        self, 
        request: Request, 
        tenant_id: Optional[str] = None
    ) -> RateLimitResult:
        """Check if request is within rate limits."""
        self._stats["requests_processed"] += 1
        
        # Find matching policy
        policy = self._match_policy(request, tenant_id)
        
        # Check tenant-specific quotas first if tenant_id is provided
        if tenant_id and policy.scope in [RateLimitScope.TENANT, RateLimitScope.GLOBAL]:
            try:
                tenant_allowed, retry_after = await self.tenant_manager.check_and_increment(tenant_id)
                if not tenant_allowed:
                    self._stats["requests_denied"] += 1
                    self.metrics.increment_counter(
                        "rate_limit_decisions",
                        tags={"result": "denied", "policy": "tenant_quota", "tenant": tenant_id}
                    )
                    return RateLimitResult(
                        allowed=False,
                        retry_after=retry_after,
                        policy_name="tenant_quota",
                        algorithm_used="tenant_quota"
                    )
            except Exception as e:
                self.logger.warning("Tenant quota check failed", tenant_id=tenant_id, error=str(e))
        
        # Apply rate limiting algorithm
        algorithm = self.algorithms.get(policy.algorithm)
        if not algorithm:
            self.logger.warning("Unknown algorithm", algorithm=policy.algorithm)
            algorithm = self.algorithms[RateLimitAlgorithmType.TOKEN_BUCKET]
        
        key = self._generate_rate_limit_key(request, policy, tenant_id)
        result = await algorithm.check_limit(key, policy)
        
        # Update statistics
        if result.allowed:
            self._stats["requests_allowed"] += 1
        else:
            self._stats["requests_denied"] += 1
            
        self._stats["policies_applied"] += 1
        self._stats["algorithms_used"][policy.algorithm.value] = (
            self._stats["algorithms_used"].get(policy.algorithm.value, 0) + 1
        )
        
        # Record metrics
        self.metrics.increment_counter(
            "rate_limit_decisions",
            tags={
                "result": "allowed" if result.allowed else "denied",
                "policy": policy.name,
                "algorithm": policy.algorithm.value,
                "scope": policy.scope.value,
                "tenant": tenant_id or "none"
            }
        )
        
        return result
    
    async def reset_rate_limits(self, key_pattern: str) -> int:
        """Reset rate limits matching a pattern."""
        reset_count = 0
        for algorithm in self.algorithms.values():
            # This would need pattern matching implementation
            # For now, reset specific key
            if await algorithm.reset_limits(key_pattern):
                reset_count += 1
        return reset_count
    
    async def get_rate_limit_stats(self) -> Dict[str, Any]:
        """Get rate limiting statistics."""
        return {
            "requests_processed": self._stats["requests_processed"],
            "requests_allowed": self._stats["requests_allowed"], 
            "requests_denied": self._stats["requests_denied"],
            "policies_applied": self._stats["policies_applied"],
            "algorithms_used": self._stats["algorithms_used"],
            "policies_configured": len(self.policies),
            "tenant_quotas_active": len(self.tenant_manager._usage),
            "success_rate": (
                self._stats["requests_allowed"] / max(1, self._stats["requests_processed"])
            ) * 100,
        }
    
    def add_policy(self, policy: RateLimitPolicy):
        """Add a new rate limiting policy."""
        self.policies.append(policy)
        self.policies.sort(key=lambda p: p.priority, reverse=True)
        self.logger.info("Added rate limiting policy", policy=policy.name)
    
    def remove_policy(self, policy_name: str) -> bool:
        """Remove a rate limiting policy."""
        original_count = len(self.policies)
        self.policies = [p for p in self.policies if p.name != policy_name]
        removed = len(self.policies) < original_count
        if removed:
            self.logger.info("Removed rate limiting policy", policy=policy_name)
        return removed


# Global unified rate limiter instance
_unified_rate_limiter: Optional[UnifiedRateLimiter] = None

def get_unified_rate_limiter() -> Optional[UnifiedRateLimiter]:
    """Get the global unified rate limiter."""
    return _unified_rate_limiter

def set_unified_rate_limiter(rate_limiter: UnifiedRateLimiter):
    """Set the global unified rate limiter."""
    global _unified_rate_limiter
    _unified_rate_limiter = rate_limiter


__all__ = [
    "RateLimitAlgorithmType",
    "RateLimitScope", 
    "RateLimitPolicy",
    "RateLimitResult",
    "UnifiedRateLimiter",
    "get_unified_rate_limiter",
    "set_unified_rate_limiter",
]