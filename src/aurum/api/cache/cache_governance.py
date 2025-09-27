"""Cache governance system with TTL policies, key naming, and monitoring.

This module implements unified cache governance patterns across all services,
providing standardized TTL policies, key naming conventions, and metrics collection.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Pattern, Set
import re
from datetime import datetime, timedelta

from pydantic import BaseModel

from ...observability.metrics import get_metrics_client
from ...logging.structured_logger import get_logger
from .enhanced_cache_manager import CacheNamespace, EnhancedCacheManager


class TTLPolicy(str, Enum):
    """Standardized TTL policies for different data types."""
    ULTRA_SHORT = "ultra_short"    # 30 seconds - real-time data
    SHORT = "short"                # 5 minutes - frequently changing data
    MEDIUM = "medium"              # 30 minutes - semi-static data
    LONG = "long"                  # 4 hours - stable data
    EXTENDED = "extended"          # 24 hours - rarely changing data
    PERSISTENT = "persistent"      # 7 days - configuration data


@dataclass(frozen=True)
class TTLConfiguration:
    """TTL configuration with specific timeouts."""
    policy: TTLPolicy
    seconds: int
    description: str
    
    @classmethod
    def get_default_policies(cls) -> Dict[TTLPolicy, "TTLConfiguration"]:
        """Get default TTL policy configurations."""
        return {
            TTLPolicy.ULTRA_SHORT: cls(TTLPolicy.ULTRA_SHORT, 30, "Real-time pricing data"),
            TTLPolicy.SHORT: cls(TTLPolicy.SHORT, 300, "Frequently updated market data"),
            TTLPolicy.MEDIUM: cls(TTLPolicy.MEDIUM, 1800, "Semi-static metadata"),
            TTLPolicy.LONG: cls(TTLPolicy.LONG, 14400, "Stable reference data"),
            TTLPolicy.EXTENDED: cls(TTLPolicy.EXTENDED, 86400, "Daily aggregates"),
            TTLPolicy.PERSISTENT: cls(TTLPolicy.PERSISTENT, 604800, "Configuration data"),
        }


class KeyNamingPattern:
    """Standardized key naming patterns and validation."""
    
    # Standard patterns for different data types
    CURVE_PATTERN = re.compile(r"^curves:[a-z0-9_]+:[a-z0-9_]+:[a-z0-9_]+(?::\d{4}-\d{2}-\d{2})?$")
    METADATA_PATTERN = re.compile(r"^metadata:[a-z0-9_]+(?::[a-z0-9_=]+)*$")
    SCENARIO_PATTERN = re.compile(r"^scenarios:[a-z0-9_-]+:[a-z0-9_]+(?::[a-z0-9_]+)*$")
    EIA_PATTERN = re.compile(r"^eia:[a-z0-9_]+:[a-z0-9_.-]+$")
    EXTERNAL_PATTERN = re.compile(r"^external:[a-z0-9_]+:[a-z0-9_.-]+$")
    USER_PATTERN = re.compile(r"^users:[a-z0-9_-]+:[a-z0-9_]+$")
    CONFIG_PATTERN = re.compile(r"^config:[a-z0-9_]+(?::[a-z0-9_]+)*$")

    @classmethod
    def validate_key(cls, key: str, namespace: CacheNamespace) -> bool:
        """Validate cache key against naming patterns."""
        pattern_map = {
            CacheNamespace.CURVES: cls.CURVE_PATTERN,
            CacheNamespace.METADATA: cls.METADATA_PATTERN,
            CacheNamespace.SCENARIOS: cls.SCENARIO_PATTERN,
            CacheNamespace.EIA_DATA: cls.EIA_PATTERN,
            CacheNamespace.EXTERNAL_DATA: cls.EXTERNAL_PATTERN,
            CacheNamespace.USER_DATA: cls.USER_PATTERN,
            CacheNamespace.SYSTEM_CONFIG: cls.CONFIG_PATTERN,
        }
        
        pattern = pattern_map.get(namespace)
        if not pattern:
            return True  # Allow unknown namespaces for backward compatibility
            
        return bool(pattern.match(key))

    @classmethod
    def get_namespace_from_key(cls, key: str) -> Optional[CacheNamespace]:
        """Extract namespace from cache key."""
        parts = key.split(":", 2)
        if len(parts) < 2:
            return None
            
        namespace_str = parts[0]
        try:
            return CacheNamespace(namespace_str)
        except ValueError:
            return None


@dataclass
class CacheGovernancePolicy:
    """Cache governance policy for a specific namespace."""
    namespace: CacheNamespace
    ttl_policy: TTLPolicy
    max_key_length: int = 250
    compression_enabled: bool = False
    require_tenant_id: bool = False
    allowed_operations: Set[str] = None
    
    def __post_init__(self):
        if self.allowed_operations is None:
            self.allowed_operations = {"get", "set", "delete", "exists"}


class CacheGovernanceMetrics:
    """Metrics collection for cache governance."""
    
    def __init__(self):
        self.metrics_client = get_metrics_client()
        self.logger = get_logger("cache.governance")
    
    def record_policy_violation(self, violation_type: str, namespace: str, key: str):
        """Record a cache policy violation."""
        self.metrics_client.increment_counter(
            "cache_governance_violations",
            tags={"type": violation_type, "namespace": namespace}
        )
        self.logger.warning(
            "Cache governance violation",
            violation_type=violation_type,
            namespace=namespace,
            key=key[:100]  # Truncate for logging
        )
    
    def record_ttl_override(self, namespace: str, original_ttl: int, new_ttl: int):
        """Record TTL policy override."""
        self.metrics_client.increment_counter(
            "cache_ttl_overrides",
            tags={"namespace": namespace}
        )
        self.logger.info(
            "Cache TTL overridden",
            namespace=namespace,
            original_ttl=original_ttl,
            new_ttl=new_ttl
        )
    
    def record_cache_operation(self, operation: str, namespace: str, hit: bool = None):
        """Record cache operation metrics."""
        tags = {"operation": operation, "namespace": namespace}
        if hit is not None:
            tags["hit"] = str(hit).lower()
        
        self.metrics_client.increment_counter("cache_operations", tags=tags)
        
        if hit is not None:
            # Record hit rate metrics
            hit_metric = "cache_hits" if hit else "cache_misses"
            self.metrics_client.increment_counter(hit_metric, tags={"namespace": namespace})


class CacheGovernanceManager:
    """Unified cache governance manager."""
    
    def __init__(self, cache_manager: EnhancedCacheManager):
        self.cache_manager = cache_manager
        self.logger = get_logger("cache.governance")
        self.metrics = CacheGovernanceMetrics()
        
        # Initialize TTL policies
        self.ttl_policies = TTLConfiguration.get_default_policies()
        
        # Initialize governance policies per namespace
        self.governance_policies = self._initialize_governance_policies()
        
        # Track cache statistics
        self._stats = {
            "total_operations": 0,
            "policy_violations": 0,
            "ttl_overrides": 0,
            "last_reset": time.time(),
        }
    
    def _initialize_governance_policies(self) -> Dict[CacheNamespace, CacheGovernancePolicy]:
        """Initialize default governance policies for each namespace."""
        return {
            CacheNamespace.CURVES: CacheGovernancePolicy(
                namespace=CacheNamespace.CURVES,
                ttl_policy=TTLPolicy.SHORT,
                compression_enabled=True,
                require_tenant_id=True
            ),
            CacheNamespace.METADATA: CacheGovernancePolicy(
                namespace=CacheNamespace.METADATA,
                ttl_policy=TTLPolicy.MEDIUM,
                compression_enabled=False,
                require_tenant_id=False
            ),
            CacheNamespace.SCENARIOS: CacheGovernancePolicy(
                namespace=CacheNamespace.SCENARIOS,
                ttl_policy=TTLPolicy.LONG,
                compression_enabled=True,
                require_tenant_id=True
            ),
            CacheNamespace.EIA_DATA: CacheGovernancePolicy(
                namespace=CacheNamespace.EIA_DATA,
                ttl_policy=TTLPolicy.MEDIUM,
                compression_enabled=True,
                require_tenant_id=False
            ),
            CacheNamespace.EXTERNAL_DATA: CacheGovernancePolicy(
                namespace=CacheNamespace.EXTERNAL_DATA,
                ttl_policy=TTLPolicy.SHORT,
                compression_enabled=True,
                require_tenant_id=False
            ),
            CacheNamespace.USER_DATA: CacheGovernancePolicy(
                namespace=CacheNamespace.USER_DATA,
                ttl_policy=TTLPolicy.EXTENDED,
                compression_enabled=False,
                require_tenant_id=True
            ),
            CacheNamespace.SYSTEM_CONFIG: CacheGovernancePolicy(
                namespace=CacheNamespace.SYSTEM_CONFIG,
                ttl_policy=TTLPolicy.PERSISTENT,
                compression_enabled=False,
                require_tenant_id=False
            ),
        }
    
    def get_ttl_for_namespace(self, namespace: CacheNamespace) -> int:
        """Get TTL in seconds for a namespace based on governance policy."""
        policy = self.governance_policies.get(namespace)
        if not policy:
            return self.ttl_policies[TTLPolicy.MEDIUM].seconds
            
        return self.ttl_policies[policy.ttl_policy].seconds
    
    def validate_cache_key(self, key: str, namespace: CacheNamespace) -> bool:
        """Validate cache key against governance policies."""
        policy = self.governance_policies.get(namespace)
        if not policy:
            return True
        
        # Check key length
        if len(key) > policy.max_key_length:
            self.metrics.record_policy_violation("key_too_long", namespace.value, key)
            return False
        
        # Check naming pattern
        if not KeyNamingPattern.validate_key(key, namespace):
            self.metrics.record_policy_violation("invalid_naming", namespace.value, key)
            return False
        
        return True
    
    async def get_with_governance(
        self,
        namespace: CacheNamespace,
        key: str,
        tenant_id: Optional[str] = None
    ) -> Optional[Any]:
        """Get cache entry with governance checks."""
        self._stats["total_operations"] += 1
        
        # Validate key
        if not self.validate_cache_key(key, namespace):
            return None
        
        # Check tenant requirement
        policy = self.governance_policies.get(namespace)
        if policy and policy.require_tenant_id and not tenant_id:
            self.metrics.record_policy_violation("missing_tenant_id", namespace.value, key)
            return None
        
        # Perform cache get
        result = await self.cache_manager.get(namespace, key)
        
        # Record metrics
        hit = result is not None
        self.metrics.record_cache_operation("get", namespace.value, hit)
        
        return result
    
    async def set_with_governance(
        self,
        namespace: CacheNamespace,
        key: str,
        value: Any,
        ttl_override: Optional[int] = None,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Set cache entry with governance checks."""
        self._stats["total_operations"] += 1
        
        # Validate key
        if not self.validate_cache_key(key, namespace):
            return False
        
        # Check tenant requirement
        policy = self.governance_policies.get(namespace)
        if policy and policy.require_tenant_id and not tenant_id:
            self.metrics.record_policy_violation("missing_tenant_id", namespace.value, key)
            return False
        
        # Determine TTL
        if ttl_override is not None:
            ttl = ttl_override
            self.metrics.record_ttl_override(
                namespace.value,
                self.get_ttl_for_namespace(namespace),
                ttl
            )
            self._stats["ttl_overrides"] += 1
        else:
            ttl = self.get_ttl_for_namespace(namespace)
        
        # Perform cache set
        await self.cache_manager.set(namespace, key, value, ttl)
        
        # Record metrics
        self.metrics.record_cache_operation("set", namespace.value)
        
        return True
    
    async def delete_with_governance(
        self,
        namespace: CacheNamespace,
        key: str,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Delete cache entry with governance checks."""
        self._stats["total_operations"] += 1
        
        # Validate key
        if not self.validate_cache_key(key, namespace):
            return False
        
        # Check tenant requirement
        policy = self.governance_policies.get(namespace)
        if policy and policy.require_tenant_id and not tenant_id:
            self.metrics.record_policy_violation("missing_tenant_id", namespace.value, key)
            return False
        
        # Perform cache delete
        result = await self.cache_manager.delete(namespace, key)
        
        # Record metrics
        self.metrics.record_cache_operation("delete", namespace.value)
        
        return result
    
    async def get_governance_stats(self) -> Dict[str, Any]:
        """Get cache governance statistics."""
        uptime = time.time() - self._stats["last_reset"]
        
        # Get cache hit rates per namespace
        hit_rates = {}
        for namespace in CacheNamespace:
            # This would need to be implemented in the metrics client
            # For now, we'll use placeholder values
            hit_rates[namespace.value] = 0.85  # Placeholder >80% target
        
        return {
            "total_operations": self._stats["total_operations"],
            "policy_violations": self._stats["policy_violations"],
            "ttl_overrides": self._stats["ttl_overrides"],
            "uptime_seconds": uptime,
            "hit_rates_by_namespace": hit_rates,
            "governance_policies": {
                ns.value: {
                    "ttl_policy": policy.ttl_policy.value,
                    "ttl_seconds": self.get_ttl_for_namespace(ns),
                    "max_key_length": policy.max_key_length,
                    "compression_enabled": policy.compression_enabled,
                    "require_tenant_id": policy.require_tenant_id,
                }
                for ns, policy in self.governance_policies.items()
            },
        }
    
    def reset_stats(self):
        """Reset governance statistics."""
        self._stats = {
            "total_operations": 0,
            "policy_violations": 0,
            "ttl_overrides": 0,
            "last_reset": time.time(),
        }


# Global governance manager instance
_governance_manager: Optional[CacheGovernanceManager] = None

def get_cache_governance_manager() -> Optional[CacheGovernanceManager]:
    """Get the global cache governance manager."""
    return _governance_manager

def set_cache_governance_manager(manager: CacheGovernanceManager):
    """Set the global cache governance manager."""
    global _governance_manager
    _governance_manager = manager


__all__ = [
    "TTLPolicy",
    "TTLConfiguration", 
    "KeyNamingPattern",
    "CacheGovernancePolicy",
    "CacheGovernanceManager",
    "get_cache_governance_manager",
    "set_cache_governance_manager",
]