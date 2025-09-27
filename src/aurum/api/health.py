"""Health check and monitoring endpoints."""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from fastapi import APIRouter, HTTPException

from ..telemetry.context import get_request_id
from .config import CacheConfig, TrinoConfig
from .health_checks import (
    check_redis_ready,
    check_schema_registry_ready,
    check_timescale_ready,
    check_trino_deep_health,
)
from .state import get_settings


class HealthCheckLevel(Enum):
    """Health check levels for different endpoints."""
    LIVENESS = "liveness"    # Basic "is the service running" checks
    READINESS = "readiness"  # Comprehensive dependency checks


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    component: str
    status: str
    details: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    
    def is_healthy(self) -> bool:
        """Check if this component is healthy."""
        return self.status.lower() in {"healthy", "disabled", "ready", "ok", "alive"}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        result = {"status": self.status}
        if self.details:
            result.update(self.details) 
        if self.error:
            result["error"] = self.error
        return result


@dataclass 
class HealthCheckSuite:
    """Collection of health check results."""
    level: HealthCheckLevel
    checks: List[HealthCheckResult] = field(default_factory=list)
    request_id: Optional[str] = None
    
    def add_check(self, result: HealthCheckResult) -> None:
        """Add a health check result."""
        self.checks.append(result)
    
    def is_healthy(self) -> bool:
        """Check if all components are healthy."""
        return all(check.is_healthy() for check in self.checks)
    
    def get_status(self) -> str:
        """Get overall status."""
        if self.is_healthy():
            return "ready" if self.level == HealthCheckLevel.READINESS else "ok"
        return "unavailable"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        result = {
            "status": self.get_status(),
            "request_id": self.request_id or get_request_id(),
        }
        
        if self.checks:
            result["checks"] = {
                check.component: check.to_dict() 
                for check in self.checks
            }
        
        return result


class HealthCheckService:
    """Unified health check service for all endpoints."""
    
    @staticmethod
    async def run_liveness_checks() -> HealthCheckSuite:
        """Run basic liveness checks."""
        suite = HealthCheckSuite(level=HealthCheckLevel.LIVENESS)
        
        # Basic liveness - just check that the service is responding
        suite.add_check(HealthCheckResult(
            component="api",
            status="ok"
        ))
        
        return suite
    
    @staticmethod
    async def run_readiness_checks() -> HealthCheckSuite:
        """Run comprehensive readiness checks."""
        suite = HealthCheckSuite(level=HealthCheckLevel.READINESS)
        
        settings = get_settings()
        trino_cfg = TrinoConfig.from_settings(settings)
        cache_cfg = CacheConfig.from_settings(settings)
        schema_registry_url = getattr(settings.kafka, "schema_registry_url", None)

        # Run all checks concurrently
        trino_future = asyncio.create_task(check_trino_deep_health(trino_cfg))
        schema_future = asyncio.create_task(check_schema_registry_ready(schema_registry_url))
        timescale_future = asyncio.to_thread(check_timescale_ready, settings.database.timescale_dsn)
        redis_future = asyncio.to_thread(check_redis_ready, cache_cfg)

        trino_status, schema_status, timescale_status, redis_status = await asyncio.gather(
            trino_future,
            schema_future,
            timescale_future,
            redis_future,
            return_exceptions=True,
        )
        
        # Process results and add to suite
        suite.add_check(HealthCheckService._process_check_result("trino", trino_status))
        suite.add_check(HealthCheckService._process_check_result("schema_registry", schema_status))  
        suite.add_check(HealthCheckService._process_check_result("timescale", timescale_status))
        suite.add_check(HealthCheckService._process_check_result("redis", redis_status))
        
        return suite
    
    @staticmethod
    def _process_check_result(component: str, result: Any) -> HealthCheckResult:
        """Process a raw check result into a HealthCheckResult."""
        if isinstance(result, Exception):
            return HealthCheckResult(
                component=component,
                status="unavailable", 
                error=str(result)
            )
        
        if isinstance(result, bool):
            return HealthCheckResult(
                component=component,
                status="healthy" if result else "unavailable"
            )
        
        if isinstance(result, dict):
            status = result.get("status", "unknown")
            details = {k: v for k, v in result.items() if k != "status"}
            error = result.get("error")
            
            return HealthCheckResult(
                component=component,
                status=status,
                details=details if details else None,
                error=error
            )
        
        # Fallback for unknown result types
        return HealthCheckResult(
            component=component,
            status="unknown",
            details={"raw_result": str(result)}
        )


router = APIRouter()


@router.get("/health")
@router.get("/healthz") 
@router.get("/health/live")
async def health_check() -> Dict[str, Any]:
    """Basic liveness probe."""
    suite = await HealthCheckService.run_liveness_checks()
    return suite.to_dict()


@router.get("/live")
@router.get("/livez")
async def live_check() -> Dict[str, Any]:
    """Legacy alias for liveness probes."""
    suite = await HealthCheckService.run_liveness_checks() 
    result = suite.to_dict()
    # Keep legacy status format for backward compatibility
    result["status"] = "alive" if suite.is_healthy() else "unavailable"
    return result


@router.get("/ready")
@router.get("/readyz")
async def readiness_check() -> Dict[str, Any]:
    """Comprehensive readiness probe covering core dependencies."""
    suite = await HealthCheckService.run_readiness_checks()
    
    if suite.is_healthy():
        return suite.to_dict()
    
    # Return 503 for unhealthy state
    raise HTTPException(
        status_code=503,
        detail=suite.to_dict(),
    )


__all__ = [
    "router",
    "health_check",
    "live_check",
    "readiness_check",
]
