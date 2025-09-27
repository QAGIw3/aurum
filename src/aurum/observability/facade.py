"""Observability façade pattern providing unified interface to all observability components.

This module implements a façade pattern over the existing observability modules,
providing a single, simplified interface for metrics, logging, tracing, and SLO monitoring.
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional, Union, Generator
from datetime import datetime, timedelta

from .metrics import get_metrics_client, MetricType
from .logging import get_logger
from .tracing import get_trace_collector
from .slo_monitor import SLOMonitor, SLOStatus, SLOType
from .alert_manager import AlertManager
from ..telemetry.context import get_request_id, get_tenant_id


class LogLevel(str, Enum):
    """Unified log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class SeverityLevel(str, Enum):
    """Severity levels for alerts and incidents."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ObservabilityContext:
    """Context for observability operations."""
    request_id: Optional[str] = None
    tenant_id: Optional[str] = None
    operation: Optional[str] = None
    service: str = "aurum"
    component: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    start_time: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for logging/tracing."""
        return {
            "request_id": self.request_id,
            "tenant_id": self.tenant_id,
            "operation": self.operation,
            "service": self.service,
            "component": self.component,
            "tags": self.tags,
            "duration": time.time() - self.start_time if self.start_time else None,
        }


@dataclass
class SLODefinition:
    """Service Level Objective definition."""
    name: str
    slo_type: SLOType
    target: float  # e.g., 0.995 for 99.5% availability
    window_minutes: int = 60
    description: str = ""
    alert_threshold: float = 0.9  # Alert when SLO compliance drops below this
    
    def __post_init__(self):
        if not self.description:
            self.description = f"{self.slo_type.value} SLO: {self.target * 100}%"


class ObservabilityFacade:
    """Unified observability façade providing simplified interface."""
    
    def __init__(self):
        self.logger = get_logger("observability.facade")
        self.metrics_client = get_metrics_client()
        self.trace_collector = get_trace_collector()
        self.slo_monitor = SLOMonitor()
        self.alert_manager = AlertManager()
        
        # Core SLO definitions
        self.core_slos = {
            "availability": SLODefinition(
                name="system_availability",
                slo_type=SLOType.AVAILABILITY,
                target=0.995,  # 99.5% uptime
                window_minutes=60,
                description="System availability SLO"
            ),
            "latency_p95": SLODefinition(
                name="response_latency_p95", 
                slo_type=SLOType.LATENCY,
                target=2.0,  # 2 seconds P95 latency
                window_minutes=15,
                description="95th percentile response latency SLO"
            ),
            "error_rate": SLODefinition(
                name="error_rate",
                slo_type=SLOType.ERROR_RATE,
                target=0.01,  # 1% error rate
                window_minutes=30,
                description="Error rate SLO"
            ),
            "mttd": SLODefinition(
                name="mean_time_to_detection",
                slo_type=SLOType.FRESHNESS,
                target=300,  # 5 minutes
                window_minutes=60,
                description="Mean time to detection SLO"
            ),
        }
        
        self._context_stack: List[ObservabilityContext] = []
        self._initialized = False
    
    async def initialize(self):
        """Initialize the observability façade."""
        if self._initialized:
            return
            
        try:
            await self.slo_monitor.initialize()
            await self.alert_manager.initialize()
            
            # Register core SLOs
            for slo in self.core_slos.values():
                await self.slo_monitor.register_slo(slo)
            
            self._initialized = True
            self.logger.info("Observability façade initialized", slos=len(self.core_slos))
            
        except Exception as e:
            self.logger.error("Failed to initialize observability façade", error=str(e))
            raise
    
    async def close(self):
        """Close the observability façade."""
        if not self._initialized:
            return
            
        try:
            await self.slo_monitor.close()
            await self.alert_manager.close()
            self._initialized = False
            self.logger.info("Observability façade closed")
            
        except Exception as e:
            self.logger.error("Error closing observability façade", error=str(e))
    
    # Context Management
    
    def create_context(
        self,
        operation: str,
        component: Optional[str] = None,
        **tags
    ) -> ObservabilityContext:
        """Create observability context."""
        return ObservabilityContext(
            request_id=get_request_id(),
            tenant_id=get_tenant_id(),
            operation=operation,
            component=component,
            tags=tags,
            start_time=time.time()
        )
    
    @contextmanager
    def context(
        self,
        operation: str,
        component: Optional[str] = None,
        **tags
    ) -> Generator[ObservabilityContext, None, None]:
        """Context manager for observability operations."""
        ctx = self.create_context(operation, component, **tags)
        self._context_stack.append(ctx)
        
        try:
            yield ctx
        finally:
            self._context_stack.pop()
    
    @asynccontextmanager
    async def async_context(
        self,
        operation: str,
        component: Optional[str] = None,
        **tags
    ) -> AsyncGenerator[ObservabilityContext, None]:
        """Async context manager for observability operations."""
        ctx = self.create_context(operation, component, **tags)
        self._context_stack.append(ctx)
        
        try:
            yield ctx
        finally:
            self._context_stack.pop()
    
    # Logging Interface
    
    def log(
        self,
        level: LogLevel,
        message: str,
        context: Optional[ObservabilityContext] = None,
        **extra
    ):
        """Unified logging interface."""
        ctx = context or (self._context_stack[-1] if self._context_stack else None)
        
        log_data = extra.copy()
        if ctx:
            log_data.update(ctx.to_dict())
        
        # Get appropriate logger
        component_logger = (
            get_logger(f"{ctx.component}") if ctx and ctx.component
            else self.logger
        )
        
        # Log with appropriate level
        log_method = getattr(component_logger, level.value)
        log_method(message, **log_data)
    
    def debug(self, message: str, **extra):
        """Debug logging."""
        self.log(LogLevel.DEBUG, message, **extra)
    
    def info(self, message: str, **extra):
        """Info logging."""
        self.log(LogLevel.INFO, message, **extra)
    
    def warning(self, message: str, **extra):
        """Warning logging."""
        self.log(LogLevel.WARNING, message, **extra)
    
    def error(self, message: str, **extra):
        """Error logging."""
        self.log(LogLevel.ERROR, message, **extra)
    
    def critical(self, message: str, **extra):
        """Critical logging."""
        self.log(LogLevel.CRITICAL, message, **extra)
    
    # Metrics Interface
    
    def increment_counter(
        self,
        name: str,
        value: float = 1.0,
        context: Optional[ObservabilityContext] = None,
        **tags
    ):
        """Increment a counter metric."""
        ctx = context or (self._context_stack[-1] if self._context_stack else None)
        all_tags = tags.copy()
        
        if ctx:
            all_tags.update(ctx.tags)
            if ctx.component:
                all_tags["component"] = ctx.component
            if ctx.tenant_id:
                all_tags["tenant_id"] = ctx.tenant_id
        
        self.metrics_client.increment_counter(name, value, tags=all_tags)
    
    def set_gauge(
        self,
        name: str,
        value: float,
        context: Optional[ObservabilityContext] = None,
        **tags
    ):
        """Set a gauge metric."""
        ctx = context or (self._context_stack[-1] if self._context_stack else None)
        all_tags = tags.copy()
        
        if ctx:
            all_tags.update(ctx.tags)
            if ctx.component:
                all_tags["component"] = ctx.component
            if ctx.tenant_id:
                all_tags["tenant_id"] = ctx.tenant_id
        
        self.metrics_client.set_gauge(name, value, tags=all_tags)
    
    def record_histogram(
        self,
        name: str,
        value: float,
        context: Optional[ObservabilityContext] = None,
        **tags
    ):
        """Record a histogram metric."""
        ctx = context or (self._context_stack[-1] if self._context_stack else None)
        all_tags = tags.copy()
        
        if ctx:
            all_tags.update(ctx.tags)
            if ctx.component:
                all_tags["component"] = ctx.component
            if ctx.tenant_id:
                all_tags["tenant_id"] = ctx.tenant_id
        
        self.metrics_client.record_histogram(name, value, tags=all_tags)
    
    def time_operation(
        self,
        name: str,
        context: Optional[ObservabilityContext] = None,
        **tags
    ):
        """Context manager to time an operation."""
        @contextmanager
        def timer():
            start_time = time.time()
            try:
                yield
            finally:
                duration = time.time() - start_time
                self.record_histogram(f"{name}_duration_seconds", duration, context, **tags)
        
        return timer()
    
    # SLO Interface
    
    async def check_slo_compliance(self, slo_name: str) -> SLOStatus:
        """Check SLO compliance status."""
        if slo_name in self.core_slos:
            return await self.slo_monitor.check_slo_status(slo_name)
        return SLOStatus.UNKNOWN
    
    async def record_slo_event(
        self,
        slo_name: str,
        success: bool,
        value: Optional[float] = None,
        context: Optional[ObservabilityContext] = None
    ):
        """Record an event for SLO calculation."""
        ctx = context or (self._context_stack[-1] if self._context_stack else None)
        
        await self.slo_monitor.record_event(slo_name, success, value)
        
        # Log SLO event
        self.info(
            f"SLO event recorded",
            slo=slo_name,
            success=success,
            value=value,
            context=ctx
        )
    
    async def get_slo_dashboard_data(self) -> Dict[str, Any]:
        """Get SLO dashboard data."""
        dashboard_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "slos": {},
            "overall_health": "healthy"
        }
        
        violation_count = 0
        
        for slo_name, slo_def in self.core_slos.items():
            try:
                status = await self.check_slo_compliance(slo_name)
                compliance_rate = await self.slo_monitor.get_compliance_rate(slo_name)
                
                dashboard_data["slos"][slo_name] = {
                    "name": slo_def.name,
                    "type": slo_def.slo_type.value,
                    "target": slo_def.target,
                    "current_compliance": compliance_rate,
                    "status": status.value,
                    "description": slo_def.description,
                }
                
                if status in [SLOStatus.VIOLATING, SLOStatus.DEGRADED]:
                    violation_count += 1
                    
            except Exception as e:
                self.logger.error(f"Error getting SLO data for {slo_name}", error=str(e))
                dashboard_data["slos"][slo_name] = {
                    "status": "error",
                    "error": str(e)
                }
        
        # Determine overall health
        if violation_count == 0:
            dashboard_data["overall_health"] = "healthy"
        elif violation_count <= len(self.core_slos) // 2:
            dashboard_data["overall_health"] = "degraded"
        else:
            dashboard_data["overall_health"] = "unhealthy"
        
        return dashboard_data
    
    # Alerting Interface
    
    async def send_alert(
        self,
        title: str,
        message: str,
        severity: SeverityLevel = SeverityLevel.MEDIUM,
        context: Optional[ObservabilityContext] = None,
        **metadata
    ):
        """Send an alert."""
        ctx = context or (self._context_stack[-1] if self._context_stack else None)
        
        alert_data = metadata.copy()
        if ctx:
            alert_data.update(ctx.to_dict())
        
        await self.alert_manager.send_alert(
            title=title,
            message=message,
            severity=severity.value,
            metadata=alert_data
        )
        
        # Log alert
        self.warning(
            f"Alert sent: {title}",
            message=message,
            severity=severity.value,
            context=ctx
        )
    
    # Health Check Interface
    
    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check."""
        health_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "status": "healthy",
            "components": {},
            "slos": {},
            "uptime_seconds": time.time() - (self._start_time if hasattr(self, '_start_time') else time.time())
        }
        
        # Check observability components
        components = {
            "metrics": lambda: self.metrics_client is not None,
            "logging": lambda: self.logger is not None,
            "tracing": lambda: self.trace_collector is not None,
            "slo_monitor": lambda: self.slo_monitor._initialized if hasattr(self.slo_monitor, '_initialized') else True,
            "alert_manager": lambda: self.alert_manager._initialized if hasattr(self.alert_manager, '_initialized') else True,
        }
        
        unhealthy_count = 0
        
        for component, check_func in components.items():
            try:
                is_healthy = check_func()
                health_data["components"][component] = {
                    "status": "healthy" if is_healthy else "unhealthy",
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                if not is_healthy:
                    unhealthy_count += 1
                    
            except Exception as e:
                health_data["components"][component] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
                unhealthy_count += 1
        
        # Check SLOs
        for slo_name in self.core_slos:
            try:
                status = await self.check_slo_compliance(slo_name)
                health_data["slos"][slo_name] = status.value
                
                if status in [SLOStatus.VIOLATING, SLOStatus.DEGRADED]:
                    unhealthy_count += 1
                    
            except Exception as e:
                health_data["slos"][slo_name] = "error"
                unhealthy_count += 1
        
        # Determine overall status
        total_checks = len(components) + len(self.core_slos)
        if unhealthy_count == 0:
            health_data["status"] = "healthy"
        elif unhealthy_count <= total_checks // 3:
            health_data["status"] = "degraded"
        else:
            health_data["status"] = "unhealthy"
        
        return health_data


# Global façade instance
_observability_facade: Optional[ObservabilityFacade] = None

def get_observability_facade() -> ObservabilityFacade:
    """Get the global observability façade."""
    global _observability_facade
    if _observability_facade is None:
        _observability_facade = ObservabilityFacade()
    return _observability_facade

def set_observability_facade(facade: ObservabilityFacade):
    """Set the global observability façade."""
    global _observability_facade
    _observability_facade = facade


__all__ = [
    "LogLevel",
    "SeverityLevel", 
    "ObservabilityContext",
    "SLODefinition",
    "ObservabilityFacade",
    "get_observability_facade",
    "set_observability_facade",
]