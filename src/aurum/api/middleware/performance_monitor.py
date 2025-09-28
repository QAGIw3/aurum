"""Middleware performance monitoring and optimization utilities."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger


logger = get_logger(__name__)


class MiddlewarePerformanceMonitor(BaseHTTPMiddleware):
    """Middleware to monitor and optimize middleware performance."""

    def __init__(self, app: ASGIApp, enable_monitoring: bool = True):
        super().__init__(app)
        self.enable_monitoring = enable_monitoring
        self.middleware_metrics: Dict[str, List[float]] = {}

    async def dispatch(self, request: Request, call_next: Callable[[Request], Any]) -> Response:
        """Monitor middleware performance."""
        if not self.enable_monitoring:
            return await call_next(request)

        start_time = time.time()

        try:
            response = await call_next(request)

            # Record total middleware stack performance
            total_time = (time.time() - start_time) * 1000  # Convert to ms

            # Log performance metric
            logger.debug(
                "middleware_performance",
                total_time_ms=total_time,
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
            )

            # Record metrics
            metrics_client = get_metrics_client()
            if metrics_client:
                try:
                    metrics_client.histogram(
                        "aurum_middleware_total_duration_ms",
                        total_time,
                        tags={
                            "method": request.method,
                            "path": request.url.path,
                            "status_code": str(response.status_code),
                        }
                    )
                except Exception as e:
                    logger.warning("Failed to record middleware metrics", error=str(e))

            return response

        except Exception as e:
            # Record error metrics
            error_time = (time.time() - start_time) * 1000

            logger.error(
                "middleware_error",
                error=str(e),
                total_time_ms=error_time,
                method=request.method,
                path=request.url.path,
            )

            # Re-raise the exception
            raise


class MiddlewareStackOptimizer:
    """Utility class for optimizing middleware stack performance."""

    def __init__(self):
        self.performance_data: Dict[str, Any] = {}
        self.optimization_rules = self._load_optimization_rules()

    def _load_optimization_rules(self) -> Dict[str, Any]:
        """Load middleware optimization rules."""
        return {
            "max_middleware_time": 50.0,  # ms - warn if total middleware > 50ms
            "max_individual_time": 10.0,   # ms - warn if single middleware > 10ms
            "high_frequency_threshold": 100,  # requests/min - optimize frequently used paths
        }

    def analyze_middleware_stack(self, performance_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze middleware stack performance and suggest optimizations."""
        analysis = {
            "overall_health": "good",
            "issues": [],
            "suggestions": [],
            "performance_score": 100,
        }

        # Check total middleware time
        total_time = performance_data.get("total_time", 0)
        if total_time > self.optimization_rules["max_middleware_time"]:
            analysis["issues"].append({
                "type": "slow_middleware_stack",
                "severity": "warning",
                "message": f"Total middleware time ({total_time:.2f}ms) exceeds threshold",
                "threshold": self.optimization_rules["max_middleware_time"],
            })
            analysis["performance_score"] -= 20

        # Check individual middleware performance
        middleware_times = performance_data.get("middleware_times", {})
        for middleware_name, avg_time in middleware_times.items():
            if avg_time > self.optimization_rules["max_individual_time"]:
                analysis["issues"].append({
                    "type": "slow_middleware",
                    "severity": "warning",
                    "middleware": middleware_name,
                    "message": f"Middleware {middleware_name} is slow ({avg_time:.2f}ms avg)",
                    "threshold": self.optimization_rules["max_individual_time"],
                })
                analysis["performance_score"] -= 10

        # Generate suggestions
        if analysis["performance_score"] < 80:
            analysis["suggestions"].extend([
                "Consider moving expensive middleware to application layer",
                "Optimize database queries in middleware",
                "Cache expensive computations",
                "Consider async middleware for I/O operations",
            ])

        analysis["overall_health"] = "poor" if analysis["performance_score"] < 60 else "warning" if analysis["performance_score"] < 80 else "good"

        return analysis

    def suggest_middleware_reordering(self, current_order: List[str]) -> List[str]:
        """Suggest optimal middleware ordering based on performance characteristics."""
        # Security middleware should be outermost
        security_middleware = ["cors", "security_headers", "auth"]

        # Performance-critical middleware should be innermost
        performance_critical = ["compression", "caching"]

        # Move security middleware to front
        optimized_order = []
        remaining = current_order.copy()

        for middleware in security_middleware:
            if middleware in remaining:
                optimized_order.append(middleware)
                remaining.remove(middleware)

        # Add performance-critical middleware at the end
        for middleware in performance_critical:
            if middleware in remaining:
                optimized_order.append(middleware)
                remaining.remove(middleware)

        # Add remaining middleware
        optimized_order.extend(remaining)

        return optimized_order


def create_middleware_performance_middleware(
    app: ASGIApp,
    enable_monitoring: bool = True,
    enable_optimization: bool = True
) -> ASGIApp:
    """Create middleware performance monitoring middleware."""
    return MiddlewarePerformanceMonitor(app, enable_monitoring)


__all__ = [
    "MiddlewarePerformanceMonitor",
    "MiddlewareStackOptimizer",
    "create_middleware_performance_middleware",
]
