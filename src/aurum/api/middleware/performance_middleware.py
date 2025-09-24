"""Performance monitoring and optimization middleware."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Dict, Any

try:
    from fastapi import Request, Response
    from starlette.middleware.base import BaseHTTPMiddleware
except ImportError:
    Request = None
    Response = None
    BaseHTTPMiddleware = None

from ...core import get_current_context, BackpressureManager
from ...observability.metrics import get_metrics_collector

logger = logging.getLogger(__name__)


class PerformanceMiddleware:
    """Enhanced performance monitoring middleware."""
    
    def __init__(
        self,
        enable_backpressure: bool = True,
        max_concurrent_requests: int = 1000,
        request_timeout: float = 30.0,
        enable_metrics: bool = True
    ):
        self.enable_backpressure = enable_backpressure
        self.max_concurrent_requests = max_concurrent_requests
        self.request_timeout = request_timeout
        self.enable_metrics = enable_metrics
        
        self._backpressure_manager = None
        if enable_backpressure:
            self._backpressure_manager = BackpressureManager(
                max_concurrent=max_concurrent_requests,
                max_queue_size=max_concurrent_requests * 2
            )
        
        self._metrics_collector = None
        if enable_metrics:
            try:
                self._metrics_collector = get_metrics_collector()
            except Exception:
                logger.warning("Metrics collector not available")
        
        # Track request metrics
        self._active_requests = 0
        self._total_requests = 0
        self._request_durations: Dict[str, list] = {}
    
    async def __call__(self, request: Request, call_next: Callable) -> Response:
        """Process request with performance monitoring."""
        if Request is None or Response is None:
            return await call_next(request)
        
        start_time = time.time()
        request_path = request.url.path
        request_method = request.method
        
        # Track active requests
        self._active_requests += 1
        self._total_requests += 1
        
        try:
            # Apply backpressure if enabled
            if self._backpressure_manager:
                current_load = self._backpressure_manager.current_load
                if current_load > 0.9:  # 90% capacity
                    logger.warning(f"High system load: {current_load:.2%}")
                
                async with self._backpressure_manager.acquire():
                    response = await self._process_request_with_timeout(request, call_next)
            else:
                response = await self._process_request_with_timeout(request, call_next)
            
            # Record successful request metrics
            duration = time.time() - start_time
            self._record_request_metrics(request_method, request_path, response.status_code, duration)
            
            # Add performance headers
            response.headers["X-Response-Time"] = f"{duration:.3f}s"
            response.headers["X-Active-Requests"] = str(self._active_requests)
            
            return response
        
        except asyncio.TimeoutError:
            logger.warning(f"Request timeout: {request_method} {request_path}")
            duration = time.time() - start_time
            self._record_request_metrics(request_method, request_path, 408, duration)
            
            if Response:
                return Response(
                    content="Request timeout",
                    status_code=408,
                    headers={"X-Response-Time": f"{duration:.3f}s"}
                )
            else:
                raise
        
        except Exception as e:
            logger.error(f"Request error: {request_method} {request_path}: {e}")
            duration = time.time() - start_time
            self._record_request_metrics(request_method, request_path, 500, duration)
            raise
        
        finally:
            self._active_requests -= 1
    
    async def _process_request_with_timeout(self, request: Request, call_next: Callable) -> Response:
        """Process request with timeout."""
        try:
            return await asyncio.wait_for(
                call_next(request),
                timeout=self.request_timeout
            )
        except asyncio.TimeoutError:
            raise
    
    def _record_request_metrics(
        self,
        method: str,
        path: str,
        status_code: int,
        duration: float
    ) -> None:
        """Record request performance metrics."""
        # Record duration
        metric_key = f"{method}:{path}"
        if metric_key not in self._request_durations:
            self._request_durations[metric_key] = []
        
        self._request_durations[metric_key].append(duration)
        
        # Keep only recent measurements (sliding window)
        if len(self._request_durations[metric_key]) > 1000:
            self._request_durations[metric_key] = self._request_durations[metric_key][-1000:]
        
        # Send to metrics collector if available
        if self._metrics_collector:
            try:
                self._metrics_collector.record_request_duration(method, path, status_code, duration)
                self._metrics_collector.increment_request_count(method, path, status_code)
            except Exception as e:
                logger.warning(f"Failed to record metrics: {e}")
        
        # Log slow requests
        if duration > 5.0:  # Slower than 5 seconds
            logger.warning(
                f"Slow request detected",
                extra={
                    "method": method,
                    "path": path,
                    "duration": duration,
                    "status_code": status_code
                }
            )
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics."""
        stats = {
            "active_requests": self._active_requests,
            "total_requests": self._total_requests,
            "endpoint_stats": {}
        }
        
        # Calculate endpoint statistics
        for endpoint, durations in self._request_durations.items():
            if durations:
                stats["endpoint_stats"][endpoint] = {
                    "count": len(durations),
                    "avg_duration": sum(durations) / len(durations),
                    "min_duration": min(durations),
                    "max_duration": max(durations),
                    "p95_duration": self._calculate_percentile(durations, 0.95),
                    "p99_duration": self._calculate_percentile(durations, 0.99)
                }
        
        return stats
    
    def _calculate_percentile(self, values: list, percentile: float) -> float:
        """Calculate percentile from list of values."""
        if not values:
            return 0.0
        
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile)
        if index >= len(sorted_values):
            index = len(sorted_values) - 1
        
        return sorted_values[index]


# Create middleware class compatible with Starlette/FastAPI
if BaseHTTPMiddleware is not None:
    class PerformanceHTTPMiddleware(BaseHTTPMiddleware):
        """HTTP middleware wrapper for performance monitoring."""
        
        def __init__(self, app, **kwargs):
            super().__init__(app)
            self.performance_middleware = PerformanceMiddleware(**kwargs)
        
        async def dispatch(self, request: Request, call_next) -> Response:
            return await self.performance_middleware(request, call_next)
else:
    PerformanceHTTPMiddleware = None