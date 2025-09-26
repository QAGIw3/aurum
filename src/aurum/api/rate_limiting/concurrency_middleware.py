"""API concurrency controls and timeout middleware."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict
from dataclasses import dataclass, field

try:
    from prometheus_client import Counter, Histogram, Gauge
except ImportError:
    # Fallback when prometheus is not available
    class Counter:
        def __init__(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
        def labels(self, **kwargs): return self

    class Histogram:
        def __init__(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, **kwargs): return self

    class Gauge:
        def __init__(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def labels(self, **kwargs): return self

from ..telemetry.context import get_request_id, log_structured
from ..exceptions import ServiceUnavailableException


@dataclass
class RequestLimits:
    """Request-specific limits."""
    max_concurrent_requests: int = 100
    max_requests_per_second: float = 10.0
    max_request_duration_seconds: float = 30.0
    max_requests_per_tenant: int = 20
    tenant_burst_limit: int = 50

    _tenant_requests: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _tenant_request_times: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))


@dataclass(frozen=True)
class OffloadInstruction:
    """Instruction produced by an offload predicate to enqueue work to Celery.

    Attributes:
        job_name: Registered job name for async execution.
        payload: JSON-serializable payload to send to the worker.
        queue: Optional Celery queue name (e.g., "cpu_bound").
        response_headers: Optional extra headers to include in the 202 response.
        status_url: Optional status polling URL to return in the response.
    """

    job_name: str
    payload: Dict[str, Any]
    queue: Optional[str] = None
    response_headers: Optional[Dict[str, str]] = None
    status_url: Optional[str] = None


class ConcurrencyController:
    """Controls concurrency limits and throttling."""

    def __init__(self, limits: RequestLimits):
        self.limits = limits
        self._active_requests = 0
        self._semaphore = asyncio.Semaphore(limits.max_concurrent_requests)
        self._tenant_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._lock = asyncio.Lock()

    async def acquire_slot(
        self,
        tenant_id: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> ConcurrencySlot:
        """Acquire a concurrency slot."""
        start_time = time.perf_counter()

        # Check global concurrency limit
        if not self._semaphore.locked():
            await self._semaphore.acquire()
        else:
            # Global limit reached
            log_structured(
                "warning",
                "global_concurrency_limit_reached",
                tenant_id=tenant_id,
                request_id=request_id,
                current_requests=self._active_requests,
                max_requests=self.limits.max_concurrent_requests,
            )
            raise ServiceUnavailableException(
                "concurrency_control"
            )

        # Check tenant-specific limits
        if tenant_id:
            tenant_semaphore = await self._get_tenant_semaphore(tenant_id)
            if tenant_semaphore.locked():
                # Tenant limit reached
                log_structured(
                    "warning",
                    "tenant_concurrency_limit_reached",
                    tenant_id=tenant_id,
                    request_id=request_id,
                    tenant_requests=self.limits._tenant_requests.get(tenant_id, 0),
                    max_tenant_requests=self.limits.max_requests_per_tenant,
                )
                # Release global semaphore
                self._semaphore.release()
                raise ServiceUnavailableException(
                    "concurrency_control"
                )

            await tenant_semaphore.acquire()

        async with self._lock:
            self._active_requests += 1
            if tenant_id:
                self.limits._tenant_requests[tenant_id] += 1

                cutoff_time = start_time - 60.0  # Track last minute of activity
                recent_times = [
                    t for t in self.limits._tenant_request_times[tenant_id]
                    if t > cutoff_time
                ]
                recent_times.append(start_time)
                self.limits._tenant_request_times[tenant_id] = recent_times

        return ConcurrencySlot(
            controller=self,
            tenant_id=tenant_id,
            acquire_time=start_time,
            request_id=request_id
        )

    async def _get_tenant_semaphore(self, tenant_id: str) -> asyncio.Semaphore:
        """Get or create tenant-specific semaphore."""
        if tenant_id not in self._tenant_semaphores:
            self._tenant_semaphores[tenant_id] = asyncio.Semaphore(
                self.limits.max_requests_per_tenant
            )
        return self._tenant_semaphores[tenant_id]

    async def get_stats(self) -> Dict[str, Any]:
        """Get concurrency statistics."""
        async with self._lock:
            return {
                "active_requests": self._active_requests,
                "global_limit": self.limits.max_concurrent_requests,
                "global_available": self._semaphore._value,
                "tenants": dict(self.limits._tenant_requests),
            }


class ConcurrencySlot:
    """Represents an acquired concurrency slot."""

    def __init__(
        self,
        controller: ConcurrencyController,
        tenant_id: Optional[str] = None,
        acquire_time: float = 0.0,
        request_id: Optional[str] = None
    ):
        self.controller = controller
        self.tenant_id = tenant_id
        self.acquire_time = acquire_time
        self.request_id = request_id
        self._released = False

    async def release(self) -> None:
        """Release the concurrency slot."""
        if self._released:
            return

        async with self.controller._lock:
            self.controller._active_requests -= 1
            if self.tenant_id:
                self.controller.limits._tenant_requests[self.tenant_id] -= 1
                # Clean up old request times
                request_times = self.controller.limits._tenant_request_times[self.tenant_id]
                cutoff_time = time.perf_counter() - 60.0  # Keep last 60 seconds
                self.controller.limits._tenant_request_times[self.tenant_id] = [
                    t for t in request_times if t > cutoff_time
                ]

        # Release semaphores
        self.controller._semaphore.release()
        if self.tenant_id:
            tenant_semaphore = await self.controller._get_tenant_semaphore(self.tenant_id)
            tenant_semaphore.release()

        self._released = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()


class RateLimiter:
    """Rate limiter for API endpoints."""

    def __init__(self):
        self._requests: Dict[str, List[float]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def check_rate_limit(
        self,
        identifier: str,
        max_requests: float,
        window_seconds: float = 60.0
    ) -> bool:
        """Check if request is within rate limits."""
        now = time.perf_counter()

        async with self._lock:
            # Clean old requests
            cutoff = now - window_seconds
            self._requests[identifier] = [
                req_time for req_time in self._requests[identifier]
                if req_time > cutoff
            ]

            # Check rate limit
            if len(self._requests[identifier]) >= max_requests:
                return False

            # Add current request
            self._requests[identifier].append(now)
            return True

    async def get_remaining_requests(
        self,
        identifier: str,
        max_requests: float,
        window_seconds: float = 60.0
    ) -> int:
        """Get remaining requests for rate limit."""
        now = time.perf_counter()

        async with self._lock:
            cutoff = now - window_seconds
            self._requests[identifier] = [
                req_time for req_time in self._requests[identifier]
                if req_time > cutoff
            ]

            return max(0, int(max_requests - len(self._requests[identifier])))


class TimeoutController:
    """Controls request timeouts and exposes retry hints."""

    def __init__(self, default_timeout: float = 30.0):
        self.default_timeout = default_timeout
        self._active_timeouts: Set[str] = set()
        self._lock = asyncio.Lock()

    async def create_timeout(
        self,
        request_id: str,
        timeout_seconds: Optional[float] = None
    ) -> TimeoutHandle:
        """Create a timeout handle for a request."""
        timeout = timeout_seconds or self.default_timeout

        async with self._lock:
            self._active_timeouts.add(request_id)

        return TimeoutHandle(
            controller=self,
            request_id=request_id,
            timeout_seconds=timeout,
            start_time=time.perf_counter()
        )

    def get_retry_after_header(self, retry_seconds: float) -> Dict[str, str]:
        """Get Retry-After header for rate limiting."""
        return {"Retry-After": str(int(retry_seconds))}

    def get_timeout_header(self, timeout_seconds: float) -> Dict[str, str]:
        """Get timeout hint headers."""
        return {
            "X-Timeout-Seconds": str(int(timeout_seconds)),
            "X-Retry-After-Timeout": str(int(timeout_seconds * 2)),
        }


class TimeoutHandle:
    """Handle for request timeout."""

    def __init__(
        self,
        controller: TimeoutController,
        request_id: str,
        timeout_seconds: float,
        start_time: float
    ):
        self.controller = controller
        self.request_id = request_id
        self.timeout_seconds = timeout_seconds
        self.start_time = start_time
        self._cancelled = False

    def is_expired(self) -> bool:
        """Check if timeout has expired."""
        if self._cancelled:
            return False
        return time.perf_counter() - self.start_time >= self.timeout_seconds

    def get_remaining_seconds(self) -> float:
        """Get remaining seconds until timeout."""
        if self._cancelled:
            return 0.0
        elapsed = time.perf_counter() - self.start_time
        return max(0.0, self.timeout_seconds - elapsed)

    async def cancel(self) -> None:
        """Cancel the timeout."""
        self._cancelled = True

        async with self.controller._lock:
            self.controller._active_timeouts.discard(self.request_id)


class ConcurrencyMiddleware:
    """Middleware for API concurrency controls and timeouts."""

    def __init__(
        self,
        app,
        concurrency_controller: ConcurrencyController,
        rate_limiter: RateLimiter,
        timeout_controller: TimeoutController,
        exclude_paths: Optional[Set[str]] = None,
        offload_predicate: Optional[Callable[[Dict[str, Any]], Optional[OffloadInstruction]]] = None,
    ):
        self.app = app
        self.concurrency_controller = concurrency_controller
        self.rate_limiter = rate_limiter
        self.timeout_controller = timeout_controller
        self.exclude_paths = exclude_paths or {"/health", "/ready", "/metrics"}
        self.offload_predicate = offload_predicate

        # Metrics (graceful degradation if prometheus not available)
        self.concurrency_requests = Counter(
            "aurum_api_concurrency_requests_total",
            "Total concurrency-controlled requests",
            ["result"]
        )
        self.request_duration = Histogram(
            "aurum_api_request_duration_seconds",
            "Request duration in seconds",
            ["endpoint", "method"]
        )
        self.active_requests = Gauge(
            "aurum_api_active_requests",
            "Number of active requests"
        )

    async def __call__(self, scope, receive, send):
        """Process request with concurrency controls."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope["path"]
        method = scope["method"]

        # Skip excluded paths
        if path in self.exclude_paths:
            await self.app(scope, receive, send)
            return

        request_id = None
        tenant_id = None
        start_time = time.perf_counter()

        try:
            # Extract request ID and tenant ID from headers
            headers = dict(scope.get("headers", []))
            request_id = next(
                (v.decode() for k, v in headers.items() if k == b"x-request-id"),
                None
            )
            tenant_id = next(
                (v.decode() for k, v in headers.items() if k == b"x-aurum-tenant"),
                None
            )

            # Check rate limits
            identifier = tenant_id or "anonymous"
            if not await self.rate_limiter.check_rate_limit(
                identifier,
                max_requests=100,  # requests per minute
                window_seconds=60.0
            ):
                retry_seconds = 60  # Wait 1 minute
                headers = self.timeout_controller.get_retry_after_header(retry_seconds)

                async def send_rate_limited():
                    await send({
                        "type": "http.response.start",
                        "status": 429,
                        "headers": [
                            (k.encode(), v.encode())
                            for k, v in {
                                **headers,
                                "Content-Type": "application/json",
                                "X-Request-Id": request_id or "unknown"
                            }.items()
                        ]
                    })

                    error_response = {
                        "error": "RateLimitExceeded",
                        "message": "Rate limit exceeded",
                        "retry_after": retry_seconds,
                        "request_id": request_id,
                    }

                    await send({
                        "type": "http.response.body",
                        "body": str(error_response).encode(),
                    })

                self.concurrency_requests.labels(result="rate_limited").inc()
                await send_rate_limited()
                return

            # Predicate-based async offload (before acquiring slot)
            if self.offload_predicate is not None:
                # Build lightweight request info for predicate
                hdrs = {}
                for k, v in dict(scope.get("headers", [])).items():
                    try:
                        hdrs[k.decode().lower()] = v.decode()
                    except Exception:
                        continue
                req_info = {
                    "path": path,
                    "method": method,
                    "headers": hdrs,
                    "query_string": (scope.get("query_string") or b"").decode(errors="ignore"),
                    "client": scope.get("client"),
                }

                try:
                    decision = self.offload_predicate(req_info)
                except Exception as exc:  # pragma: no cover - predicate errors fall through
                    log_structured(
                        "error",
                        "offload_predicate_error",
                        request_id=request_id,
                        tenant_id=tenant_id,
                        error=str(exc),
                    )
                    decision = None

                if decision is not None:
                    # Attempt to enqueue job
                    try:
                        from ..async_exec import run_job_async  # Local import to avoid hard dependency at parse time

                        task_id = run_job_async(
                            decision.job_name,
                            decision.payload,
                            queue=decision.queue,
                        )

                        # 202 Accepted with task id and optional status URL
                        headers_out = {
                            "Content-Type": "application/json",
                            "X-Request-Id": request_id or "unknown",
                        }
                        if decision.response_headers:
                            headers_out.update(decision.response_headers)

                        async def send_offloaded():
                            await send({
                                "type": "http.response.start",
                                "status": 202,
                                "headers": [
                                    (k.encode(), v.encode()) for k, v in headers_out.items()
                                ],
                            })

                            body = {
                                "status": "accepted",
                                "task_id": task_id,
                                "request_id": request_id,
                            }
                            if decision.status_url:
                                body["status_url"] = decision.status_url.format(task_id=task_id)

                            await send({
                                "type": "http.response.body",
                                "body": str(body).encode(),
                            })

                        self.concurrency_requests.labels(result="offloaded").inc()
                        await send_offloaded()
                        return

                    except Exception as exc:  # pragma: no cover - if Celery not configured
                        log_structured(
                            "error",
                            "offload_enqueue_failed",
                            request_id=request_id,
                            tenant_id=tenant_id,
                            error=str(exc),
                        )
                        # Fall through to normal in-process handling

            # Acquire concurrency slot
            concurrency_slot = await self.concurrency_controller.acquire_slot(
                tenant_id=tenant_id,
                request_id=request_id
            )

            # Create timeout handle
            timeout_handle = await self.timeout_controller.create_timeout(
                request_id or "unknown",
                timeout_seconds=30.0  # Default timeout
            )

            async def wrapped_send(message):
                """Wrapped send function with timeout checking."""
                if message["type"] == "http.response.start":
                    # Check for timeout
                    if timeout_handle.is_expired():
                        log_structured(
                            "error",
                            "request_timeout_exceeded",
                            request_id=request_id,
                            tenant_id=tenant_id,
                            timeout_seconds=timeout_handle.timeout_seconds,
                            actual_duration=time.perf_counter() - start_time,
                        )

                        # Send timeout response
                        timeout_headers = self.timeout_controller.get_timeout_header(
                            timeout_handle.timeout_seconds
                        )

                        await send({
                            "type": "http.response.start",
                            "status": 504,
                            "headers": [
                                (k.encode(), v.encode())
                                for k, v in {
                                    **timeout_headers,
                                    "Content-Type": "application/json",
                                    "X-Request-Id": request_id or "unknown"
                                }.items()
                            ]
                        })

                        timeout_response = {
                            "error": "GatewayTimeout",
                            "message": "Request timeout exceeded",
                            "timeout_seconds": timeout_handle.timeout_seconds,
                            "request_id": request_id,
                        }

                        await send({
                            "type": "http.response.body",
                            "body": str(timeout_response).encode(),
                        })
                        return

                await send(message)

            # Update active requests metric
            self.active_requests.set(self.concurrency_controller._active_requests)

            try:
                # Process request
                await self.app(scope, receive, wrapped_send)

            finally:
                # Clean up resources
                await concurrency_slot.release()
                await timeout_handle.cancel()

                # Record metrics
                duration = time.perf_counter() - start_time
                self.request_duration.labels(
                    endpoint=path,
                    method=method
                ).observe(duration)

                self.active_requests.set(self.concurrency_controller._active_requests)

                log_structured(
                    "info",
                    "request_completed",
                    request_id=request_id,
                    tenant_id=tenant_id,
                    duration_seconds=round(duration, 3),
                    path=path,
                    method=method,
                )

        except ServiceUnavailableException as exc:
            # Handle concurrency/rate limit errors
            self.concurrency_requests.labels(result="rejected").inc()

            # Send error response
            async def send_error(response):
                await send({
                    "type": "http.response.start",
                    "status": exc.status_code,
                    "headers": [
                        (k.encode(), v.encode())
                        for k, v in {
                            "Content-Type": "application/json",
                            "X-Request-Id": request_id or "unknown"
                        }.items()
                    ]
                })

                error_detail = {
                    "error": exc.__class__.__name__,
                    "message": exc.detail,
                    "request_id": request_id,
                }

                await send({
                    "type": "http.response.body",
                    "body": str(error_detail).encode(),
                })

            await send_error(exc)
            return

        except Exception as exc:
            # Handle unexpected errors
            self.concurrency_requests.labels(result="error").inc()

            log_structured(
                "error",
                "concurrency_middleware_error",
                request_id=request_id,
                tenant_id=tenant_id,
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )

            # Send error response
            async def send_error_response():
                await send({
                    "type": "http.response.start",
                    "status": 500,
                    "headers": [
                        (k.encode(), v.encode())
                        for k, v in {
                            "Content-Type": "application/json",
                            "X-Request-Id": request_id or "unknown"
                        }.items()
                    ]
                })

                error_detail = {
                    "error": "InternalServerError",
                    "message": "Internal server error",
                    "request_id": request_id,
                }

                await send({
                    "type": "http.response.body",
                    "body": str(error_detail).encode(),
                })

            await send_error_response()
            return


# Factory functions for creating middleware components
def create_concurrency_controller(
    max_concurrent_requests: int = 100,
    max_requests_per_tenant: int = 20,
    tenant_burst_limit: int = 50,
) -> ConcurrencyController:
    """Create a concurrency controller with default limits."""
    limits = RequestLimits(
        max_concurrent_requests=max_concurrent_requests,
        max_requests_per_tenant=max_requests_per_tenant,
        tenant_burst_limit=tenant_burst_limit,
    )
    return ConcurrencyController(limits)


def create_rate_limiter() -> RateLimiter:
    """Create a rate limiter."""
    return RateLimiter()


def create_timeout_controller(default_timeout: float = 30.0) -> TimeoutController:
    """Create a timeout controller."""
    return TimeoutController(default_timeout=default_timeout)


def create_concurrency_middleware(
    app,
    concurrency_controller: Optional[ConcurrencyController] = None,
    rate_limiter: Optional[RateLimiter] = None,
    timeout_controller: Optional[TimeoutController] = None,
    offload_predicate: Optional[Callable[[Dict[str, Any]], Optional[OffloadInstruction]]] = None,
) -> ConcurrencyMiddleware:
    """Create concurrency middleware with all components."""
    if concurrency_controller is None:
        concurrency_controller = create_concurrency_controller()

    if rate_limiter is None:
        rate_limiter = create_rate_limiter()

    if timeout_controller is None:
        timeout_controller = create_timeout_controller()

    return ConcurrencyMiddleware(
        app=app,
        concurrency_controller=concurrency_controller,
        rate_limiter=rate_limiter,
        timeout_controller=timeout_controller,
        offload_predicate=offload_predicate,
    )
