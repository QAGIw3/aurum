"""API concurrency controls and timeout middleware."""

from __future__ import annotations

import asyncio
import copy
import time
from typing import Any, Callable, ClassVar, Deque, Dict, List, Optional, Set, Tuple
from collections import defaultdict, deque
from dataclasses import dataclass, field

from ..telemetry.context import log_structured
from ..exceptions import ServiceUnavailableException
from ...core.settings import AurumSettings, get_settings
try:
    from ...observability.profiling import ProfileAsyncContext
except ImportError:  # pragma: no cover - optional profiling dependency
    class ProfileAsyncContext:  # type: ignore[override]
        """No-op async context manager used when profiling is unavailable."""

        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

from ...observability.metric_helpers import (
    get_counter,
    get_gauge,
    get_histogram,
)


def _create_lock() -> asyncio.Lock:
    """Create an asyncio.Lock with a guaranteed event loop."""

    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    return asyncio.Lock()


@dataclass
class RequestLimits:
    """Request-specific limits."""
    max_concurrent_requests: int = 100
    max_requests_per_second: float = 10.0
    max_request_duration_seconds: float = 30.0
    max_requests_per_tenant: int = 20
    tenant_burst_limit: int = 50
    tenant_queue_limit: int = 64
    queue_timeout_seconds: float = 2.0
    burst_refill_per_second: float = 0.5
    slow_start_initial_limit: int = 2
    slow_start_step_seconds: float = 3.0
    slow_start_step_size: int = 1
    slow_start_cooldown_seconds: float = 30.0
    tenant_overrides: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    _tenant_requests: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _tenant_request_times: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))

    TENANT_SCOPED_FIELDS: ClassVar[Tuple[str, ...]] = (
        "max_requests_per_tenant",
        "tenant_burst_limit",
        "tenant_queue_limit",
        "queue_timeout_seconds",
        "burst_refill_per_second",
        "slow_start_initial_limit",
        "slow_start_step_seconds",
        "slow_start_step_size",
        "slow_start_cooldown_seconds",
        "max_request_duration_seconds",
        "max_requests_per_second",
    )

    def _resolve_override(self, tenant_key: Optional[str], field_name: str) -> Optional[Any]:
        if tenant_key is None:
            return None

        direct = self.tenant_overrides.get(tenant_key)
        if isinstance(direct, dict) and field_name in direct:
            return direct[field_name]

        wildcard = self.tenant_overrides.get("*")
        if isinstance(wildcard, dict) and field_name in wildcard:
            return wildcard[field_name]

        return None

    def get_value(self, field_name: str, tenant_key: Optional[str] = None) -> Any:
        """Return a limit value, honoring tenant-specific overrides when present."""

        override = self._resolve_override(tenant_key, field_name)
        if override is not None:
            return override
        return getattr(self, field_name)

    def resolve_for_tenant(self, tenant_key: Optional[str]) -> Dict[str, Any]:
        """Resolve tenant-scoped limits for the provided tenant key."""

        return {
            field_name: self.get_value(field_name, tenant_key)
            for field_name in self.TENANT_SCOPED_FIELDS
        }

    def validate(self) -> None:
        """Validate base and override limits to guard against misconfiguration."""

        if self.max_concurrent_requests <= 0:
            raise ValueError("max_concurrent_requests must be positive")
        if self.max_requests_per_tenant <= 0:
            raise ValueError("max_requests_per_tenant must be positive")

        for tenant, override in self.tenant_overrides.items():
            if not isinstance(override, dict):
                raise ValueError(f"Tenant override for '{tenant}' must be a mapping")
            for field_name, value in override.items():
                if field_name not in self.TENANT_SCOPED_FIELDS:
                    continue
                if field_name in {"max_requests_per_tenant", "tenant_queue_limit", "slow_start_initial_limit", "slow_start_step_size"}:
                    if int(value) <= 0:
                        raise ValueError(
                            f"Tenant override '{field_name}' for '{tenant}' must be positive"
                        )


REQUEST_LIMIT_FIELD_NAMES: Tuple[str, ...] = (
    "max_concurrent_requests",
    "max_requests_per_second",
    "max_request_duration_seconds",
    "max_requests_per_tenant",
    "tenant_burst_limit",
    "tenant_queue_limit",
    "queue_timeout_seconds",
    "burst_refill_per_second",
    "slow_start_initial_limit",
    "slow_start_step_seconds",
    "slow_start_step_size",
    "slow_start_cooldown_seconds",
    "tenant_overrides",
)


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


class ConcurrencyRejected(Exception):
    """Internal signal used when concurrency acquisition fails."""

    def __init__(
        self,
        reason: str,
        status_code: int,
        tenant_id: Optional[str] = None,
        retry_after: Optional[float] = None,
        queue_depth: Optional[int] = None,
    ):
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code
        self.tenant_id = tenant_id
        self.retry_after = retry_after
        self.queue_depth = queue_depth


@dataclass
class TenantWaiter:
    """Represents a queued tenant request waiting for capacity."""

    future: asyncio.Future
    enqueued_at: float
    tenant_id: Optional[str]
    request_id: Optional[str]


@dataclass
class TenantState:
    """Tracks per-tenant concurrency, queue, and burst state."""

    active: int = 0
    burst_tokens: float = 0.0
    waiters: Deque[TenantWaiter] = field(default_factory=deque)
    last_refill: float = field(default_factory=time.perf_counter)
    last_activity: float = field(default_factory=time.perf_counter)
    slow_start_cap: int = 0
    last_slow_start_reset: float = field(default_factory=time.perf_counter)
    last_slow_start_increment: float = field(default_factory=time.perf_counter)
    enqueued: bool = False


ANONYMOUS_TENANT = "anonymous"

class ConcurrencyController:
    """Controls concurrency limits with fairness, bursts, and slow-start."""

    def __init__(self, limits: RequestLimits):
        self.limits = limits
        self.limits.validate()

        self._lock = _create_lock()
        self._tenant_states: Dict[str, TenantState] = {}
        self._waiting_tenants: Deque[str] = deque()
        self._active_requests = 0

        # Optional observers wired up by middleware for metrics
        self._queue_depth_observer: Optional[Callable[[str, int], None]] = None
        self._active_observer: Optional[Callable[[int], None]] = None
        self._tenant_active_observer: Optional[Callable[[str, int], None]] = None
        self._global_queue_observer: Optional[Callable[[int], None]] = None

    def register_metrics_observer(
        self,
        *,
        queue_depth: Optional[Callable[[str, int], None]] = None,
        active: Optional[Callable[[int], None]] = None,
        tenant_active: Optional[Callable[[str, int], None]] = None,
        global_queue: Optional[Callable[[int], None]] = None,
    ) -> None:
        """Register callbacks to receive queue and activity updates."""

        self._queue_depth_observer = queue_depth
        self._active_observer = active
        self._tenant_active_observer = tenant_active
        self._global_queue_observer = global_queue

    def _tenant_limits(self, tenant_key: str) -> Dict[str, Any]:
        return self.limits.resolve_for_tenant(tenant_key)

    async def acquire_slot(
        self,
        tenant_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> "ConcurrencySlot":
        """Acquire a concurrency slot, enforcing fairness and bursts."""

        tenant_key = tenant_id or ANONYMOUS_TENANT
        loop = asyncio.get_running_loop()
        queued_at = time.perf_counter()

        async with self._lock:
            state = self._get_tenant_state(tenant_key)
            now = time.perf_counter()
            tenant_limits = self._tenant_limits(tenant_key)
            self._refill_burst_tokens(tenant_key, state, now, tenant_limits)
            allowed = self._current_allowed_capacity(tenant_key, state, now, tenant_limits)

            if (
                self._active_requests < self.limits.max_concurrent_requests
                and state.active < allowed
            ):
                slot = self._create_slot(
                    tenant_id=tenant_id,
                    tenant_key=tenant_key,
                    state=state,
                    granted_at=now,
                    request_id=request_id,
                    queued_at=queued_at,
                    tenant_limits=tenant_limits,
                )
                return slot

            tenant_queue_limit = max(0, int(tenant_limits["tenant_queue_limit"]))
            if tenant_queue_limit > 0 and len(state.waiters) >= tenant_queue_limit:
                raise ConcurrencyRejected(
                    reason="tenant_queue_full",
                    status_code=429,
                    tenant_id=tenant_id,
                    retry_after=tenant_limits["queue_timeout_seconds"],
                    queue_depth=len(state.waiters),
                )

            waiter_future = loop.create_future()
            waiter = TenantWaiter(
                future=waiter_future,
                enqueued_at=queued_at,
                tenant_id=tenant_id,
                request_id=request_id,
            )
            state.waiters.append(waiter)
            self._queue_depth_update(tenant_key, len(state.waiters))
            if not state.enqueued:
                self._waiting_tenants.append(tenant_key)
                state.enqueued = True
            state.last_activity = now

        tenant_limits = self._tenant_limits(tenant_key)
        timeout = max(0.0, tenant_limits["queue_timeout_seconds"])
        try:
            slot: ConcurrencySlot = await asyncio.wait_for(waiter_future, timeout=timeout)
            return slot
        except asyncio.TimeoutError as exc:
            async with self._lock:
                state = self._tenant_states.get(tenant_key)
                self._remove_waiter_locked(tenant_key, waiter_future)
                queue_depth = len(state.waiters) if state else 0
            raise ConcurrencyRejected(
                reason="queue_timeout",
                status_code=503,
                tenant_id=tenant_id,
                retry_after=tenant_limits["queue_timeout_seconds"],
                queue_depth=queue_depth,
            ) from exc
        except asyncio.CancelledError:
            async with self._lock:
                self._remove_waiter_locked(tenant_key, waiter_future)
            raise

    async def get_stats(self) -> Dict[str, Any]:
        """Get concurrency statistics."""

        async with self._lock:
            tenants_snapshot = {
                tenant: {
                    "active": state.active,
                    "queued": len(state.waiters),
                    "burst_tokens": round(state.burst_tokens, 2),
                }
                for tenant, state in self._tenant_states.items()
            }
            return {
                "active_requests": self._active_requests,
                "global_limit": self.limits.max_concurrent_requests,
                "tenants": tenants_snapshot,
            }

    # Internal helpers -------------------------------------------------

    def _get_tenant_state(self, tenant_key: str) -> TenantState:
        state = self._tenant_states.get(tenant_key)
        if state is None:
            state = TenantState()
            now = time.perf_counter()
            state.last_refill = now
            state.last_activity = now
            state.last_slow_start_reset = now
            state.last_slow_start_increment = now
            tenant_limits = self._tenant_limits(tenant_key)
            state.slow_start_cap = min(
                tenant_limits["max_requests_per_tenant"],
                max(1, tenant_limits["slow_start_initial_limit"]),
            )
            self._tenant_states[tenant_key] = state
        return state

    def _refill_burst_tokens(
        self,
        tenant_key: str,
        state: TenantState,
        now: float,
        tenant_limits: Optional[Dict[str, Any]] = None,
    ) -> None:
        limits = tenant_limits or self._tenant_limits(tenant_key)
        burst_capacity = max(0, limits["tenant_burst_limit"] - limits["max_requests_per_tenant"])
        if burst_capacity <= 0:
            state.burst_tokens = 0.0
            state.last_refill = now
            return

        elapsed = max(0.0, now - state.last_refill)
        if elapsed <= 0.0:
            return

        state.burst_tokens = min(
            burst_capacity,
            state.burst_tokens + elapsed * max(0.0, limits["burst_refill_per_second"]),
        )
        state.last_refill = now

    def _maybe_reset_slow_start(
        self,
        tenant_key: str,
        state: TenantState,
        now: float,
        tenant_limits: Optional[Dict[str, Any]] = None,
    ) -> None:
        limits = tenant_limits or self._tenant_limits(tenant_key)
        if now - state.last_activity >= limits["slow_start_cooldown_seconds"]:
            state.slow_start_cap = min(
                limits["max_requests_per_tenant"],
                max(1, limits["slow_start_initial_limit"]),
            )
            state.last_slow_start_reset = now
            state.last_slow_start_increment = now

    def _current_allowed_capacity(
        self,
        tenant_key: str,
        state: TenantState,
        now: float,
        tenant_limits: Optional[Dict[str, Any]] = None,
    ) -> int:
        limits = tenant_limits or self._tenant_limits(tenant_key)
        base_limit = limits["max_requests_per_tenant"]
        self._maybe_reset_slow_start(tenant_key, state, now, limits)

        if (
            state.slow_start_cap < base_limit
            and now - state.last_slow_start_increment >= limits["slow_start_step_seconds"]
        ):
            increment = max(1, limits["slow_start_step_size"])
            state.slow_start_cap = min(base_limit, state.slow_start_cap + increment)
            state.last_slow_start_increment = now

        allowed = min(base_limit, max(1, state.slow_start_cap))
        burst_capacity = max(0, limits["tenant_burst_limit"] - base_limit)

        if burst_capacity > 0 and state.slow_start_cap >= base_limit:
            allowed += min(int(state.burst_tokens), burst_capacity)

        return max(1, allowed)

    def _create_slot(
        self,
        *,
        tenant_id: Optional[str],
        tenant_key: str,
        state: TenantState,
        granted_at: float,
        request_id: Optional[str],
        queued_at: float,
        tenant_limits: Optional[Dict[str, Any]] = None,
    ) -> "ConcurrencySlot":
        limits = tenant_limits or self._tenant_limits(tenant_key)
        state.active += 1
        self._active_requests += 1
        state.last_activity = granted_at

        base_limit = limits["max_requests_per_tenant"]
        if state.active > base_limit:
            state.burst_tokens = max(0.0, state.burst_tokens - 1.0)

        cutoff_time = granted_at - 60.0
        recent_times = [
            t for t in self.limits._tenant_request_times[tenant_key]
            if t > cutoff_time
        ]
        recent_times.append(granted_at)
        self.limits._tenant_request_times[tenant_key] = recent_times
        self.limits._tenant_requests[tenant_key] = state.active

        self._notify_active_locked()
        self._notify_tenant_active(tenant_key, state.active)

        slot = ConcurrencySlot(
            controller=self,
            tenant_id=tenant_id,
            tenant_key=tenant_key,
            acquire_time=granted_at,
            queued_at=queued_at,
            request_id=request_id,
        )
        return slot

    def _dispatch_locked(self, now: float) -> None:
        if not self._waiting_tenants:
            return

        visits = 0
        max_visits = len(self._waiting_tenants) or 1

        while (
            self._waiting_tenants
            and self._active_requests < self.limits.max_concurrent_requests
        ):
            tenant_key = self._waiting_tenants.popleft()
            state = self._tenant_states.get(tenant_key)
            if state is None:
                continue

            if not state.waiters:
                state.enqueued = False
                self._queue_depth_update(tenant_key, 0)
                continue

            tenant_limits = self._tenant_limits(tenant_key)
            self._refill_burst_tokens(tenant_key, state, now, tenant_limits)
            allowed = self._current_allowed_capacity(tenant_key, state, now, tenant_limits)

            if state.active >= allowed:
                # Tenant cannot progress right now; rotate to keep fairness
                self._waiting_tenants.append(tenant_key)
                visits += 1
                if visits >= max_visits:
                    break
                continue

            granted_any = False
            while (
                state.waiters
                and self._active_requests < self.limits.max_concurrent_requests
                and state.active < allowed
            ):
                waiter = state.waiters.popleft()
                self._queue_depth_update(tenant_key, len(state.waiters))

                if waiter.future.cancelled() or waiter.future.done():
                    continue

                granted_at = time.perf_counter()
                slot = self._create_slot(
                    tenant_id=waiter.tenant_id,
                    tenant_key=tenant_key,
                    state=state,
                    granted_at=granted_at,
                    request_id=waiter.request_id,
                    queued_at=waiter.enqueued_at,
                    tenant_limits=tenant_limits,
                )
                waiter.future.set_result(slot)
                granted_any = True
                allowed = self._current_allowed_capacity(
                    tenant_key,
                    state,
                    granted_at,
                    tenant_limits,
                )

            if state.waiters:
                self._waiting_tenants.append(tenant_key)
            else:
                state.enqueued = False

            if not granted_any:
                visits += 1
                if visits >= max_visits:
                    break

    def _remove_waiter_locked(
        self,
        tenant_key: str,
        future: asyncio.Future,
    ) -> None:
        state = self._tenant_states.get(tenant_key)
        if state is None:
            return

        new_queue: Deque[TenantWaiter] = deque()
        removed = False
        while state.waiters:
            waiter = state.waiters.popleft()
            if waiter.future is future:
                removed = True
                continue
            new_queue.append(waiter)
        state.waiters = new_queue

        if removed:
            self._queue_depth_update(tenant_key, len(state.waiters))

        if not state.waiters and state.enqueued:
            try:
                self._waiting_tenants.remove(tenant_key)
            except ValueError:
                pass
            state.enqueued = False
            self._queue_depth_update(tenant_key, 0)

    def _notify_active_locked(self) -> None:
        if self._active_observer:
            self._active_observer(self._active_requests)

    def _notify_tenant_active(self, tenant_key: str, value: int) -> None:
        if self._tenant_active_observer:
            self._tenant_active_observer(tenant_key, value)

    def _queue_depth_update(self, tenant_key: str, depth: int) -> None:
        if self._queue_depth_observer:
            self._queue_depth_observer(tenant_key, depth)
        if self._global_queue_observer:
            total_depth = sum(len(state.waiters) for state in self._tenant_states.values())
            self._global_queue_observer(total_depth)


class ConcurrencySlot:
    """Represents an acquired concurrency slot."""

    def __init__(
        self,
        *,
        controller: ConcurrencyController,
        tenant_id: Optional[str],
        tenant_key: str,
        acquire_time: float,
        queued_at: float,
        request_id: Optional[str],
    ):
        self.controller = controller
        self.tenant_id = tenant_id
        self.tenant_key = tenant_key
        self.acquire_time = acquire_time
        self.granted_at = acquire_time
        self.queued_at = queued_at
        self.queue_wait_seconds = max(0.0, acquire_time - queued_at)
        self.request_id = request_id
        self._released = False

    async def release(self) -> None:
        """Release the concurrency slot back to the controller."""

        if self._released:
            return

        async with self.controller._lock:
            state = self.controller._tenant_states.get(self.tenant_key)
            if state and state.active > 0:
                state.active -= 1
                state.last_activity = time.perf_counter()
                self.controller._notify_tenant_active(self.tenant_key, state.active)
                self.controller.limits._tenant_requests[self.tenant_key] = state.active

                # Clean up old request times (keep last 60s window)
                cutoff_time = time.perf_counter() - 60.0
                recent_times = [
                    t for t in self.controller.limits._tenant_request_times[self.tenant_key]
                    if t > cutoff_time
                ]
                self.controller.limits._tenant_request_times[self.tenant_key] = recent_times

            if self.controller._active_requests > 0:
                self.controller._active_requests -= 1
            self.controller._notify_active_locked()

            self.controller._dispatch_locked(time.perf_counter())

        self._released = True

    async def __aenter__(self) -> "ConcurrencySlot":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.release()


class RateLimiter:
    """Rate limiter for API endpoints."""

    def __init__(self):
        self._requests: Dict[str, List[float]] = defaultdict(list)
        self._lock = _create_lock()

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
        self._lock = _create_lock()

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
        self.concurrency_requests = get_counter(
            "aurum_api_concurrency_requests_total",
            "Total concurrency-controlled requests",
            ["result"],
        )
        self.request_duration = get_histogram(
            "aurum_api_request_duration_seconds",
            "Request duration in seconds",
            ["endpoint", "method"],
        )
        self.active_requests = get_gauge(
            "aurum_api_active_requests",
            "Number of active requests",
        )
        self.acquire_latency = get_histogram(
            "aurum_api_concurrency_acquire_seconds",
            "Time spent waiting to acquire a concurrency slot",
            ["tenant"],
        )
        self.queue_wait_time = get_histogram(
            "aurum_api_tenant_queue_wait_seconds",
            "Observed queue wait durations per tenant",
            ["tenant"],
        )
        self.rejections = get_counter(
            "aurum_api_concurrency_rejections_total",
            "Rejected requests by reason",
            ["reason", "tenant"],
        )
        self.tenant_queue_depth = get_gauge(
            "aurum_api_tenant_queue_depth",
            "Depth of the tenant-specific concurrency queue",
            ["tenant"],
        )
        self.tenant_active = get_gauge(
            "aurum_api_tenant_active_requests",
            "Number of active in-flight requests per tenant",
            ["tenant"],
        )
        self.global_queue_depth = get_gauge(
            "aurum_api_global_queue_depth",
            "Total queued requests across all tenants",
        )

        def _queue_observer(tenant: str, depth: int) -> None:
            try:
                self.tenant_queue_depth.labels(tenant=tenant).set(depth)
            except Exception:
                pass

        def _global_queue_observer(total: int) -> None:
            try:
                self.global_queue_depth.set(total)
            except Exception:
                pass

        def _active_observer(total: int) -> None:
            try:
                self.active_requests.set(total)
            except Exception:
                pass

        def _tenant_active_observer(tenant: str, value: int) -> None:
            try:
                self.tenant_active.labels(tenant=tenant).set(value)
            except Exception:
                pass

        self.concurrency_controller.register_metrics_observer(
            queue_depth=_queue_observer,
            active=_active_observer,
            tenant_active=_tenant_active_observer,
            global_queue=_global_queue_observer,
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
        tenant_label = ANONYMOUS_TENANT
        concurrency_slot: Optional[ConcurrencySlot] = None
        timeout_handle: Optional[TimeoutHandle] = None
        queue_wait_seconds = 0.0
        handled_successfully = False

        try:
            # Extract request ID and tenant ID from headers
            headers = dict(scope.get("headers", []))
            request_id = next(
                (v.decode() for k, v in headers.items() if k == b"x-request-id"),
                None,
            )
            tenant_id = next(
                (v.decode() for k, v in headers.items() if k == b"x-aurum-tenant"),
                None,
            )
            tenant_label = tenant_id or ANONYMOUS_TENANT

            # Check rate limits (fixed window per-tenant)
            if not await self.rate_limiter.check_rate_limit(
                tenant_label,
                max_requests=100,
                window_seconds=60.0,
            ):
                retry_seconds = 60
                retry_headers = self.timeout_controller.get_retry_after_header(retry_seconds)

                async def send_rate_limited() -> None:
                    await send({
                        "type": "http.response.start",
                        "status": 429,
                        "headers": [
                            (k.encode(), v.encode())
                            for k, v in {
                                **retry_headers,
                                "Content-Type": "application/json",
                                "X-Request-Id": request_id or "unknown",
                            }.items()
                        ],
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
                hdrs: Dict[str, str] = {}
                for key, value in dict(scope.get("headers", [])).items():
                    try:
                        hdrs[key.decode().lower()] = value.decode()
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
                    try:
                        from ..async_exec import run_job_async  # Local import to avoid hard dependency at parse time

                        task_id = run_job_async(
                            decision.job_name,
                            decision.payload,
                            queue=decision.queue,
                        )

                        headers_out = {
                            "Content-Type": "application/json",
                            "X-Request-Id": request_id or "unknown",
                        }
                        if decision.response_headers:
                            headers_out.update(decision.response_headers)

                        async def send_offloaded() -> None:
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

            # Acquire concurrency slot with fairness
            concurrency_slot = await self.concurrency_controller.acquire_slot(
                tenant_id=tenant_id,
                request_id=request_id,
            )
            queue_wait_seconds = getattr(concurrency_slot, "queue_wait_seconds", 0.0)
            try:
                self.acquire_latency.labels(tenant=tenant_label).observe(queue_wait_seconds)
                self.queue_wait_time.labels(tenant=tenant_label).observe(queue_wait_seconds)
            except Exception:
                pass

            # Create timeout handle for the request lifecycle
            timeout_handle = await self.timeout_controller.create_timeout(
                request_id or "unknown",
            )

            async def wrapped_send(message):
                """Wrapped send function with timeout checking."""
                if (
                    message["type"] == "http.response.start"
                    and timeout_handle is not None
                    and timeout_handle.is_expired()
                ):
                    log_structured(
                        "error",
                        "request_timeout_exceeded",
                        request_id=request_id,
                        tenant_id=tenant_id,
                        timeout_seconds=timeout_handle.timeout_seconds,
                        actual_duration=time.perf_counter() - start_time,
                    )

                    timeout_headers = self.timeout_controller.get_timeout_header(
                        timeout_handle.timeout_seconds,
                    )

                    await send({
                        "type": "http.response.start",
                        "status": 504,
                        "headers": [
                            (k.encode(), v.encode())
                            for k, v in {
                                **timeout_headers,
                                "Content-Type": "application/json",
                                "X-Request-Id": request_id or "unknown",
                            }.items()
                        ],
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

            async with ProfileAsyncContext(
                operation=f"api_request.{path}.{method}",
                threshold_ms=50,
            ):
                await self.app(scope, receive, wrapped_send)

            handled_successfully = True

        except ConcurrencyRejected as rejection:
            self.rejections.labels(reason=rejection.reason, tenant=tenant_label).inc()
            self.concurrency_requests.labels(result="rejected").inc()

            status_code = rejection.status_code
            queue_depth_value = (
                int(rejection.queue_depth)
                if rejection.queue_depth is not None
                else 0
            )
            response_headers = {
                "Content-Type": "application/json",
                "X-Request-Id": request_id or "unknown",
            }
            if rejection.retry_after:
                response_headers["Retry-After"] = str(int(max(1, round(rejection.retry_after))))
            response_headers["X-Queue-Depth"] = str(max(0, queue_depth_value))

            message_map = {
                "tenant_queue_full": "Per-tenant concurrency queue is full",
                "queue_timeout": "Timed out waiting for available capacity",
            }
            payload = {
                "error": "TooManyRequests" if status_code == 429 else "ServiceUnavailable",
                "message": message_map.get(rejection.reason, "Concurrency capacity unavailable"),
                "reason": rejection.reason,
                "request_id": request_id,
                "tenant_id": tenant_id,
                "queue_depth": queue_depth_value,
            }

            log_structured(
                "warning",
                "concurrency_rejected",
                request_id=request_id,
                tenant_id=tenant_id,
                reason=rejection.reason,
                queue_depth=queue_depth_value,
                status=status_code,
            )

            await send({
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    (k.encode(), v.encode()) for k, v in response_headers.items()
                ],
            })
            await send({
                "type": "http.response.body",
                "body": str(payload).encode(),
            })
            return

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

        finally:
            if timeout_handle is not None:
                await timeout_handle.cancel()

            if concurrency_slot is not None:
                await concurrency_slot.release()

            if handled_successfully:
                duration = time.perf_counter() - start_time
                try:
                    self.concurrency_requests.labels(result="success").inc()
                    self.request_duration.labels(endpoint=path, method=method).observe(duration)
                except Exception:
                    pass

                log_structured(
                    "info",
                    "request_completed",
                    request_id=request_id,
                    tenant_id=tenant_id,
                    duration_seconds=round(duration, 3),
                    queue_wait_seconds=round(queue_wait_seconds, 3),
                    path=path,
                    method=method,
                )


# Factory functions for creating middleware components
def create_concurrency_controller(
    max_concurrent_requests: int = 100,
    max_requests_per_tenant: int = 20,
    tenant_burst_limit: int = 50,
    **overrides: Any,
) -> ConcurrencyController:
    """Create a concurrency controller with default limits."""
    limits = RequestLimits(
        max_concurrent_requests=max_concurrent_requests,
        max_requests_per_tenant=max_requests_per_tenant,
        tenant_burst_limit=tenant_burst_limit,
        **overrides,
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


def create_concurrency_middleware_from_settings(
    app,
    *,
    settings: Optional[AurumSettings] = None,
    overrides: Optional[Dict[str, Any]] = None,
    rate_limiter: Optional[RateLimiter] = None,
    timeout_controller: Optional[TimeoutController] = None,
    offload_predicate: Optional[Callable[[Dict[str, Any]], Optional[OffloadInstruction]]] = None,
):
    """Build concurrency middleware using configuration derived from settings.

    When concurrency controls are disabled via settings, the original app is
    returned unchanged to support feature-flagged rollouts.
    """

    active_settings = settings or get_settings()
    concurrency_config = getattr(getattr(active_settings, "api", None), "concurrency", None)
    if concurrency_config is None:
        raise ValueError("Settings object does not expose api.concurrency configuration")

    if getattr(concurrency_config, "enabled", True) is False:
        return app

    limit_kwargs: Dict[str, Any] = {}
    for field_name in REQUEST_LIMIT_FIELD_NAMES:
        if hasattr(concurrency_config, field_name):
            limit_kwargs[field_name] = getattr(concurrency_config, field_name)

    limit_kwargs.setdefault("tenant_overrides", {})
    limit_kwargs["tenant_overrides"] = copy.deepcopy(limit_kwargs["tenant_overrides"])

    if overrides:
        overrides_copy = dict(overrides)
        override_tenants = overrides_copy.pop("tenant_overrides", None)
        limit_kwargs.update(overrides_copy)
        if override_tenants:
            merged_overrides = copy.deepcopy(limit_kwargs.get("tenant_overrides", {}))
            for tenant, values in override_tenants.items():
                merged_overrides[tenant] = values
            limit_kwargs["tenant_overrides"] = merged_overrides

    limits = RequestLimits(**limit_kwargs)
    concurrency_controller = ConcurrencyController(limits)

    rate_limiter = rate_limiter or create_rate_limiter()

    timeout_controller = timeout_controller or create_timeout_controller(
        default_timeout=limits.max_request_duration_seconds,
    )

    return ConcurrencyMiddleware(
        app=app,
        concurrency_controller=concurrency_controller,
        rate_limiter=rate_limiter,
        timeout_controller=timeout_controller,
        offload_predicate=offload_predicate,
    )
