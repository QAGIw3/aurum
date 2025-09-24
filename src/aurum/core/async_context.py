"""Enhanced async context management and resource handling."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, Optional, Set, TypeVar
from weakref import WeakSet

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Context variables for request tracking
request_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("request_id", default=None)
tenant_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("tenant_id", default=None)
user_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("user_id", default=None)
operation_context_var: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar("operation_context", default=None)


@dataclass
class RequestContext:
    """Request context with correlation IDs and metadata."""
    request_id: str
    tenant_id: Optional[str] = None
    user_id: Optional[str] = None
    operation: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/tracing."""
        return {
            "request_id": self.request_id,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "operation": self.operation,
            "metadata": self.metadata or {}
        }


class AsyncResourceManager:
    """Manager for async resources with proper cleanup."""
    
    def __init__(self) -> None:
        self._resources: WeakSet[Any] = WeakSet()
        self._cleanup_tasks: Set[asyncio.Task] = set()
        self._disposed = False
    
    def register_resource(self, resource: Any) -> None:
        """Register a resource for cleanup."""
        if not self._disposed:
            self._resources.add(resource)
    
    async def cleanup_all(self) -> None:
        """Clean up all registered resources."""
        if self._disposed:
            return
            
        self._disposed = True
        cleanup_errors = []
        
        # Clean up resources
        resources = list(self._resources)
        for resource in resources:
            try:
                if hasattr(resource, "aclose"):
                    await resource.aclose()
                elif hasattr(resource, "close"):
                    if asyncio.iscoroutinefunction(resource.close):
                        await resource.close()
                    else:
                        resource.close()
            except Exception as e:
                cleanup_errors.append(e)
                logger.warning(f"Error cleaning up resource {resource}: {e}")
        
        # Cancel cleanup tasks
        for task in self._cleanup_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    cleanup_errors.append(e)
                    logger.warning(f"Error cancelling cleanup task: {e}")
        
        self._cleanup_tasks.clear()
        
        if cleanup_errors:
            logger.error(f"Encountered {len(cleanup_errors)} errors during cleanup")


class BackpressureManager:
    """Manages backpressure for async operations."""
    
    def __init__(self, max_concurrent: int = 100, max_queue_size: int = 1000):
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._queue_size = 0
        self._max_queue_size = max_queue_size
        self._lock = asyncio.Lock()
    
    @asynccontextmanager
    async def acquire(self):
        """Acquire slot with backpressure control."""
        async with self._lock:
            if self._queue_size >= self._max_queue_size:
                raise asyncio.QueueFull("Too many pending operations")
            self._queue_size += 1
        
        try:
            async with self._semaphore:
                yield
        finally:
            async with self._lock:
                self._queue_size -= 1
    
    @property
    def current_load(self) -> float:
        """Get current load as percentage (0-1)."""
        return self._queue_size / self._max_queue_size if self._max_queue_size > 0 else 0.0


class AsyncBatch:
    """Batching utility for async operations."""
    
    def __init__(self, batch_size: int = 100, timeout_seconds: float = 1.0):
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self._items = []
        self._futures = []
        self._timer_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
    
    async def add(self, item: T) -> T:
        """Add item to batch and wait for result."""
        future = asyncio.Future()
        
        async with self._lock:
            self._items.append(item)
            self._futures.append(future)
            
            if len(self._items) >= self.batch_size:
                await self._process_batch()
            elif self._timer_task is None:
                self._timer_task = asyncio.create_task(self._timer())
        
        return await future
    
    async def _timer(self) -> None:
        """Timer to process batch after timeout."""
        try:
            await asyncio.sleep(self.timeout_seconds)
            async with self._lock:
                if self._items:  # Still have items to process
                    await self._process_batch()
        except asyncio.CancelledError:
            pass
    
    async def _process_batch(self) -> None:
        """Process current batch (override in subclass)."""
        if not self._items:
            return
            
        items = self._items[:]
        futures = self._futures[:]
        self._items.clear()
        self._futures.clear()
        
        if self._timer_task:
            self._timer_task.cancel()
            self._timer_task = None
        
        # Default implementation - just return items as-is
        for item, future in zip(items, futures):
            if not future.cancelled():
                future.set_result(item)


# Context management functions
def get_request_id() -> Optional[str]:
    """Get current request ID."""
    return request_id_var.get()


def get_tenant_id() -> Optional[str]:
    """Get current tenant ID."""
    return tenant_id_var.get()


def get_user_id() -> Optional[str]:
    """Get current user ID."""
    return user_id_var.get()


def get_operation_context() -> Optional[Dict[str, Any]]:
    """Get current operation context."""
    return operation_context_var.get()


def get_current_context() -> RequestContext:
    """Get current request context."""
    return RequestContext(
        request_id=get_request_id() or str(uuid.uuid4()),
        tenant_id=get_tenant_id(),
        user_id=get_user_id(),
        operation=get_operation_context().get("operation") if get_operation_context() else None,
        metadata=get_operation_context()
    )


@asynccontextmanager
async def request_context(
    request_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
    user_id: Optional[str] = None,
    operation: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> AsyncGenerator[RequestContext, None]:
    """Set request context for async operations."""
    context = RequestContext(
        request_id=request_id or str(uuid.uuid4()),
        tenant_id=tenant_id,
        user_id=user_id,
        operation=operation,
        metadata=metadata
    )
    
    token_request = request_id_var.set(context.request_id)
    token_tenant = tenant_id_var.set(context.tenant_id)
    token_user = user_id_var.set(context.user_id)
    token_operation = operation_context_var.set(context.metadata)
    
    try:
        yield context
    finally:
        request_id_var.reset(token_request)
        tenant_id_var.reset(token_tenant)
        user_id_var.reset(token_user)
        operation_context_var.reset(token_operation)


@asynccontextmanager
async def managed_resources() -> AsyncGenerator[AsyncResourceManager, None]:
    """Context manager for async resource cleanup."""
    manager = AsyncResourceManager()
    try:
        yield manager
    finally:
        await manager.cleanup_all()


class AsyncPool:
    """Generic async pool for reusable resources."""
    
    def __init__(self, factory, max_size: int = 10, timeout: float = 30.0):
        self._factory = factory
        self._pool = asyncio.Queue(maxsize=max_size)
        self._created = 0
        self._max_size = max_size
        self._timeout = timeout
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire resource from pool."""
        try:
            # Try to get from pool first
            return self._pool.get_nowait()
        except asyncio.QueueEmpty:
            # Create new if under limit
            async with self._lock:
                if self._created < self._max_size:
                    self._created += 1
                    return await self._factory()
            
            # Wait for available resource
            return await asyncio.wait_for(self._pool.get(), timeout=self._timeout)
    
    async def release(self, resource):
        """Release resource back to pool."""
        try:
            self._pool.put_nowait(resource)
        except asyncio.QueueFull:
            # Pool is full, dispose the resource
            if hasattr(resource, "close"):
                if asyncio.iscoroutinefunction(resource.close):
                    await resource.close()
                else:
                    resource.close()
            
            async with self._lock:
                self._created -= 1
    
    @asynccontextmanager
    async def get(self):
        """Context manager for pool resource."""
        resource = await self.acquire()
        try:
            yield resource
        finally:
            await self.release(resource)
    
    async def close(self):
        """Close all resources in pool."""
        while not self._pool.empty():
            try:
                resource = self._pool.get_nowait()
                if hasattr(resource, "close"):
                    if asyncio.iscoroutinefunction(resource.close):
                        await resource.close()
                    else:
                        resource.close()
            except asyncio.QueueEmpty:
                break
        self._created = 0