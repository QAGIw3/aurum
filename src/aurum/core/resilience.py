"""Resilience patterns: Circuit breakers, retries, and error handling."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Type, Union
from functools import wraps

logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5       # Failures before opening
    success_threshold: int = 3       # Successes to close from half-open
    timeout_seconds: float = 60.0    # Time before trying half-open
    expected_exceptions: tuple = (Exception,)  # Exceptions to track


@dataclass
class CircuitBreakerMetrics:
    """Circuit breaker metrics."""
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[float] = None
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests


class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """Circuit breaker implementation for resilience."""
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.metrics = CircuitBreakerMetrics()
        self._lock = asyncio.Lock()
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        async with self._lock:
            # Check if circuit breaker should be opened
            if self.metrics.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self.metrics.state = CircuitBreakerState.HALF_OPEN
                    self.metrics.success_count = 0
                    logger.info("Circuit breaker entering half-open state")
                else:
                    raise CircuitBreakerOpenException("Circuit breaker is open")
        
        # Execute the function
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            await self._record_success()
            return result
            
        except self.config.expected_exceptions as e:
            await self._record_failure()
            raise
    
    async def _record_success(self) -> None:
        """Record successful execution."""
        async with self._lock:
            self.metrics.total_requests += 1
            self.metrics.successful_requests += 1
            
            if self.metrics.state == CircuitBreakerState.HALF_OPEN:
                self.metrics.success_count += 1
                if self.metrics.success_count >= self.config.success_threshold:
                    self.metrics.state = CircuitBreakerState.CLOSED
                    self.metrics.failure_count = 0
                    logger.info("Circuit breaker closed - service recovered")
    
    async def _record_failure(self) -> None:
        """Record failed execution."""
        async with self._lock:
            self.metrics.total_requests += 1
            self.metrics.failed_requests += 1
            self.metrics.failure_count += 1
            self.metrics.last_failure_time = time.time()
            
            if (self.metrics.state == CircuitBreakerState.CLOSED and 
                self.metrics.failure_count >= self.config.failure_threshold):
                self.metrics.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker opened after {self.metrics.failure_count} failures")
            elif self.metrics.state == CircuitBreakerState.HALF_OPEN:
                self.metrics.state = CircuitBreakerState.OPEN
                logger.warning("Circuit breaker re-opened due to failure in half-open state")
    
    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt reset."""
        if self.metrics.last_failure_time is None:
            return True
        
        return (time.time() - self.metrics.last_failure_time) >= self.config.timeout_seconds
    
    def get_metrics(self) -> CircuitBreakerMetrics:
        """Get current metrics."""
        return self.metrics


class RetryConfig:
    """Retry configuration."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_backoff: bool = True,
        jitter: bool = True,
        retryable_exceptions: tuple = (Exception,),
        non_retryable_exceptions: tuple = ()
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_backoff = exponential_backoff
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions
        self.non_retryable_exceptions = non_retryable_exceptions


class RetryExhaustedException(Exception):
    """Exception raised when all retry attempts are exhausted."""
    pass


async def retry_with_backoff(
    func: Callable,
    config: RetryConfig,
    *args,
    **kwargs
) -> Any:
    """Execute function with retry and exponential backoff."""
    import random
    
    last_exception = None
    
    for attempt in range(config.max_attempts):
        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
                
        except config.non_retryable_exceptions as e:
            logger.warning(f"Non-retryable exception: {e}")
            raise
            
        except config.retryable_exceptions as e:
            last_exception = e
            
            if attempt == config.max_attempts - 1:
                # Last attempt failed
                break
            
            # Calculate delay
            if config.exponential_backoff:
                delay = min(config.base_delay * (2 ** attempt), config.max_delay)
            else:
                delay = config.base_delay
            
            # Add jitter to prevent thundering herd
            if config.jitter:
                delay *= (0.5 + random.random() * 0.5)  # 50-100% of calculated delay
            
            logger.info(f"Retry attempt {attempt + 1}/{config.max_attempts} after {delay:.2f}s delay")
            await asyncio.sleep(delay)
    
    # All attempts failed
    raise RetryExhaustedException(
        f"All {config.max_attempts} retry attempts failed"
    ) from last_exception


def circuit_breaker(config: CircuitBreakerConfig):
    """Decorator for circuit breaker functionality."""
    cb = CircuitBreaker(config)
    
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await cb.call(func, *args, **kwargs)
        return wrapper
    return decorator


def retry(config: RetryConfig):
    """Decorator for retry functionality."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await retry_with_backoff(func, config, *args, **kwargs)
        return wrapper
    return decorator


class BulkheadConfig:
    """Bulkhead isolation configuration."""
    
    def __init__(self, max_concurrent: int = 10, queue_size: int = 100):
        self.max_concurrent = max_concurrent
        self.queue_size = queue_size


class Bulkhead:
    """Bulkhead pattern for resource isolation."""
    
    def __init__(self, config: BulkheadConfig):
        self.config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent)
        self._queue_size = 0
        self._max_queue_size = config.queue_size
        self._lock = asyncio.Lock()
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with bulkhead isolation."""
        # Check queue capacity
        async with self._lock:
            if self._queue_size >= self._max_queue_size:
                raise asyncio.QueueFull("Bulkhead queue is full")
            self._queue_size += 1
        
        try:
            async with self._semaphore:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
        finally:
            async with self._lock:
                self._queue_size -= 1


class TimeoutConfig:
    """Timeout configuration."""
    
    def __init__(self, timeout_seconds: float = 30.0):
        self.timeout_seconds = timeout_seconds


async def with_timeout(func: Callable, config: TimeoutConfig, *args, **kwargs) -> Any:
    """Execute function with timeout."""
    try:
        if asyncio.iscoroutinefunction(func):
            return await asyncio.wait_for(func(*args, **kwargs), timeout=config.timeout_seconds)
        else:
            # For non-async functions, run in executor with timeout
            loop = asyncio.get_event_loop()
            return await asyncio.wait_for(
                loop.run_in_executor(None, func, *args, **kwargs),
                timeout=config.timeout_seconds
            )
    except asyncio.TimeoutError:
        raise TimeoutError(f"Operation timed out after {config.timeout_seconds} seconds")


class ResilienceManager:
    """Combined resilience patterns manager."""
    
    def __init__(
        self,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        retry_config: Optional[RetryConfig] = None,
        bulkhead_config: Optional[BulkheadConfig] = None,
        timeout_config: Optional[TimeoutConfig] = None
    ):
        self.circuit_breaker = CircuitBreaker(circuit_breaker_config) if circuit_breaker_config else None
        self.retry_config = retry_config
        self.bulkhead = Bulkhead(bulkhead_config) if bulkhead_config else None
        self.timeout_config = timeout_config
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with all configured resilience patterns."""
        
        async def wrapped_func(*inner_args, **inner_kwargs):
            # Apply timeout if configured
            if self.timeout_config:
                return await with_timeout(func, self.timeout_config, *inner_args, **inner_kwargs)
            else:
                if asyncio.iscoroutinefunction(func):
                    return await func(*inner_args, **inner_kwargs)
                else:
                    return func(*inner_args, **inner_kwargs)
        
        # Apply bulkhead if configured
        if self.bulkhead:
            execution_func = lambda: self.bulkhead.execute(wrapped_func, *args, **kwargs)
        else:
            execution_func = lambda: wrapped_func(*args, **kwargs)
        
        # Apply circuit breaker if configured
        if self.circuit_breaker:
            circuit_func = lambda: self.circuit_breaker.call(execution_func)
        else:
            circuit_func = execution_func
        
        # Apply retry if configured
        if self.retry_config:
            return await retry_with_backoff(circuit_func, self.retry_config)
        else:
            return await circuit_func()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get metrics from all configured patterns."""
        metrics = {}
        
        if self.circuit_breaker:
            metrics["circuit_breaker"] = self.circuit_breaker.get_metrics()
        
        if self.bulkhead:
            metrics["bulkhead"] = {
                "max_concurrent": self.bulkhead.config.max_concurrent,
                "current_queue_size": self.bulkhead._queue_size,
                "max_queue_size": self.bulkhead._max_queue_size
            }
        
        return metrics


# Convenience function for common resilience patterns
def create_resilient_service(
    failure_threshold: int = 5,
    retry_attempts: int = 3,
    timeout_seconds: float = 30.0,
    max_concurrent: int = 10
) -> ResilienceManager:
    """Create resilience manager with common patterns."""
    return ResilienceManager(
        circuit_breaker_config=CircuitBreakerConfig(failure_threshold=failure_threshold),
        retry_config=RetryConfig(max_attempts=retry_attempts),
        bulkhead_config=BulkheadConfig(max_concurrent=max_concurrent),
        timeout_config=TimeoutConfig(timeout_seconds=timeout_seconds)
    )