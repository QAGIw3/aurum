"""Performance profiling tools for critical paths with environment-based toggles."""

from __future__ import annotations

import asyncio
import functools
import os
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Callable, Dict, Optional, Union

try:
    import pyinstrument
    PYINSTRUMENT_AVAILABLE = True
except ImportError:
    pyinstrument = None  # type: ignore
    PYINSTRUMENT_AVAILABLE = False

try:
    import yappi
    YAPPI_AVAILABLE = True
except ImportError:
    yappi = None  # type: ignore
    YAPPI_AVAILABLE = False


# Environment variables for profiling control
PROFILING_ENABLED = os.getenv("AURUM_PROFILING_ENABLED", "false").lower() in ("true", "1", "yes")
PROFILING_BACKEND = os.getenv("AURUM_PROFILING_BACKEND", "pyinstrument")  # pyinstrument, yappi
PROFILING_THRESHOLD_MS = int(os.getenv("AURUM_PROFILING_THRESHOLD_MS", "100"))
PROFILING_OUTPUT_DIR = os.getenv("AURUM_PROFILING_OUTPUT_DIR", "profiles/")


def is_profiling_enabled() -> bool:
    """Check if profiling is enabled via environment variables."""
    return PROFILING_ENABLED


def get_profiling_backend() -> str:
    """Get the configured profiling backend."""
    return PROFILING_BACKEND if PROFILING_ENABLED else "disabled"


@contextmanager
def ProfileContext(
    operation: str,
    threshold_ms: Optional[int] = None,
    async_context: bool = False
):
    """Context manager for profiling operations.

    Args:
        operation: Name of the operation being profiled
        threshold_ms: Minimum duration to profile (default from env)
        async_context: Whether this is an async context
    """
    if not is_profiling_enabled():
        yield
        return

    threshold = threshold_ms or PROFILING_THRESHOLD_MS

    if get_profiling_backend() == "pyinstrument" and PYINSTRUMENT_AVAILABLE:
        with _pyinstrument_context(operation, threshold):
            yield
    elif get_profiling_backend() == "yappi" and YAPPI_AVAILABLE:
        with _yappi_context(operation, threshold):
            yield
    else:
        yield


@asynccontextmanager
async def ProfileAsyncContext(
    operation: str,
    threshold_ms: Optional[int] = None
):
    """Async context manager for profiling operations."""
    if not is_profiling_enabled():
        yield
        return

    threshold = threshold_ms or PROFILING_THRESHOLD_MS

    if get_profiling_backend() == "pyinstrument" and PYINSTRUMENT_AVAILABLE:
        async with _pyinstrument_async_context(operation, threshold):
            yield
    elif get_profiling_backend() == "yappi" and YAPPI_AVAILABLE:
        async with _yappi_async_context(operation, threshold):
            yield
    else:
        yield


def profile_function(
    operation: Optional[str] = None,
    threshold_ms: Optional[int] = None
):
    """Decorator to profile a function."""
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            op_name = operation or f"{func.__module__}.{func.__qualname__}"

            with ProfileContext(op_name, threshold_ms):
                return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            op_name = operation or f"{func.__module__}.{func.__qualname__}"

            async with ProfileAsyncContext(op_name, threshold_ms):
                return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper

    return decorator


# PyInstrument implementation
@contextmanager
def _pyinstrument_context(operation: str, threshold_ms: int):
    """PyInstrument context manager."""
    if not pyinstrument:
        yield
        return

    profiler = pyinstrument.Profiler()
    profiler.start()

    start_time = time.perf_counter()

    try:
        yield
    finally:
        profiler.stop()
        end_time = time.perf_counter()
        duration = (end_time - start_time) * 1000

        if duration >= threshold_ms:
            # Create output directory if it doesn't exist
            os.makedirs(PROFILING_OUTPUT_DIR, exist_ok=True)

            # Generate filename with timestamp
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"{PROFILING_OUTPUT_DIR}/{operation}_{timestamp}.html"

            # Write profile
            with open(filename, 'w') as f:
                f.write(profiler.output_html())

            print(f"[PROFILING] {operation} took {duration:.2f}ms - saved to {filename}")


@asynccontextmanager
async def _pyinstrument_async_context(operation: str, threshold_ms: int):
    """Async PyInstrument context manager."""
    if not pyinstrument:
        yield
        return

    profiler = pyinstrument.Profiler(async_mode="enabled")
    profiler.start()

    start_time = time.perf_counter()

    try:
        yield
    finally:
        profiler.stop()
        end_time = time.perf_counter()
        duration = (end_time - start_time) * 1000

        if duration >= threshold_ms:
            # Create output directory if it doesn't exist
            os.makedirs(PROFILING_OUTPUT_DIR, exist_ok=True)

            # Generate filename with timestamp
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"{PROFILING_OUTPUT_DIR}/{operation}_{timestamp}.html"

            # Write profile
            with open(filename, 'w') as f:
                f.write(profiler.output_html())

            print(f"[PROFILING] {operation} took {duration:.2f}ms - saved to {filename}")


# Yappi implementation
@contextmanager
def _yappi_context(operation: str, threshold_ms: int):
    """Yappi context manager."""
    if not yappi:
        yield
        return

    yappi.start()

    start_time = time.perf_counter()

    try:
        yield
    finally:
        yappi.stop()
        end_time = time.perf_counter()
        duration = (end_time - start_time) * 1000

        if duration >= threshold_ms:
            # Create output directory if it doesn't exist
            os.makedirs(PROFILING_OUTPUT_DIR, exist_ok=True)

            # Generate filename with timestamp
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"{PROFILING_OUTPUT_DIR}/{operation}_{timestamp}.prof"

            # Get stats and write to file
            stats = yappi.get_func_stats()
            stats.save(filename, type='callgrind')

            print(f"[PROFILING] {operation} took {duration:.2f}ms - saved to {filename}")


@asynccontextmanager
async def _yappi_async_context(operation: str, threshold_ms: int):
    """Async Yappi context manager."""
    if not yappi:
        yield
        return

    yappi.start()

    start_time = time.perf_counter()

    try:
        yield
    finally:
        yappi.stop()
        end_time = time.perf_counter()
        duration = (end_time - start_time) * 1000

        if duration >= threshold_ms:
            # Create output directory if it doesn't exist
            os.makedirs(PROFILING_OUTPUT_DIR, exist_ok=True)

            # Generate filename with timestamp
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"{PROFILING_OUTPUT_DIR}/{operation}_{timestamp}.prof"

            # Get stats and write to file
            stats = yappi.get_func_stats()
            stats.save(filename, type='callgrind')

            print(f"[PROFILING] {operation} took {duration:.2f}ms - saved to {filename}")


def enable_profiling(backend: str = "pyinstrument") -> None:
    """Enable profiling globally."""
    os.environ["AURUM_PROFILING_ENABLED"] = "true"
    os.environ["AURUM_PROFILING_BACKEND"] = backend


def disable_profiling() -> None:
    """Disable profiling globally."""
    os.environ["AURUM_PROFILING_ENABLED"] = "false"


def get_profiling_stats() -> Dict[str, Any]:
    """Get current profiling configuration and status."""
    return {
        "enabled": is_profiling_enabled(),
        "backend": get_profiling_backend(),
        "threshold_ms": PROFILING_THRESHOLD_MS,
        "output_dir": PROFILING_OUTPUT_DIR,
        "pyinstrument_available": PYINSTRUMENT_AVAILABLE,
        "yappi_available": YAPPI_AVAILABLE,
    }
