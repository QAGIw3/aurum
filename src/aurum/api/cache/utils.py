"""Cache utility helpers for API services.

These utilities standardize how API code interacts with CacheManager
without pulling CacheManager into pure business logic.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Awaitable, Callable, Optional


async def cache_get_or_set(
    manager: Optional[Any],
    key_suffix: str,
    supplier: Callable[[], Awaitable[Any]] | Callable[[], Any],
    ttl_seconds: int,
) -> Any:
    """Get from cache or set the value from a supplier.

    If `manager` is None, simply computes the value.
    The `supplier` can be sync or async.
    """
    if manager is not None:
        # Prefer unified interface if available
        if callable(getattr(manager, "get_cache_entry", None)):
            cached = await manager.get_cache_entry(key_suffix)
        elif callable(getattr(manager, "get", None)):
            cached = await manager.get(key_suffix)
        else:
            cached = None
        if cached is not None:
            return cached
        value = supplier()
        if asyncio.iscoroutine(value):
            value = await value
        if callable(getattr(manager, "set_cache_entry", None)):
            await manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds)
        elif callable(getattr(manager, "set", None)):
            await manager.set(key_suffix, value, ttl_seconds)
        else:
            # Unknown manager shape; fall back to computing only
            return value
        return value

    value = supplier()
    if asyncio.iscoroutine(value):
        value = await value
    return value


def cache_get_or_set_sync(
    manager: Optional[Any],
    key_suffix: str,
    supplier: Callable[[], Any],
    ttl_seconds: int,
) -> Any:
    """Synchronous variant of cache_get_or_set.

    Executes cache operations using a short-lived event loop if necessary.
    Intended for transitional use in synchronous service paths.
    """
    if manager is not None:
        try:
            loop = asyncio.get_running_loop()
            running = loop.is_running()
        except RuntimeError:
            running = False
            loop = None  # type: ignore

        if running:
            # Run get/set in a dedicated loop in a worker thread
            from threading import Thread

            result_box: dict[str, Any] = {}
            error_box: dict[str, BaseException] = {}

            def _runner():
                _loop = asyncio.new_event_loop()
                asyncio.set_event_loop(_loop)
                try:
                    if callable(getattr(manager, "get_cache_entry", None)):
                        existing = _loop.run_until_complete(manager.get_cache_entry(key_suffix))
                    elif callable(getattr(manager, "get", None)):
                        existing = _loop.run_until_complete(manager.get(key_suffix))
                    else:
                        existing = None
                    if existing is not None:
                        result_box["result"] = existing
                        return
                    value = supplier()
                    if callable(getattr(manager, "set_cache_entry", None)):
                        _loop.run_until_complete(manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds))
                    elif callable(getattr(manager, "set", None)):
                        _loop.run_until_complete(manager.set(key_suffix, value, ttl_seconds))
                    else:
                        result_box["result"] = value
                        return
                    result_box["result"] = value
                except BaseException as exc:
                    error_box["error"] = exc
                finally:
                    try:
                        _loop.stop()
                    finally:
                        _loop.close()
                        asyncio.set_event_loop(None)

            t = Thread(target=_runner, daemon=True)
            t.start()
            t.join()
            if error_box:
                raise error_box["error"]
            return result_box.get("result")

        # No running loop in this thread; operate directly
        loop2 = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop2)
            if callable(getattr(manager, "get_cache_entry", None)):
                existing = loop2.run_until_complete(manager.get_cache_entry(key_suffix))
            elif callable(getattr(manager, "get", None)):
                existing = loop2.run_until_complete(manager.get(key_suffix))
            else:
                existing = None
            if existing is not None:
                return existing
            value = supplier()
            if callable(getattr(manager, "set_cache_entry", None)):
                loop2.run_until_complete(manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds))
            elif callable(getattr(manager, "set", None)):
                loop2.run_until_complete(manager.set(key_suffix, value, ttl_seconds))
            return value
        finally:
            try:
                loop2.stop()
            finally:
                loop2.close()
                asyncio.set_event_loop(None)

    # No manager available
    return supplier()


__all__ = ["cache_get_or_set", "cache_get_or_set_sync"]


def cache_get_sync(manager: Optional[Any], key_suffix: str) -> Any:
    """Synchronously get a cache entry if manager is available, else None."""
    if manager is None:
        return None
    try:
        loop = asyncio.get_running_loop()
        running = loop.is_running()
    except RuntimeError:
        running = False
        loop = None  # type: ignore

    if running:
        from threading import Thread

        result_box: dict[str, Any] = {}
        error_box: dict[str, BaseException] = {}

        def _runner():
            _loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_loop)
            try:
                if callable(getattr(manager, "get_cache_entry", None)):
                    result_box["result"] = _loop.run_until_complete(manager.get_cache_entry(key_suffix))
                elif callable(getattr(manager, "get", None)):
                    result_box["result"] = _loop.run_until_complete(manager.get(key_suffix))
                else:
                    result_box["result"] = None
            except BaseException as exc:
                error_box["error"] = exc
            finally:
                try:
                    _loop.stop()
                finally:
                    _loop.close()
                    asyncio.set_event_loop(None)

        t = Thread(target=_runner, daemon=True)
        t.start()
        t.join()
        if error_box:
            raise error_box["error"]
        return result_box.get("result")

    loop2 = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop2)
        if callable(getattr(manager, "get_cache_entry", None)):
            return loop2.run_until_complete(manager.get_cache_entry(key_suffix))
        if callable(getattr(manager, "get", None)):
            return loop2.run_until_complete(manager.get(key_suffix))
        return None
    finally:
        try:
            loop2.stop()
        finally:
            loop2.close()
            asyncio.set_event_loop(None)


def cache_set_sync(manager: Optional[Any], key_suffix: str, value: Any, ttl_seconds: int) -> None:
    """Synchronously set a cache entry if manager is available."""
    if manager is None:
        return
    try:
        loop = asyncio.get_running_loop()
        running = loop.is_running()
    except RuntimeError:
        running = False
        loop = None  # type: ignore

    if running:
        from threading import Thread

        error_box: dict[str, BaseException] = {}

        def _runner():
            _loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_loop)
            try:
                if callable(getattr(manager, "set_cache_entry", None)):
                    _loop.run_until_complete(manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds))
                elif callable(getattr(manager, "set", None)):
                    _loop.run_until_complete(manager.set(key_suffix, value, ttl_seconds))
            except BaseException as exc:
                error_box["error"] = exc
            finally:
                try:
                    _loop.stop()
                finally:
                    _loop.close()
                    asyncio.set_event_loop(None)

        t = Thread(target=_runner, daemon=True)
        t.start()
        t.join()
        if error_box:
            raise error_box["error"]
        return

    # No running loop; operate directly
    loop2 = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop2)
        if callable(getattr(manager, "set_cache_entry", None)):
            loop2.run_until_complete(manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds))
        elif callable(getattr(manager, "set", None)):
            loop2.run_until_complete(manager.set(key_suffix, value, ttl_seconds))
    finally:
        try:
            loop2.stop()
        finally:
            loop2.close()
            asyncio.set_event_loop(None)


def cache_key(params: dict[str, Any]) -> str:
    """Generate a cache key from parameters.

    Args:
        params: Dictionary of parameters to hash

    Returns:
        SHA256 hash of the sorted JSON representation of params
    """
    payload = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def iso_lmp_cache_key(operation: str, params: dict[str, Any], sql: str, namespace: str) -> str:
    """Generate a cache key for ISO LMP operations.

    Args:
        operation: The operation name (e.g., 'lmp_last_24h', 'lmp_hourly')
        params: Parameters for the operation
        sql: SQL query string
        namespace: Cache namespace

    Returns:
        Formatted cache key string
    """
    payload = dict(params)
    payload["sql"] = sql
    prefix = f"{namespace}:" if namespace else ""
    return f"{prefix}iso-lmp:{operation}:{cache_key(payload)}"


def iso_lmp_effective_ttl(cache_ttl: int, settings_ttl: int | None = None, override_ttl: int | None = None) -> int:
    """Calculate effective TTL for ISO LMP cache entries.

    Args:
        cache_ttl: Base cache TTL from configuration
        settings_ttl: TTL from settings (if provided)
        override_ttl: Override TTL (if provided)

    Returns:
        Effective TTL in seconds (minimum 0)
    """
    if override_ttl is not None:
        return max(0, override_ttl)
    if settings_ttl is not None:
        return max(0, settings_ttl)
    return max(0, cache_ttl)
