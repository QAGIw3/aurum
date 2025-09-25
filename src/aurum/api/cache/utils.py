"""Cache utility helpers for API services.

These utilities standardize how API code interacts with CacheManager
without pulling CacheManager into pure business logic.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional

from .cache import CacheManager


async def cache_get_or_set(
    manager: Optional[CacheManager],
    key_suffix: str,
    supplier: Callable[[], Awaitable[Any]] | Callable[[], Any],
    ttl_seconds: int,
) -> Any:
    """Get from cache or set the value from a supplier.

    If `manager` is None, simply computes the value.
    The `supplier` can be sync or async.
    """
    if isinstance(manager, CacheManager):
        cached = await manager.get_cache_entry(key_suffix)
        if cached is not None:
            return cached
        value = supplier()
        if asyncio.iscoroutine(value):
            value = await value
        await manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds)
        return value

    value = supplier()
    if asyncio.iscoroutine(value):
        value = await value
    return value


def cache_get_or_set_sync(
    manager: Optional[CacheManager],
    key_suffix: str,
    supplier: Callable[[], Any],
    ttl_seconds: int,
) -> Any:
    """Synchronous variant of cache_get_or_set.

    Executes cache operations using a short-lived event loop if necessary.
    Intended for transitional use in synchronous service paths.
    """
    if isinstance(manager, CacheManager):
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
                    existing = _loop.run_until_complete(manager.get_cache_entry(key_suffix))
                    if existing is not None:
                        result_box["result"] = existing
                        return
                    value = supplier()
                    _loop.run_until_complete(manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds))
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
            existing = loop2.run_until_complete(manager.get_cache_entry(key_suffix))
            if existing is not None:
                return existing
            value = supplier()
            loop2.run_until_complete(manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds))
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


def cache_get_sync(manager: Optional[CacheManager], key_suffix: str) -> Any:
    """Synchronously get a cache entry if manager is available, else None."""
    if not isinstance(manager, CacheManager):
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
                result_box["result"] = _loop.run_until_complete(manager.get_cache_entry(key_suffix))
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
        return loop2.run_until_complete(manager.get_cache_entry(key_suffix))
    finally:
        try:
            loop2.stop()
        finally:
            loop2.close()
            asyncio.set_event_loop(None)


def cache_set_sync(manager: Optional[CacheManager], key_suffix: str, value: Any, ttl_seconds: int) -> None:
    """Synchronously set a cache entry if manager is available."""
    if not isinstance(manager, CacheManager):
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
                _loop.run_until_complete(manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds))
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

    loop2 = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop2)
        loop2.run_until_complete(manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl_seconds))
    finally:
        try:
            loop2.stop()
        finally:
            loop2.close()
            asyncio.set_event_loop(None)
