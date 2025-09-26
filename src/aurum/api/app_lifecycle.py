from __future__ import annotations

"""Application lifecycle helpers split out of the app factory.

- Trino client lifecycle wiring
- Prometheus metrics endpoint registration
"""

import atexit
import contextlib
import logging
import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Response

try:  # pragma: no cover - optional dependency
    from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
    from prometheus_client import multiprocess
except Exception:  # pragma: no cover - handled gracefully at runtime
    CONTENT_TYPE_LATEST = "text/plain; charset=utf-8"
    CollectorRegistry = None  # type: ignore[assignment]
    generate_latest = None  # type: ignore[assignment]
    multiprocess = None  # type: ignore[assignment]

from aurum.core import AurumSettings

LOGGER = logging.getLogger(__name__)


def register_trino_lifecycle(app: FastAPI) -> None:
    """Ensure Trino clients are gracefully closed with the application."""
    try:
        from .database.trino_client import HybridTrinoClientManager  # type: ignore
    except Exception:  # pragma: no cover - optional dependency for tests without DB stack
        return

    previous_lifespan = getattr(app.router, "lifespan_context", None)

    @contextlib.asynccontextmanager
    async def _trino_lifespan(_app: FastAPI):
        manager = HybridTrinoClientManager.get_instance()
        try:
            yield
        finally:
            await manager.close_all()

    if previous_lifespan is None:
        app.router.lifespan_context = _trino_lifespan
    else:
        @contextlib.asynccontextmanager
        async def _chained_lifespan(_app: FastAPI):
            async with previous_lifespan(_app):
                async with _trino_lifespan(_app):
                    yield

        app.router.lifespan_context = _chained_lifespan

    manager = HybridTrinoClientManager.get_instance()

    try:
        atexit.register(manager.close_all_sync)
    except Exception:  # pragma: no cover - best effort cleanup
        pass


def register_metrics_endpoint(app: FastAPI, settings: AurumSettings) -> None:
    """Register Prometheus metrics endpoint with optional multiprocess support."""

    metrics_cfg = getattr(settings.api, "metrics", None)
    if metrics_cfg is None or not getattr(metrics_cfg, "enabled", False):
        return

    metrics_path = getattr(metrics_cfg, "path", "/metrics") or "/metrics"
    if not metrics_path.startswith("/"):
        metrics_path = f"/{metrics_path}"

    existing_routes = [
        route for route in list(app.router.routes)
        if getattr(route, "path", None) == metrics_path
    ]
    for route in existing_routes:
        try:
            app.router.routes.remove(route)
        except ValueError:  # pragma: no cover - defensive
            continue

    @app.get(metrics_path)
    async def prometheus_metrics() -> Response:  # type: ignore[override]
        if generate_latest is None:
            raise HTTPException(status_code=503, detail="Prometheus instrumentation not available")

        registry = None
        multiproc_dir = os.getenv("PROMETHEUS_MULTIPROC_DIR")
        if multiproc_dir and multiprocess and CollectorRegistry is not None:
            registry = CollectorRegistry()
            try:
                multiprocess.MultiProcessCollector(registry)
            except Exception as exc:  # pragma: no cover - unlikely but defensive
                LOGGER.warning("prometheus_multiprocess_init_failed", exc_info=exc)
                registry = None

        try:
            payload = generate_latest(registry) if registry is not None else generate_latest()
        except Exception as exc:  # pragma: no cover - guard against collector errors
            LOGGER.warning("prometheus_generate_latest_failed", exc_info=exc)
            raise HTTPException(status_code=500, detail="Failed to render metrics") from exc

        return Response(content=payload, media_type=CONTENT_TYPE_LATEST)


__all__ = ["register_trino_lifecycle", "register_metrics_endpoint"]

