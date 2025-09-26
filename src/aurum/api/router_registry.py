"""Central registry for versioned API routers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, Iterable, Mapping

from fastapi import APIRouter

from aurum.core import AurumSettings

logger = logging.getLogger(__name__)

_LIGHTWEIGHT_ROUTER_CACHE: dict[str, APIRouter] = {}


@dataclass(frozen=True)
class RouterSpec:
    """Definition for including a router in the FastAPI application."""

    router: APIRouter
    include_kwargs: Mapping[str, Any] = field(default_factory=dict)
    name: str | None = None


def _try_import_router(module_path: str, *, attr: str = "router") -> APIRouter | None:
    """Import a router from the given module path, logging failures."""

    if os.getenv("AURUM_API_LIGHT_INIT", "0") == "1":
        router = _LIGHTWEIGHT_ROUTER_CACHE.get(module_path)
        if router is None:
            router = APIRouter()
            _LIGHTWEIGHT_ROUTER_CACHE[module_path] = router
        return router

    try:
        module = import_module(module_path)
    except Exception:  # pragma: no cover - defensive guard
        logger.warning("Failed to import router module '%s'", module_path, exc_info=True)
        return None

    router = getattr(module, attr, None)
    if isinstance(router, APIRouter):
        return router

    logger.warning("Module '%s' does not expose '%s' of type APIRouter", module_path, attr)
    return None


def _build_specs(module_paths: Iterable[str], *, seen: set[str] | None = None) -> list[RouterSpec]:
    """Build router specs for the provided module paths, de-duplicating entries."""

    specs: list[RouterSpec] = []
    tracked = seen if seen is not None else set()
    for path in module_paths:
        if path in tracked:
            continue
        router = _try_import_router(path)
        if router is not None:
            specs.append(RouterSpec(router=router, name=path))
            tracked.add(path)
    return specs


def get_v1_router_specs(_settings: AurumSettings) -> list[RouterSpec]:
    """Return the list of v1 routers to include for the given settings."""
    seen: set[str] = set()
    specs: list[RouterSpec] = []

    curves_module = "aurum.api.v1.curves"
    specs.extend(_build_specs((curves_module,), seen=seen))

    mandatory_modules = (
        "aurum.api.scenarios.scenarios",
        "aurum.api.metadata",
        "aurum.api.v1.ppa",
    )

    specs.extend(_build_specs(mandatory_modules, seen=seen))

    optional_modules = {
        "AURUM_API_V1_SPLIT_EIA": "aurum.api.v1.eia",
        "AURUM_API_V1_SPLIT_ISO": "aurum.api.v1.iso",
        "AURUM_API_V1_SPLIT_PPA": "aurum.api.v1.ppa",
        "AURUM_API_V1_SPLIT_DROUGHT": "aurum.api.v1.drought",
        "AURUM_API_V1_SPLIT_ADMIN": "aurum.api.v1.admin",
    }

    for env_var, module_path in optional_modules.items():
        if os.getenv(env_var, "0") == "1":
            if module_path in seen:
                continue
            router = _try_import_router(module_path)
            if router is not None:
                specs.append(RouterSpec(router=router, name=module_path))
                seen.add(module_path)

    supplemental_modules: tuple[tuple[str, Mapping[str, Any]], ...] = (
        ("aurum.api.handlers.external", {}),
        ("aurum.api.database.performance", {"prefix": "/v1/admin/db", "tags": ["Database"]}),
        ("aurum.api.database.query_analysis", {"prefix": "/v1/admin/db", "tags": ["Database"]}),
        ("aurum.api.database.optimization", {"prefix": "/v1/admin/db", "tags": ["Database"]}),
        ("aurum.api.database.connections", {"prefix": "/v1/admin/db", "tags": ["Database"]}),
        ("aurum.api.database.health", {"prefix": "/v1/admin/db", "tags": ["Database"]}),
        ("aurum.api.database.trino_admin", {"prefix": "/v1/admin/trino", "tags": ["Trino"]}),
    )

    for module_path, include_kwargs in supplemental_modules:
        if module_path in seen:
            continue
        router = _try_import_router(module_path)
        if router is not None:
            specs.append(
                RouterSpec(
                    router=router,
                    include_kwargs=dict(include_kwargs),
                    name=module_path,
                )
            )
            seen.add(module_path)

    return specs


def get_v2_router_specs(_settings: AurumSettings) -> list[RouterSpec]:
    """Return the list of v2 routers to include for the given settings."""
    module_paths = (
        "aurum.api.v2.scenarios",
        "aurum.api.v2.curves",
        "aurum.api.v2.metadata",
        "aurum.api.v2.iso",
        "aurum.api.v2.eia",
        "aurum.api.v2.ppa",
        "aurum.api.v2.drought",
        "aurum.api.v2.admin",
    )
    return _build_specs(module_paths)


__all__ = [
    "RouterSpec",
    "get_v1_router_specs",
    "get_v2_router_specs",
]
