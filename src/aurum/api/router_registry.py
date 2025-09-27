"""Central registry for versioned API routers."""

from __future__ import annotations

import asyncio
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

    # Enhanced V1 split flag handling with runtime toggle support
    from .features.v1_split_integration import get_v1_split_manager
    
    # Get the V1 split manager (this uses runtime feature flags if available, falls back to env vars)
    v1_manager = get_v1_split_manager()
    
    # Map environment variables to flag names for backward compatibility
    optional_modules = {
        "AURUM_API_V1_SPLIT_EIA": ("eia", "aurum.api.v1.eia"),
        "AURUM_API_V1_SPLIT_ISO": ("iso", "aurum.api.v1.iso"),
        "AURUM_API_V1_SPLIT_PPA": ("ppa", "aurum.api.v1.ppa"),
        "AURUM_API_V1_SPLIT_DROUGHT": ("drought", "aurum.api.v1.drought"),
        "AURUM_API_V1_SPLIT_ADMIN": ("admin", "aurum.api.v1.admin"),
    }

    for env_var, (flag_name, module_path) in optional_modules.items():
        # Check if enabled via runtime feature flags or environment variable
        try:
            # Try async check if possible (this will be a sync fallback for now)
            is_enabled = _check_v1_flag_sync(v1_manager, flag_name, env_var)
        except Exception as e:
            logger.warning(f"Error checking V1 flag {flag_name}, falling back to env var: {e}")
            is_enabled = os.getenv(env_var, "0") == "1"
        
        if is_enabled:
            if module_path in seen:
                continue
            router = _try_import_router(module_path)
            if router is not None:
                specs.append(RouterSpec(router=router, name=module_path))
                seen.add(module_path)
                logger.info(f"Loaded V1 split router: {module_path} (flag: {flag_name})")

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


def _check_v1_flag_sync(v1_manager, flag_name: str, env_var: str) -> bool:
    """Synchronous helper to check V1 flag status with fallback to environment variable."""
    try:
        # Check if we can do async call (this would need proper async context in real usage)
        # For now, we'll use a sync fallback that checks the feature flag store directly
        if hasattr(v1_manager, 'feature_manager') and v1_manager.feature_manager:
            feature_key = f"v1_split_{flag_name}"
            # Try to access store directly for sync check
            if hasattr(v1_manager.feature_manager, 'store') and hasattr(v1_manager.feature_manager.store, '_flags'):
                flag = v1_manager.feature_manager.store._flags.get(feature_key)
                if flag:
                    return flag.status.value == "enabled"
        
        # Fallback to environment variable
        config = v1_manager.V1_SPLIT_FLAGS.get(flag_name)
        if config:
            env_value = os.getenv(config.env_var, "0")
            return env_value == "1" or config.default_enabled
        else:
            return os.getenv(env_var, "0") == "1"
            
    except Exception as e:
        logger.debug(f"Error in sync V1 flag check for {flag_name}: {e}")
        return os.getenv(env_var, "0") == "1"


async def get_v1_router_specs_async(_settings: AurumSettings) -> list[RouterSpec]:
    """Async version of get_v1_router_specs that properly supports runtime flag checks."""
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

    # Enhanced V1 split flag handling with runtime toggle support
    from .features.v1_split_integration import get_v1_split_manager
    
    v1_manager = get_v1_split_manager()
    
    # Ensure V1 flags are initialized
    try:
        await v1_manager.initialize_v1_flags()
    except Exception as e:
        logger.warning(f"Failed to initialize V1 flags: {e}")
    
    # Map flag names to modules  
    flag_modules = {
        "eia": "aurum.api.v1.eia",
        "iso": "aurum.api.v1.iso", 
        "drought": "aurum.api.v1.drought",
        "admin": "aurum.api.v1.admin",
        "ppa": "aurum.api.v1.ppa",  # Will be enabled by default
    }

    for flag_name, module_path in flag_modules.items():
        try:
            is_enabled = await v1_manager.is_v1_split_enabled(flag_name)
        except Exception as e:
            logger.warning(f"Error checking V1 flag {flag_name}: {e}")
            # Fallback to environment variable
            config = v1_manager.V1_SPLIT_FLAGS.get(flag_name)
            if config:
                env_value = os.getenv(config.env_var, "0")
                is_enabled = env_value == "1" or config.default_enabled
            else:
                is_enabled = False
        
        if is_enabled:
            if module_path in seen:
                continue
            router = _try_import_router(module_path)
            if router is not None:
                specs.append(RouterSpec(router=router, name=module_path))
                seen.add(module_path)
                logger.info(f"Loaded V1 split router: {module_path} (flag: {flag_name})")

    # Continue with supplemental modules
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
