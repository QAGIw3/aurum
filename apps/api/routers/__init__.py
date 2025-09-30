"""Local router re-exports for the unified API app.

These modules import routers from `src/aurum/api/v2/*` to avoid deep imports in the app.
Replace re-exports with local implementations over time.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


def _get_router(module_path: str, attr: str = "router") -> Any:
    module = import_module(module_path)
    return getattr(module, attr)


# Re-export commonly used routers
curves = _get_router("aurum.api.v2.curves")
scenarios = _get_router("aurum.api.v2.scenarios")
catalog = _get_router("aurum.api.v2.metadata")
market = _get_router("apps.api.routers.market")
admin = _get_router("aurum.api.v2.admin")


