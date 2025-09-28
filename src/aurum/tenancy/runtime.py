"""Runtime helpers for accessing the process-wide tenant manager."""
from __future__ import annotations

from typing import Optional

from .tenant_manager import TenantManager

_TENANT_MANAGER: Optional[TenantManager] = None


def set_tenant_manager(manager: Optional[TenantManager]) -> None:
    """Register the active tenant manager for the current process."""
    global _TENANT_MANAGER
    _TENANT_MANAGER = manager


def get_tenant_manager() -> Optional[TenantManager]:
    """Return the active tenant manager if one has been registered."""
    return _TENANT_MANAGER


__all__ = ["set_tenant_manager", "get_tenant_manager"]
