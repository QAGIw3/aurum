"""Route utilities for scenario endpoints.

This module provides utility functions for handling tenant resolution
and other common route operations in the scenario API.
"""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import Request


def _resolve_tenant(request: Request) -> str:
    """Resolve tenant ID from request.

    Args:
        request: FastAPI request object

    Returns:
        Tenant identifier

    Raises:
        HTTPException: If tenant cannot be resolved
    """
    # This is a placeholder implementation
    # In a real implementation, this would extract tenant info from
    # request headers, JWT token, or other authentication context
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        # Try to get from headers
        tenant_id = request.headers.get("x-tenant-id")
    if not tenant_id:
        raise ValueError("Tenant ID not found in request")
    return tenant_id


def _resolve_tenant_optional(request: Request) -> Optional[str]:
    """Resolve tenant ID from request, optionally.

    Args:
        request: FastAPI request object

    Returns:
        Tenant identifier or None if not found
    """
    # This is a placeholder implementation
    # In a real implementation, this would extract tenant info from
    # request headers, JWT token, or other authentication context
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        # Try to get from headers
        tenant_id = request.headers.get("x-tenant-id")
    return tenant_id


__all__ = ["_resolve_tenant", "_resolve_tenant_optional"]
