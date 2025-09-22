"""Aurum API v2 - Enhanced version with improved features.

This module contains the v2 API implementations with:
- Cursor-only pagination
- RFC 7807 compliant error responses
- Enhanced ETag support
- Improved observability
- Better error handling
- Consistent response formats
- Tenant context enforcement
- Link headers for navigation
- Rate limiting headers
"""

from __future__ import annotations

import os as _os

if _os.getenv("AURUM_API_V2_LIGHT_INIT", "0") == "1":
    __all__: list[str] = []
else:
    from . import scenarios, curves, metadata, iso, eia, ppa, drought, admin

    __all__ = ["scenarios", "curves", "metadata", "iso", "eia", "ppa", "drought", "admin"]
