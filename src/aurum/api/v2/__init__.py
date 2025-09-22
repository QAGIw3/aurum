"""Aurum API v2 - Enhanced version with improved features.

This module contains the v2 API implementations with:
- Cursor-only pagination
- RFC 7807 compliant error responses
- Enhanced ETag support
- Improved observability
- Better error handling
- Consistent response formats
"""

from __future__ import annotations

from . import scenarios, curves

__all__ = ["scenarios", "curves"]
