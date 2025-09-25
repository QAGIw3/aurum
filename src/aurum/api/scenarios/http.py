"""HTTP utilities for scenario endpoints.

This module provides HTTP utility functions for handling ETags, cursors,
and other HTTP-specific functionality in the scenario API.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from base64 import b64encode, b64decode


def respond_with_etag(
    data: Any,
    etag: Optional[str] = None,
    status_code: int = 200
) -> Dict[str, Any]:
    """Respond with data and ETag header.

    Args:
        data: Response data
        etag: Optional ETag value
        status_code: HTTP status code

    Returns:
        Response dictionary with data and metadata
    """
    response = {
        "data": data,
        "status_code": status_code
    }
    if etag:
        response["etag"] = etag
    return response


def decode_cursor(cursor: str) -> Dict[str, Any]:
    """Decode a cursor string to get pagination information.

    Args:
        cursor: Base64 encoded cursor string

    Returns:
        Dictionary with cursor data
    """
    try:
        decoded = b64decode(cursor).decode('utf-8')
        # This is a placeholder implementation
        # In a real implementation, this would parse the cursor data
        return {"offset": 0, "limit": 50}
    except Exception:
        return {"offset": 0, "limit": 50}


def encode_cursor(offset: int, limit: int, **kwargs) -> str:
    """Encode pagination information into a cursor string.

    Args:
        offset: Pagination offset
        limit: Pagination limit
        **kwargs: Additional cursor data

    Returns:
        Base64 encoded cursor string
    """
    cursor_data = {
        "offset": offset,
        "limit": limit,
        **kwargs
    }
    cursor_json = str(cursor_data)
    return b64encode(cursor_json.encode('utf-8')).decode('utf-8')


def normalize_cursor_input(cursor: Optional[str] = None) -> Dict[str, Any]:
    """Normalize cursor input for pagination.

    Args:
        cursor: Optional cursor string

    Returns:
        Normalized cursor data
    """
    if cursor:
        return decode_cursor(cursor)
    return {"offset": 0, "limit": 50}


__all__ = [
    "respond_with_etag",
    "decode_cursor",
    "encode_cursor",
    "normalize_cursor_input"
]
