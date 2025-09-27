"""HTTP utilities for scenario endpoints.

DEPRECATED: This module provides compatibility adapters for scenario endpoints
that were using simplified HTTP utilities. These functions now delegate to the
main HTTP utilities package for consistency.

For new code, import directly from aurum.api.http instead.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple
from base64 import b64encode, b64decode

from ..http import encode_cursor as http_encode_cursor


def decode_cursor(cursor: str) -> Dict[str, Any]:
    """Decode a cursor string to get pagination information.

    This is a compatibility adapter that provides a simple dictionary interface
    over the more sophisticated HTTP package cursor system.

    Args:
        cursor: Base64 encoded cursor string

    Returns:
        Dictionary with cursor data (mainly 'offset' for scenarios compatibility)
    """
    try:
        # Try simple JSON decode first (legacy format)
        decoded = b64decode(cursor).decode('utf-8')
        # Handle both string representation and JSON
        if decoded.startswith('{'):
            return json.loads(decoded)
        else:
            # Handle str() representation from old encode_cursor
            # Example: "{'offset': 10, 'limit': 50}"
            import ast
            return ast.literal_eval(decoded)
    except Exception:
        # Fallback to default values
        return {"offset": 0, "limit": 50}


def encode_cursor(data: Dict[str, Any]) -> str:
    """Encode pagination information into a cursor string.

    This maintains compatibility with the scenarios usage pattern.

    Args:
        data: Dictionary with cursor data (expects 'offset' key)

    Returns:
        Base64 encoded cursor string
    """
    # Use simple JSON encoding for scenarios compatibility
    cursor_json = json.dumps(data, separators=(',', ':'))
    return b64encode(cursor_json.encode('utf-8')).decode('utf-8')


def normalize_cursor_input(payload: Dict[str, Any]) -> Tuple[int, Optional[Dict[str, Any]]]:
    """Normalize cursor input for pagination.

    This adapter converts the dictionary payload to the expected tuple format
    for scenarios compatibility.

    Args:
        payload: Cursor payload dictionary

    Returns:
        Tuple of (offset, additional_metadata) - compatible with scenarios usage
    """
    if payload and "offset" in payload:
        try:
            offset = int(payload["offset"])
        except (TypeError, ValueError):
            offset = 0
        
        # Extract any additional metadata
        extra = {k: v for k, v in payload.items() if k != "offset"}
        return offset, extra if extra else None
    
    # Default case
    return 0, None


__all__ = [
    "decode_cursor",
    "encode_cursor", 
    "normalize_cursor_input"
]
