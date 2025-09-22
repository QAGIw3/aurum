"""Helper utilities for consistent cursor pagination in v2 endpoints."""

from __future__ import annotations

import time
from typing import Dict, Optional, Tuple

from fastapi import HTTPException
from starlette.datastructures import URL

from ..http import (
    MAX_PAGE_SIZE,
    create_pagination_metadata,
    decode_cursor,
    encode_cursor,
    normalize_cursor_input,
)


def _normalize_filters(filters: Optional[Dict[str, object]]) -> Dict[str, object]:
    if not filters:
        return {}
    return {key: value for key, value in filters.items() if value is not None}


def resolve_pagination(
    *,
    cursor: Optional[str],
    limit: Optional[int],
    default_limit: int,
    max_limit: int = MAX_PAGE_SIZE,
    filters: Optional[Dict[str, object]] = None,
) -> Tuple[int, int]:
    """Decode pagination inputs and return ``(offset, limit)`` pair."""

    effective_limit = limit or default_limit
    if effective_limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")
    effective_limit = min(effective_limit, max_limit)

    normalized_filters = _normalize_filters(filters)

    if not cursor:
        return 0, effective_limit

    payload = decode_cursor(cursor)
    offset, extras = normalize_cursor_input(payload)

    if offset is None:
        raise HTTPException(status_code=400, detail="Cursor payload missing offset")
    if offset < 0:
        raise HTTPException(status_code=400, detail="Cursor offset must be non-negative")

    cursor_filters = _normalize_filters((extras or {}).get("filters")) if extras else {}
    if cursor_filters and cursor_filters != normalized_filters:
        raise HTTPException(status_code=400, detail="Cursor filters do not match request filters")

    cursor_limit = None
    if extras and "limit" in extras:
        try:
            cursor_limit = int(extras["limit"])
        except (TypeError, ValueError) as exc:  # pragma: no cover
            raise HTTPException(status_code=400, detail="Cursor limit is invalid") from exc

    if cursor_limit:
        effective_limit = min(cursor_limit, effective_limit, max_limit)

    return offset, effective_limit


def build_next_cursor(
    *,
    offset: int,
    limit: int,
    has_more: bool,
    filters: Optional[Dict[str, object]] = None,
) -> Optional[str]:
    """Generate the next cursor payload when additional pages exist."""

    if not has_more:
        return None

    payload = {
        "offset": offset + limit,
        "limit": limit,
        "filters": _normalize_filters(filters),
        "ts": time.time(),
    }
    return encode_cursor(payload)


def build_pagination_envelope(
    *,
    request_url: URL,
    offset: int,
    limit: int,
    total: Optional[int] = None,
    next_cursor: Optional[str] = None,
) -> Tuple[Dict[str, object], Dict[str, Optional[str]]]:
    """Return ``meta`` and ``links`` dictionaries for responses."""

    meta = create_pagination_metadata(
        total=total,
        limit=limit,
        offset=offset,
        next_cursor=next_cursor,
    )

    base_url = request_url.remove_query_params("cursor")
    links = {
        "self": str(request_url),
        "next": str(base_url.include_query_params(cursor=next_cursor)) if next_cursor else None,
    }

    return meta, links


__all__ = [
    "resolve_pagination",
    "build_next_cursor",
    "build_pagination_envelope",
]

