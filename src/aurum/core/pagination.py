"""Unified pagination utilities for API responses."""
from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple

from .models import AurumBaseModel, PaginationMeta


@dataclass
class OffsetLimit:
    """Offset and limit for pagination."""
    limit: int
    offset: int = 0


@dataclass
class Cursor:
    """Cursor for stable pagination."""
    offset: int
    limit: int
    timestamp: float
    filters: Dict[str, Any]

    def to_string(self) -> str:
        """Convert cursor to base64-encoded string."""
        data = {
            "offset": self.offset,
            "limit": self.limit,
            "timestamp": self.timestamp,
            "filters": self.filters,
        }
        json_str = json.dumps(data, default=str)
        return base64.b64encode(json_str.encode()).decode()

    @classmethod
    def from_string(cls, cursor_str: str) -> "Cursor":
        """Create cursor from base64-encoded string."""
        try:
            json_str = base64.b64decode(cursor_str.encode()).decode()
            data = json.loads(json_str)
            return cls(
                offset=data["offset"],
                limit=data["limit"],
                timestamp=data["timestamp"],
                filters=data["filters"],
            )
        except Exception as exc:
            raise ValueError(f"Invalid cursor format: {exc}") from exc


class OffsetPage(AurumBaseModel):
    """Offset-based pagination bundle."""

    items: Tuple[object, ...]
    meta: PaginationMeta


class CursorPage(AurumBaseModel):
    """Cursor-based pagination bundle."""

    items: Tuple[object, ...]
    meta: PaginationMeta


class Paginator:
    """Helper for producing ``PaginationMeta`` objects for responses."""

    def __init__(self, request_id: str, *, query_time_ms: int = 0) -> None:
        self._request_id = request_id
        self._query_time_ms = query_time_ms

    def build_meta(
        self,
        *,
        count: int | None = None,
        total: int | None = None,
        offset: int | None = None,
        limit: int | None = None,
        next_cursor: str | None = None,
        prev_cursor: str | None = None,
    ) -> PaginationMeta:
        return PaginationMeta(
            request_id=self._request_id,
            query_time_ms=self._query_time_ms,
            count=count,
            total=total,
            offset=offset,
            limit=limit,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

    def offset_page(
        self,
        items: Iterable[object],
        *,
        total: int | None = None,
        offset: int = 0,
        limit: int | None = None,
        next_cursor: str | None = None,
        prev_cursor: str | None = None,
    ) -> OffsetPage:
        sequence = tuple(items)
        meta = self.build_meta(
            count=len(sequence),
            total=total,
            offset=offset,
            limit=limit,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )
        return OffsetPage(items=sequence, meta=meta)

    def cursor_page(
        self,
        items: Iterable[object],
        *,
        next_cursor: str | None = None,
        prev_cursor: str | None = None,
        limit: int | None = None,
        total: int | None = None,
    ) -> CursorPage:
        sequence = tuple(items)
        meta = self.build_meta(
            count=len(sequence),
            total=total,
            limit=limit,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )
        return CursorPage(items=sequence, meta=meta)


__all__ = ["Paginator", "OffsetPage", "CursorPage"]
