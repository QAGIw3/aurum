"""Unified pagination utilities for API responses."""
from __future__ import annotations

import base64
import hashlib
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


@dataclass(frozen=True)
class Cursor:
    """Cursor for stable pagination with validation and integrity checks."""
    offset: int
    limit: int
    timestamp: float
    filters: Dict[str, Any]
    _signature: str = ""

    def __post_init__(self) -> None:
        """Validate cursor parameters after initialization."""
        if self.offset < 0:
            raise ValueError(f"Cursor offset must be non-negative: {self.offset}")
        if self.limit <= 0:
            raise ValueError(f"Cursor limit must be positive: {self.limit}")
        if self.timestamp <= 0:
            raise ValueError(f"Cursor timestamp must be positive: {self.timestamp}")

    def to_string(self) -> str:
        """Convert cursor to base64-encoded string with integrity signature."""
        data = {
            "offset": self.offset,
            "limit": self.limit,
            "timestamp": self.timestamp,
            "filters": self.filters,
        }
        json_str = json.dumps(data, default=str, separators=(",", ":"))
        payload = json_str.encode("utf-8")
        signature = hashlib.sha256(payload).hexdigest()[:8]  # 8-char integrity check
        signed_data = {"data": data, "sig": signature}
        signed_json = json.dumps(signed_data, default=str, separators=(",", ":"))
        return base64.urlsafe_b64encode(signed_json.encode()).decode()

    @classmethod
    def from_string(cls, cursor_str: str) -> "Cursor":
        """Create cursor from base64-encoded string with integrity verification."""
        try:
            signed_json = base64.urlsafe_b64decode(cursor_str.encode("ascii"))
            signed_data = json.loads(signed_json.decode("utf-8"))
            data = signed_data["data"]
            expected_sig = signed_data["sig"]

            # Verify integrity
            json_str = json.dumps(data, default=str, separators=(",", ":"))
            payload = json_str.encode("utf-8")
            actual_sig = hashlib.sha256(payload).hexdigest()[:8]
            if actual_sig != expected_sig:
                raise ValueError("Cursor signature mismatch - possible corruption or tampering")

            return cls(
                offset=data["offset"],
                limit=data["limit"],
                timestamp=data["timestamp"],
                filters=data["filters"],
            )
        except Exception as exc:
            raise ValueError(f"Invalid cursor format: {exc}") from exc

    def is_expired(self, max_age_seconds: int = 3600) -> bool:
        """Check if cursor has expired."""
        return time.time() - self.timestamp > max_age_seconds

    def matches_filters(self, current_filters: Dict[str, Any]) -> bool:
        """Check if cursor filters match current query filters."""
        # Normalize None values for comparison
        def normalize_filters(filters: Dict[str, Any]) -> Dict[str, Any]:
            return {k: (v if v is not None else "") for k, v in filters.items()}

        return normalize_filters(self.filters) == normalize_filters(current_filters)


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
