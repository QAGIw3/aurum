"""Small response-builder factories to standardize ETag responses.

These helpers return callables capturing `request`/`response` plus
optional pagination and canonical URL parameters. They defer importing
`respond_with_etag` until call time to keep import cost low in tools/tests.

Maintainers: See docs for examples and guidance
- docs/api_usage_guide.md#builder-usage-maintainers
- Prefer these builders over inline `respond_with_etag` in new routes
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from fastapi import Request, Response


def etag_response_builder(
    request: Request,
    response: Response,
    *,
    extra_headers: Optional[Dict[str, str]] = None,
    cache_seconds: Optional[int] = None,
    cache_control: Optional[str] = None,
    canonical_url: Optional[str] = None,
) -> Callable[[Any], Any]:
    """Return a builder that adds ETag to a response for any model-like payload."""

    def _builder(model: Any) -> Any:
        from .responses import respond_with_etag  # deferred import
        return respond_with_etag(
            model,
            request,
            response,
            extra_headers=extra_headers,
            cache_seconds=cache_seconds,
            cache_control=cache_control,
            canonical_url=canonical_url,
        )

    return _builder


def etag_cursor_response_builder(
    request: Request,
    response: Response,
    *,
    next_cursor: Optional[str] = None,
    prev_cursor: Optional[str] = None,
    canonical_url: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    cache_seconds: Optional[int] = None,
    cache_control: Optional[str] = None,
) -> Callable[[Any], Any]:
    """Return a builder adding ETag and cursor Link headers."""

    def _builder(model: Any) -> Any:
        from .responses import respond_with_etag  # deferred import
        return respond_with_etag(
            model,
            request,
            response,
            extra_headers=extra_headers,
            cache_seconds=cache_seconds,
            cache_control=cache_control,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
            canonical_url=canonical_url,
        )

    return _builder


__all__ = [
    "etag_response_builder",
    "etag_cursor_response_builder",
]
