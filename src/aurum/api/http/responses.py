"""HTTP response utilities for the Aurum API.

Standardized helpers for:
- ETag generation + conditional responses (304)
- CSV streaming responses with pagination headers
- Cache-Control headers (`stale-while-revalidate`, `stale-if-error`)
- Error envelope construction

See also: docs/api_usage_guide.md for endpoint examples.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request, Response
from fastapi.responses import StreamingResponse

DEFAULT_CACHE_SECONDS = 60
_MAX_STALE_IF_ERROR_SECONDS = 86_400


def model_dump(value: Any) -> Dict[str, Any]:
    """Extract data from Pydantic models or dict-like objects.

    Args:
        value: Object to extract data from (model, dict, etc.)

    Returns:
        Dictionary representation of the value
    """
    dump_method = getattr(value, "model_dump", None)
    if callable(dump_method):
        return dump_method()
    if isinstance(value, Mapping):
        return value
    try:
        return dict(value)
    except TypeError as exc:  # pragma: no cover - defensive
        raise TypeError(f"Unsupported payload type for serialization: {type(value)!r}") from exc


def build_cache_control_header(cache_seconds: int) -> str:
    """Return a standards-compliant Cache-Control header string.

    Args:
        cache_seconds: Desired max-age in seconds. Zero disables caching.

    Returns:
        Cache-Control header value.
    """
    if cache_seconds <= 0:
        return "no-store"
    stale_while_revalidate = min(max(cache_seconds // 2, 1), 600)
    stale_if_error = min(cache_seconds * 3, _MAX_STALE_IF_ERROR_SECONDS)
    return (
        f"public, max-age={cache_seconds}, "
        f"stale-while-revalidate={stale_while_revalidate}, "
        f"stale-if-error={stale_if_error}"
    )


def compute_etag(payload: Dict[str, Any]) -> str:
    """Compute an ETag hash for the given payload.

    Args:
        payload: Data to hash for ETag generation

    Returns:
        Hex digest string to use as ETag
    """
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def respond_with_etag(
    model,
    request: Request,
    response: Response,
    *,
    extra_headers: Optional[Dict[str, str]] = None,
    cache_seconds: Optional[int] = DEFAULT_CACHE_SECONDS,
    cache_control: Optional[str] = None,
) -> Any:
    """Add ETag header and handle conditional requests.

    Args:
        model: Response model to generate ETag from
        request: FastAPI request object
        response: FastAPI response object
        extra_headers: Additional headers to set
        cache_seconds: TTL used to build Cache-Control header; ``None`` disables automatic header
        cache_control: Explicit Cache-Control header value (takes precedence over ``cache_seconds``)

    Returns:
        304 Response if ETag matches, otherwise original model with ETag header
    """
    payload = model_dump(model)
    etag_payload = {k: v for k, v in payload.items() if k != "meta"}
    etag = compute_etag(etag_payload)
    incoming = request.headers.get("if-none-match")

    headers: Dict[str, str] = {}
    if extra_headers:
        headers.update(extra_headers)

    if cache_control:
        headers.setdefault("Cache-Control", cache_control)
    elif cache_seconds is not None:
        headers.setdefault("Cache-Control", build_cache_control_header(cache_seconds))

    if incoming and incoming == etag:
        headers["ETag"] = etag
        return Response(status_code=304, headers=headers)

    response.headers["ETag"] = etag
    for key, value in headers.items():
        response.headers.setdefault(key, value)
    return model


def prepare_csv_value(value: Any) -> str:
    """Convert a value to a CSV-compatible string.

    Args:
        value: Value to convert

    Returns:
        String representation safe for CSV output
    """
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, default=str)
        except TypeError:
            return str(value)
    return str(value)


async def csv_stream(
    rows: Iterable[Mapping[str, Any]] | AsyncIterable[Mapping[str, Any]],
    fieldnames: Sequence[str],
) -> AsyncIterator[str]:
    """Generate CSV content as an async streaming response.

    Args:
        rows: Data rows to convert to CSV (sync or async iterable)
        fieldnames: Column names in order

    Yields:
        CSV content chunks
    """
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    yield buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)

    async def _write_row(row: Mapping[str, Any]) -> AsyncIterator[str]:
        writer.writerow({field: prepare_csv_value(row.get(field)) for field in fieldnames})
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

    if isinstance(rows, AsyncIterable):
        async for row in rows:
            async for chunk in _write_row(row):
                yield chunk
    else:
        for row in rows:
            async for chunk in _write_row(row):
                yield chunk
            await asyncio.sleep(0)


def csv_response(
    request_id: str,
    rows: Iterable[Mapping[str, Any]] | AsyncIterable[Mapping[str, Any]],
    fieldnames: Sequence[str],
    filename: str,
    *,
    cache_control: Optional[str] = None,
    cache_seconds: Optional[int] = DEFAULT_CACHE_SECONDS,
    next_cursor: Optional[str] = None,
    prev_cursor: Optional[str] = None,
) -> StreamingResponse:
    """Create a streaming CSV response.

    Args:
        request_id: Request identifier for headers
        rows: Data rows to stream as CSV
        fieldnames: Column names in order
        filename: Suggested filename for download
        cache_control: Cache-Control header value
        cache_seconds: TTL used to build Cache-Control header when ``cache_control`` is not supplied
        next_cursor: Pagination cursor for next page
        prev_cursor: Pagination cursor for previous page

    Returns:
        StreamingResponse with CSV content
    """
    stream = csv_stream(rows, fieldnames)
    response = StreamingResponse(stream, media_type="text/csv; charset=utf-8")
    response.headers["X-Request-Id"] = request_id
    if cache_control:
        response.headers.setdefault("Cache-Control", cache_control)
    elif cache_seconds is not None:
        response.headers.setdefault("Cache-Control", build_cache_control_header(cache_seconds))
    if next_cursor:
        response.headers["X-Next-Cursor"] = next_cursor
    if prev_cursor:
        response.headers["X-Prev-Cursor"] = prev_cursor
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def create_error_response(
    status_code: int,
    detail: str,
    *,
    request_id: Optional[str] = None,
    field_errors: Optional[Dict[str, str]] = None,
    retry_after: Optional[int] = None,
) -> Dict[str, Any]:
    """Create a standardized error response.

    Args:
        status_code: HTTP status code
        detail: Error description
        request_id: Request identifier
        field_errors: Field-specific validation errors
        retry_after: Seconds to wait before retrying

    Returns:
        Error response dictionary
    """
    error_response = {
        "error": {
            "code": status_code,
            "message": detail,
        }
    }

    if request_id:
        error_response["request_id"] = request_id

    if field_errors:
        error_response["error"]["field_errors"] = field_errors

    if retry_after is not None:
        error_response["error"]["retry_after"] = retry_after

    return error_response
