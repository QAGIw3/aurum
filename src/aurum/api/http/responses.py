"""HTTP response utilities for the Aurum API.

This module provides standardized response handling including:
- ETag generation and conditional responses
- CSV streaming responses
- Error response formatting
- Response metadata handling
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from fastapi import HTTPException, Request, Response
from fastapi.responses import StreamingResponse


def model_dump(value: Any) -> Dict[str, Any]:
    """Extract data from Pydantic models or dict-like objects.

    Args:
        value: Object to extract data from (model, dict, etc.)

    Returns:
        Dictionary representation of the value
    """
    if hasattr(value, "model_dump"):
        return value.model_dump()  # type: ignore[attr-defined]
    if hasattr(value, "dict"):
        return value.dict()  # type: ignore[attr-defined]
    if isinstance(value, dict):
        return value
    return dict(value)  # type: ignore[arg-type]


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
) -> Any:
    """Add ETag header and handle conditional requests.

    Args:
        model: Response model to generate ETag from
        request: FastAPI request object
        response: FastAPI response object
        extra_headers: Additional headers to set

    Returns:
        304 Response if ETag matches, otherwise original model with ETag header
    """
    payload = model_dump(model)
    etag_payload = {k: v for k, v in payload.items() if k != "meta"}
    etag = compute_etag(etag_payload)
    incoming = request.headers.get("if-none-match")

    if incoming and incoming == etag:
        headers = {"ETag": etag}
        if extra_headers:
            headers.update(extra_headers)
        return Response(status_code=304, headers=headers)

    response.headers["ETag"] = etag
    if extra_headers:
        for key, value in extra_headers.items():
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


def csv_stream(rows: Iterable[Mapping[str, Any]], fieldnames: Sequence[str]) -> Iterable[str]:
    """Generate CSV content as a streaming response.

    Args:
        rows: Data rows to convert to CSV
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

    for row in rows:
        writer.writerow({field: prepare_csv_value(row.get(field)) for field in fieldnames})
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)


def csv_response(
    request_id: str,
    rows: Iterable[Mapping[str, Any]],
    fieldnames: Sequence[str],
    filename: str,
    *,
    cache_control: Optional[str] = None,
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
        next_cursor: Pagination cursor for next page
        prev_cursor: Pagination cursor for previous page

    Returns:
        StreamingResponse with CSV content
    """
    stream = csv_stream(rows, fieldnames)
    response = StreamingResponse(stream, media_type="text/csv; charset=utf-8")
    response.headers["X-Request-Id"] = request_id
    if cache_control:
        response.headers["Cache-Control"] = cache_control
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
