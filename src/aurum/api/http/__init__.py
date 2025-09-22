"""HTTP utilities package for the Aurum API.

This package provides modular HTTP utilities for:
- Response handling (ETags, CSV, errors)
- Pagination (cursor-based, offset-based)
- Request validation and processing
"""

from .pagination import (
    create_pagination_metadata,
    decode_cursor,
    deprecation_warning_headers,
    encode_cursor,
    normalize_cursor_input,
)
from .responses import (
    compute_etag,
    create_error_response,
    csv_response,
    csv_stream,
    model_dump,
    prepare_csv_value,
    respond_with_etag,
)

__all__ = [
    # Pagination utilities
    "create_pagination_metadata",
    "decode_cursor",
    "deprecation_warning_headers",
    "encode_cursor",
    "normalize_cursor_input",
    # Response utilities
    "compute_etag",
    "create_error_response",
    "csv_response",
    "csv_stream",
    "model_dump",
    "prepare_csv_value",
    "respond_with_etag",
]
