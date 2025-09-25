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
    extract_cursor_payload_from_row,
    normalize_cursor_input,
    DEFAULT_PAGE_SIZE,
    MAX_CURSOR_LENGTH,
    MAX_PAGE_SIZE,
)
from .responses import (
    DEFAULT_CACHE_SECONDS,
    build_cache_control_header,
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
    "extract_cursor_payload_from_row",
    "normalize_cursor_input",
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "MAX_CURSOR_LENGTH",
    # Response utilities
    "DEFAULT_CACHE_SECONDS",
    "build_cache_control_header",
    "compute_etag",
    "create_error_response",
    "csv_response",
    "csv_stream",
    "model_dump",
    "prepare_csv_value",
    "respond_with_etag",
]
