"""Enhanced response utilities with caching, compression, and streaming support."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import time
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

try:
    from fastapi import Response
    from fastapi.responses import StreamingResponse
except ImportError:
    Response = None
    StreamingResponse = None

from ...core import get_current_context


@dataclass
class ResponseMetrics:
    """Response performance metrics."""
    generation_time_ms: float
    compression_ratio: Optional[float] = None
    cache_hit: bool = False
    size_bytes: int = 0


def create_etag_response(
    data: Any,
    response: Optional[Response] = None,
    enable_compression: bool = True,
    cache_max_age: int = 3600
) -> Any:
    """Create response with ETag caching support."""
    if Response is None:
        return data
    
    start_time = time.time()
    
    # Serialize data to generate ETag
    if hasattr(data, 'model_dump_json'):
        content = data.model_dump_json()
    else:
        content = json.dumps(data, default=str)
    
    # Generate ETag from content hash
    etag = hashlib.md5(content.encode()).hexdigest()
    
    if response is not None:
        # Set caching headers
        response.headers["ETag"] = f'"{etag}"'
        response.headers["Cache-Control"] = f"public, max-age={cache_max_age}"
        
        # Enable compression if requested and beneficial
        if enable_compression and len(content) > 1024:  # Only compress if > 1KB
            response.headers["Content-Encoding"] = "gzip"
            response.headers["Vary"] = "Accept-Encoding"
    
    # Calculate metrics
    generation_time = (time.time() - start_time) * 1000
    
    return data


def create_csv_response(
    data: List[Dict[str, Any]],
    filename: str,
    delimiter: str = ",",
    enable_compression: bool = True
) -> Union[Response, StreamingResponse]:
    """Create CSV response with optional compression."""
    if Response is None or StreamingResponse is None:
        return data
    
    def generate_csv():
        """Generate CSV content."""
        output = io.StringIO()
        
        if data:
            # Get field names from first row
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=delimiter)
            
            # Write header
            writer.writeheader()
            
            # Write data rows
            for row in data:
                writer.writerow(row)
        
        return output.getvalue()
    
    csv_content = generate_csv()
    
    # Determine if compression is beneficial
    should_compress = enable_compression and len(csv_content) > 1024
    
    if should_compress:
        # Compress content
        compressed_content = gzip.compress(csv_content.encode())
        
        return Response(
            content=compressed_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Encoding": "gzip",
                "Content-Length": str(len(compressed_content))
            }
        )
    else:
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(csv_content))
            }
        )


async def create_streaming_response(
    data_generator: AsyncGenerator[Any, None],
    media_type: str = "application/json",
    filename: Optional[str] = None,
    enable_compression: bool = False
) -> StreamingResponse:
    """Create streaming response for large datasets."""
    if StreamingResponse is None:
        return None
    
    headers = {}
    
    if filename:
        headers["Content-Disposition"] = f"attachment; filename={filename}"
    
    if enable_compression:
        headers["Content-Encoding"] = "gzip"
        headers["Vary"] = "Accept-Encoding"
    
    async def compress_stream():
        """Compress streaming data on the fly."""
        if enable_compression:
            # For streaming compression, we'd need a more sophisticated approach
            # This is a simplified version
            async for chunk in data_generator:
                if isinstance(chunk, str):
                    yield gzip.compress(chunk.encode())
                else:
                    yield gzip.compress(str(chunk).encode())
        else:
            async for chunk in data_generator:
                if isinstance(chunk, str):
                    yield chunk.encode()
                else:
                    yield str(chunk).encode()
    
    return StreamingResponse(
        compress_stream(),
        media_type=media_type,
        headers=headers
    )


def create_error_response(
    error: Exception,
    status_code: int = 500,
    include_detail: bool = True,
    include_trace: bool = False
) -> Dict[str, Any]:
    """Create standardized error response."""
    context = get_current_context()
    
    error_response = {
        "error": {
            "type": type(error).__name__,
            "message": str(error) if include_detail else "An error occurred",
            "request_id": context.request_id,
            "timestamp": time.time()
        }
    }
    
    if include_trace and hasattr(error, '__traceback__'):
        import traceback
        error_response["error"]["trace"] = traceback.format_exception(
            type(error), error, error.__traceback__
        )
    
    # Add context information
    if context.tenant_id:
        error_response["error"]["tenant_id"] = context.tenant_id
    
    if context.operation:
        error_response["error"]["operation"] = context.operation
    
    return error_response


class ResponseCache:
    """Response-level caching utility."""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        self._cache: Dict[str, tuple] = {}
        self.max_size = max_size
        self.default_ttl = default_ttl
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached response."""
        if key in self._cache:
            data, expiry_time = self._cache[key]
            if time.time() < expiry_time:
                return data
            else:
                # Remove expired entry
                del self._cache[key]
        return None
    
    def set(self, key: str, data: Any, ttl: Optional[int] = None) -> None:
        """Cache response data."""
        if len(self._cache) >= self.max_size:
            # Simple eviction - remove oldest entry
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
        
        expiry_time = time.time() + (ttl or self.default_ttl)
        self._cache[key] = (data, expiry_time)
    
    def invalidate(self, pattern: Optional[str] = None) -> None:
        """Invalidate cached responses."""
        if pattern is None:
            self._cache.clear()
        else:
            # Remove entries matching pattern
            keys_to_remove = [key for key in self._cache.keys() if pattern in key]
            for key in keys_to_remove:
                del self._cache[key]


# Global response cache instance
_response_cache = ResponseCache()


def get_response_cache() -> ResponseCache:
    """Get global response cache."""
    return _response_cache