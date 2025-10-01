ETag and Cache-Control helpers
================================

This package provides canonical HTTP response utilities for ETag and caching.

Use `respond_with_etag` for all endpoints that need conditional responses or consistent cache headers. It:
- Computes a stable ETag from the response model (excluding `meta`)
- Handles `If-None-Match` (304) and `If-Match` (412) per RFC 7232
- Adds `Cache-Control` headers (`stale-while-revalidate`, `stale-if-error`)
- Supports pagination Link headers via optional arguments

Quick example

```python
from aurum.api.http import respond_with_etag

@router.get("/items")
async def list_items(request: Request, response: Response):
    data = {"data": items, "meta": {"request_id": get_request_id()}}
    return respond_with_etag(
        data,
        request,
        response,
        cache_seconds=300,
        canonical_url=str(request.url.remove_query_params("cursor")),
    )
```

For cursor-based pagination, prefer the small builders in `response_builders.py` to assemble Link headers correctly:

```python
from aurum.api.http.response_builders import etag_cursor_response_builder

build = etag_cursor_response_builder(
    request,
    response,
    next_cursor=next_cursor,
    canonical_url=str(request.url.remove_query_params("cursor")),
)
return build(model)
```

Guidance
- Do not manually set ETag or Cache-Control headers in routers
- Use these helpers to ensure consistent behavior across v2 endpoints

