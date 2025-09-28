"""GraphQL response caching helpers.

Provides a lightweight read-through cache tailored for GraphQL requests that
fan out to Trino. The implementation is intentionally self-contained so the
router can remain framework-agnostic while still offering per-tenant cache
keys, TTL based eviction, and HTTP conditional response support (ETag/304).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import Request, Response

from ..telemetry.context import normalize_tenant_id, TenantIdValidationError

from ..http import build_cache_control_header
from ..http.responses import _matches_any_etag

_DEFAULT_TTL_SECONDS = 180
_MAX_CACHE_ENTRIES = 256
_CACHE_BYPASS_HEADERS = {"cache-control", "pragma"}

# Operations that typically execute heavier Trino reads. Keys are matched using
# lowercase values of either the provided ``operationName`` or the first field
# in the GraphQL selection set.
_HEAVY_OPERATION_HINTS: Dict[str, int] = {
    "probabilistic_forecast": 300,
    "forecast_history": 600,
    "scenarios": 120,
    "scenario_runs": 120,
}


@dataclass
class GraphQLCacheParams:
    """Computed parameters describing a cacheable request."""

    cache_key: str
    ttl_seconds: int
    tenant_id: str
    operation_hint: Optional[str]


@dataclass
class GraphQLCacheEntry:
    """Stored response payload for a cacheable GraphQL query."""

    content: bytes
    status_code: int
    content_type: str
    etag: str
    cache_control: str
    headers: Dict[str, str]
    expires_at: float


class GraphQLResponseCache:
    """In-memory async-safe cache for GraphQL responses."""

    def __init__(
        self,
        *,
        default_ttl: int = _DEFAULT_TTL_SECONDS,
        max_entries: int = _MAX_CACHE_ENTRIES,
        operation_overrides: Optional[Dict[str, int]] = None,
    ) -> None:
        self._default_ttl = max(1, int(default_ttl))
        self._max_entries = max(1, int(max_entries))
        self._entries: "OrderedDict[str, GraphQLCacheEntry]" = OrderedDict()
        self._entry_lock = asyncio.Lock()
        self._locks: Dict[str, asyncio.Lock] = {}
        self._locks_lock = asyncio.Lock()

        overrides = operation_overrides or {}
        self._operation_overrides = {key.lower(): int(value) for key, value in overrides.items() if value}

    async def resolve_params(self, request: Request) -> Optional[GraphQLCacheParams]:
        """Return caching parameters for the incoming request if eligible."""

        if request.method not in {"GET", "POST"}:
            return None

        # Honour explicit cache bypass headers.
        for header in _CACHE_BYPASS_HEADERS:
            value = request.headers.get(header)
            if not value:
                continue
            lowered = value.lower()
            if "no-cache" in lowered or "no-store" in lowered:
                return None

        if request.headers.get("x-aurum-bypass-cache", "").lower() in {"1", "true", "yes"}:
            return None

        tenant_id = getattr(request.state, "tenant_id", None) or request.headers.get("x-aurum-tenant")
        try:
            tenant_id = normalize_tenant_id(tenant_id)
        except TenantIdValidationError:
            return None
        if not tenant_id:
            # Per-tenant isolation is required for correctness.
            return None

        query: Optional[str] = None
        variables: Dict[str, Any] = {}
        operation_name: Optional[str] = None

        if request.method == "GET":
            query = request.query_params.get("query")
            operation_name = request.query_params.get("operationName") or request.query_params.get("operation")
            variables_param = request.query_params.get("variables")
            if variables_param:
                try:
                    variables = json.loads(variables_param)
                except (TypeError, ValueError):
                    return None
        else:
            try:
                body = await request.body()
            except Exception:
                return None
            if not body:
                return None
            try:
                payload = json.loads(body)
            except ValueError:
                return None
            query = payload.get("query")
            variables_raw = payload.get("variables")
            if isinstance(variables_raw, dict):
                variables = variables_raw
            elif isinstance(variables_raw, str):
                try:
                    variables = json.loads(variables_raw)
                except ValueError:
                    variables = {}
            operation_name = payload.get("operationName")

        if not query:
            return None

        stripped = query.lstrip()
        lowered_head = stripped[:16].lower()
        if lowered_head.startswith("mutation") or lowered_head.startswith("subscription"):
            return None
        if "__schema" in stripped or "__type" in stripped:
            # Skip GraphQL introspection and IDE helper queries.
            return None

        operation_hint = self._determine_operation_hint(operation_name, query)
        ttl_seconds = self._resolve_ttl(operation_hint)
        if ttl_seconds <= 0:
            return None

        variables_key = json.dumps(variables, sort_keys=True, separators=(",", ":"), default=str)
        key_material = "|".join([tenant_id, operation_hint or "unknown", query.strip(), variables_key])
        cache_key = hashlib.sha256(key_material.encode("utf-8")).hexdigest()
        namespaced_key = f"tenant:{tenant_id}:{cache_key}"

        return GraphQLCacheParams(
            cache_key=namespaced_key,
            ttl_seconds=ttl_seconds,
            tenant_id=tenant_id,
            operation_hint=operation_hint,
        )

    async def key_lock(self, cache_key: str) -> asyncio.Lock:
        """Return an asyncio lock for the supplied cache key."""

        async with self._locks_lock:
            lock = self._locks.get(cache_key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[cache_key] = lock
            return lock

    async def get_cached_response(self, params: GraphQLCacheParams, request: Request) -> Optional[Response]:
        """Return a cached response if available and valid."""

        async with self._entry_lock:
            entry = self._entries.get(params.cache_key)
            if entry is None:
                return None
            if entry.expires_at <= time.time():
                self._entries.pop(params.cache_key, None)
                return None
            # Promote to maintain LRU ordering
            self._entries.move_to_end(params.cache_key)

        if_match = request.headers.get("if-match")
        if if_match and not _matches_any_etag(if_match, entry.etag):
            headers = {
                "ETag": entry.etag,
                "Cache-Control": entry.cache_control,
            }
            return Response(status_code=412, headers=headers)

        if_none_match = request.headers.get("if-none-match")
        if if_none_match and _matches_any_etag(if_none_match, entry.etag):
            headers = self._render_headers(entry, cache_status="HIT")
            return Response(status_code=304, headers=headers)

        headers = self._render_headers(entry, cache_status="HIT")
        return Response(
            content=entry.content,
            status_code=entry.status_code,
            media_type=entry.content_type,
            headers=headers,
        )

    async def store_response(
        self,
        params: GraphQLCacheParams,
        request: Request,
        response: Response,
    ) -> Response:
        """Persist the response in cache if eligible and return a fresh copy."""

        status_code = getattr(response, "status_code", 200)
        if status_code != 200:
            response.headers.setdefault("X-Cache", "BYPASS")
            return response

        # Respect explicit caching directives from the response itself.
        existing_cache_control = response.headers.get("cache-control")
        if existing_cache_control and "no-store" in existing_cache_control.lower():
            response.headers.setdefault("X-Cache", "BYPASS")
            return response

        content = await self._read_response_body(response)
        etag = f'"{hashlib.sha256(content).hexdigest()}"'
        cache_control_header = build_cache_control_header(params.ttl_seconds)

        headers = {
            key: value
            for key, value in response.headers.items()
            if key.lower() not in {"content-length", "etag", "cache-control", "x-cache"}
        }
        headers.update({
            "ETag": etag,
            "Cache-Control": cache_control_header,
            "X-Cache": "MISS",
        })

        media_type = response.headers.get("content-type") or response.media_type or "application/json"
        cloned = Response(
            content=content,
            status_code=status_code,
            media_type=media_type,
            headers=headers,
        )

        entry = GraphQLCacheEntry(
            content=content,
            status_code=status_code,
            content_type=media_type,
            etag=etag,
            cache_control=cache_control_header,
            headers={k: v for k, v in headers.items() if k.lower() != "x-cache"},
            expires_at=time.time() + params.ttl_seconds,
        )

        async with self._entry_lock:
            self._entries[params.cache_key] = entry
            self._entries.move_to_end(params.cache_key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

        return cloned

    async def _read_response_body(self, response: Response) -> bytes:
        """Return the response body as bytes, consuming iterators when needed."""

        if hasattr(response, "body"):
            body = response.body
            if isinstance(body, (bytes, bytearray)):
                return bytes(body)

        body_bytes = b""
        body_iterator = getattr(response, "body_iterator", None)
        if body_iterator is not None:
            chunks = []
            async for chunk in body_iterator:
                chunks.append(chunk)
            body_bytes = b"".join(chunks)
        return body_bytes

    def _render_headers(self, entry: GraphQLCacheEntry, *, cache_status: str) -> Dict[str, str]:
        headers = dict(entry.headers)
        headers["ETag"] = entry.etag
        headers["Cache-Control"] = entry.cache_control
        headers["X-Cache"] = cache_status
        return headers

    def _determine_operation_hint(self, operation_name: Optional[str], query: str) -> Optional[str]:
        if operation_name:
            return operation_name.strip().lower()

        cleaned = re.sub(r"#.*", "", query)
        match = re.search(r"\{\s*(\w+)", cleaned)
        if match:
            return match.group(1).lower()
        return None

    def _resolve_ttl(self, operation_hint: Optional[str]) -> int:
        if operation_hint:
            lowered = operation_hint.lower()
            if lowered in self._operation_overrides:
                return max(0, self._operation_overrides[lowered])
            if lowered in _HEAVY_OPERATION_HINTS:
                return max(0, _HEAVY_OPERATION_HINTS[lowered])
        return self._default_ttl

__all__ = [
    "GraphQLResponseCache",
    "GraphQLCacheEntry",
    "GraphQLCacheParams",
]
