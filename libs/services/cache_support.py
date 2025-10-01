"""Reusable cache helpers for service layers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import Any, Awaitable, Callable, Dict, Generic, Optional, Protocol, Tuple, TypeVar

from .contracts import (
    CacheDirective,
    CacheStatus,
    ServiceExecutionMetadata,
    ServiceExecutionResult,
)


class AsyncCacheProtocol(Protocol):
    """Minimal async cache protocol used by services."""

    async def get(self, key: str, *, namespace: Optional[str] = None) -> Optional[Any]:
        ...

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl_seconds: Optional[int] = None,
        namespace: Optional[str] = None,
    ) -> bool:
        ...

    async def invalidate(self, key: str, *, namespace: Optional[str] = None) -> int:
        ...


CachePayloadParser = Callable[[Dict[str, Any]], Tuple[Any, int, Dict[str, Any]]]
RepoFetchResult = Tuple[Any, float, Optional[str]]
TResult = TypeVar("TResult")


@dataclass(slots=True)
class CacheReadyResult(Generic[TResult]):
    """Container describing freshly fetched data ready for cache persistence."""

    data: TResult
    row_count: int
    payload_factory: Callable[[ServiceExecutionMetadata], Dict[str, Any]]
    debug_payload: Dict[str, Any] = field(default_factory=dict)


def _serialize_payload(payload: Any) -> Any:
    """Ensure payload is JSON serialisable where possible."""

    try:
        json.dumps(payload)
        return payload
    except (TypeError, ValueError):
        return json.loads(json.dumps(payload, default=str))


async def cache_lookup(
    cache: Optional[AsyncCacheProtocol],
    directive: Optional[CacheDirective],
    key: str,
) -> Tuple[CacheStatus, Optional[str], Optional[Any]]:
    """Attempt to resolve a value from cache."""

    if cache is None or directive is None or directive.allow_bypass:
        return CacheStatus.BYPASS, None, None

    cached_value = await cache.get(key, namespace=directive.namespace)
    if cached_value is None:
        return CacheStatus.MISS, key, None
    return CacheStatus.HIT, key, cached_value


async def cache_store(
    cache: Optional[AsyncCacheProtocol],
    directive: Optional[CacheDirective],
    namespaced_key: Optional[str],
    value: Any,
) -> CacheStatus:
    """Persist a value to cache, returning the resulting status."""

    if cache is None or directive is None or directive.ttl_seconds <= 0 or namespaced_key is None:
        return CacheStatus.BYPASS

    payload = _serialize_payload(value)
    await cache.set(
        namespaced_key,
        payload,
        ttl_seconds=directive.ttl_seconds,
        namespace=directive.namespace,
    )
    return CacheStatus.MISS


class CachedServiceMixin:
    """Mixin exposing shared cache orchestration helpers for services."""

    _cache: Optional[AsyncCacheProtocol]
    _cache_namespace: Optional[str]

    @property
    def cache_namespace(self) -> Optional[str]:
        return getattr(self, "_cache_namespace", None)

    def _cache_key(self, prefix: str, *parts: Any) -> str:
        rendered = ":".join("*" if part is None else str(part) for part in parts)
        suffix = f"{prefix}:{rendered}" if rendered else prefix
        namespace = self.cache_namespace
        return f"{namespace}:{suffix}" if namespace else suffix

    def _metadata_key(
        self,
        cache_directive: Optional[CacheDirective],
        namespaced_key: Optional[str],
    ) -> Optional[str]:
        if not namespaced_key:
            return None
        if cache_directive and cache_directive.namespace:
            return f"{cache_directive.namespace}:{namespaced_key}"
        return namespaced_key

    def _build_metadata(
        self,
        *,
        cache_status: CacheStatus,
        namespaced_key: Optional[str],
        cache_directive: Optional[CacheDirective],
        elapsed_ms: float,
        backend: Optional[str],
        row_count: int,
    ) -> ServiceExecutionMetadata:
        return ServiceExecutionMetadata(
            elapsed_ms=elapsed_ms,
            cache_status=cache_status,
            cache_key=self._metadata_key(cache_directive, namespaced_key),
            cache_version=cache_directive.version if cache_directive else None,
            backend=backend,
            row_count=row_count,
        )

    def _metadata_from_cache_payload(
        self,
        *,
        payload: Dict[str, Any],
        row_count: int,
        cache_directive: Optional[CacheDirective],
        namespaced_key: Optional[str],
    ) -> ServiceExecutionMetadata:
        meta_payload = payload.get("metadata", {})
        return self._build_metadata(
            cache_status=CacheStatus.HIT,
            namespaced_key=namespaced_key,
            cache_directive=cache_directive,
            elapsed_ms=float(meta_payload.get("elapsed_ms", 0.0)),
            backend=meta_payload.get("backend"),
            row_count=row_count,
        )

    async def _resolve_cache_hit(
        self,
        *,
        cache_directive: Optional[CacheDirective],
        cache_key: str,
        payload_parser: CachePayloadParser,
    ) -> Tuple[Optional[ServiceExecutionResult[Any]], CacheStatus, Optional[str], Dict[str, Any]]:
        cache_status, namespaced_key, cached_payload = await cache_lookup(
            getattr(self, "_cache", None), cache_directive, cache_key
        )

        if cache_status is CacheStatus.HIT and cached_payload:
            data, row_count, debug_payload = payload_parser(cached_payload)
            metadata = self._metadata_from_cache_payload(
                payload=cached_payload,
                row_count=row_count,
                cache_directive=cache_directive,
                namespaced_key=namespaced_key,
            )
            return ServiceExecutionResult(data, metadata), cache_status, namespaced_key, debug_payload

        return None, cache_status, namespaced_key, {}

    async def _execute_cached_operation(
        self,
        *,
        cache_directive: Optional[CacheDirective],
        cache_key: str,
        payload_parser: CachePayloadParser,
        fetcher: Callable[[], Awaitable[RepoFetchResult]],
        result_builder: Callable[[Any, Optional[str]], CacheReadyResult[TResult]],
        backend: Optional[str] = None,
    ) -> Tuple[ServiceExecutionResult[TResult], Dict[str, Any]]:
        cached_result, cache_status, namespaced_key, debug_payload = await self._resolve_cache_hit(
            cache_directive=cache_directive,
            cache_key=cache_key,
            payload_parser=payload_parser,
        )

        if cached_result:
            return cached_result, debug_payload

        payload, elapsed_ms, raw_query = await fetcher()
        shaped_result = result_builder(payload, raw_query)

        metadata = await self._build_metadata_and_cache(
            cache_status=cache_status,
            cache_directive=cache_directive,
            namespaced_key=namespaced_key,
            elapsed_ms=elapsed_ms,
            backend=backend,
            row_count=shaped_result.row_count,
            payload_factory=shaped_result.payload_factory,
        )

        return ServiceExecutionResult(shaped_result.data, metadata), shaped_result.debug_payload

    async def _build_metadata_and_cache(
        self,
        *,
        cache_status: CacheStatus,
        cache_directive: Optional[CacheDirective],
        namespaced_key: Optional[str],
        elapsed_ms: float,
        backend: Optional[str],
        row_count: int,
        payload_factory: Callable[[ServiceExecutionMetadata], Dict[str, Any]],
    ) -> ServiceExecutionMetadata:
        metadata = self._build_metadata(
            cache_status=cache_status,
            namespaced_key=namespaced_key,
            cache_directive=cache_directive,
            elapsed_ms=elapsed_ms,
            backend=backend,
            row_count=row_count,
        )
        payload = payload_factory(metadata)
        _, metadata = await self._finalize_cache(
            cache_status,
            cache_directive=cache_directive,
            namespaced_key=namespaced_key,
            payload=payload,
            metadata=metadata,
        )
        return metadata

    def _build_cache_payload(
        self,
        *,
        data_key: str,
        data: Any,
        metadata: ServiceExecutionMetadata,
        debug: Optional[Dict[str, Any]] = None,
        metadata_extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        meta_payload: Dict[str, Any] = {
            "elapsed_ms": metadata.elapsed_ms,
            "backend": metadata.backend,
        }
        if metadata_extra:
            meta_payload.update(metadata_extra)

        payload: Dict[str, Any] = {data_key: data, "metadata": meta_payload}
        if debug is not None:
            payload["debug"] = debug
        return payload

    def _payload_factory(
        self,
        *,
        data_key: str,
        data: Any,
        debug: Optional[Dict[str, Any]] = None,
        metadata_extra: Optional[Dict[str, Any]] = None,
    ) -> Callable[[ServiceExecutionMetadata], Dict[str, Any]]:
        return lambda metadata: self._build_cache_payload(
            data_key=data_key,
            data=data,
            metadata=metadata,
            debug=debug,
            metadata_extra=metadata_extra,
        )

    async def _finalize_cache(
        self,
        cache_status: CacheStatus,
        *,
        cache_directive: Optional[CacheDirective],
        namespaced_key: Optional[str],
        payload: Any,
        metadata: ServiceExecutionMetadata,
    ) -> Tuple[CacheStatus, ServiceExecutionMetadata]:
        if cache_status is not CacheStatus.MISS:
            return cache_status, metadata

        updated_status = await cache_store(
            getattr(self, "_cache", None),
            cache_directive,
            namespaced_key,
            payload,
        )

        return updated_status, replace(metadata, cache_status=updated_status)


__all__ = [
    "AsyncCacheProtocol",
    "CachePayloadParser",
    "RepoFetchResult",
    "CacheReadyResult",
    "CachedServiceMixin",
    "cache_lookup",
    "cache_store",
]
