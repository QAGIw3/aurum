"""Shared service contract dataclasses and enums."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, Optional, Sequence, TypeVar


class CacheStatus(str, Enum):
    """Represents the cache outcome for a service call."""

    HIT = "hit"
    MISS = "miss"
    BYPASS = "bypass"
    STALE = "stale"


@dataclass(frozen=True)
class CacheDirective:
    """Hints describing how a service should interact with its cache."""

    namespace: str
    ttl_seconds: int
    version: Optional[str] = None
    tags: Sequence[str] = field(default_factory=tuple)
    allow_bypass: bool = False


@dataclass(frozen=True)
class ServiceExecutionMetadata:
    """Execution metadata returned alongside service data."""

    elapsed_ms: float
    cache_status: CacheStatus = CacheStatus.BYPASS
    cache_key: Optional[str] = None
    cache_version: Optional[str] = None
    backend: Optional[str] = None
    row_count: Optional[int] = None


T = TypeVar("T")


@dataclass(frozen=True)
class ServiceExecutionResult(Generic[T]):
    """Generic wrapper for returning data plus metadata."""

    data: T
    metadata: ServiceExecutionMetadata


__all__ = [
    "CacheDirective",
    "CacheStatus",
    "ServiceExecutionMetadata",
    "ServiceExecutionResult",
]
