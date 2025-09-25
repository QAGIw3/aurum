from __future__ import annotations

"""Cache management response models."""

from .base import AurumBaseModel
from .common import Meta


class CachePurgeDetail(AurumBaseModel):
    scope: str
    redis_keys_removed: int
    local_entries_removed: int


class CachePurgeResponse(AurumBaseModel):
    meta: Meta
    data: list[CachePurgeDetail]


__all__ = ["CachePurgeDetail", "CachePurgeResponse"]
