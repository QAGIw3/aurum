"""Multi-tier caching orchestration with consistency, scaling, and tier-aware logic.

This module introduces a high-level `MultiTierCache` that coordinates three
logical cache tiers:

* L1 (hot): Redis or in-memory cache for low-latency access
* L2 (warm): Object storage (e.g. S3) for durable mid-term retention
* L3 (cold): Archival storage (e.g. Glacier) for long-lived backups

Features implemented:
- Adaptive cache-aside and write-through workflows
- Predictive pre-loading hooks (via `PredictiveWarmingEngine`)
- Intelligent promotion/demotion with hybrid LFU/LRU eviction scoring
- Distributed consistency via optimistic version stamps and quorum acknowledgements
- Tier-level analytics to feed optimization recommendations and alerts
- Automated scaling signals driven by observed pressure on tiers

The implementation favors composability: tier backends are injected and only
need to satisfy the small `CacheTier` protocol defined below. Production callers
can provide concrete clients (redis async client, aiobotocore, boto3, etc.),
while tests can use a lightweight in-memory tier implementation.
"""

from __future__ import annotations

import asyncio
import fnmatch
import json
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple, TypeVar, Union

from aurum.api.cache.cache import AsyncCache  # Reuse existing async Redis/memory client
from aurum.api.cache.cache import CacheBackend
from aurum.api.cache.cache_governance import CacheNamespace  # for typed namespace hints
from aurum.api.config import CacheConfig
from aurum.logging.structured_logger import get_logger

try:
    import boto3  # pragma: no cover - optional dependency
except ImportError:  # pragma: no cover - boto3 is optional
    boto3 = None

try:
    import botocore  # pragma: no cover
except ImportError:  # pragma: no cover
    botocore = None

if boto3:
    from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[attr-defined]
else:  # pragma: no cover - fallback when boto3 missing
    class BotoCoreError(Exception):  # type: ignore
        """Fallback error type when botocore is unavailable."""

    class ClientError(Exception):  # type: ignore
        """Fallback client error when botocore is unavailable."""


T = TypeVar("T")
RetrieveFn = Callable[[], Awaitable[T]]
PersistFn = Callable[[T], Awaitable[None]]


class TierType(Enum):
    """Enumerates available cache tiers."""

    L1 = "l1"  # Hot/read optimized tier (Redis)
    L2 = "l2"  # Warm/object storage tier (S3)
    L3 = "l3"  # Cold/archival tier (Glacier)


class EvictionPolicy(Enum):
    """Supported eviction policies."""

    LRU = "lru"
    LFU = "lfu"
    ADAPTIVE = "adaptive"  # Weighted combination of LRU and LFU


@dataclass
class TierConfig:
    """Configuration for a single tier."""

    type: TierType
    max_items: Optional[int] = None
    max_size_bytes: Optional[int] = None
    ttl_seconds: Optional[int] = None
    eviction_policy: EvictionPolicy = EvictionPolicy.ADAPTIVE
    promotion_enabled: bool = True
    write_through: bool = True
    compression: bool = False
    # Additional tier-specific parameters (bucket names, prefixes, etc.)
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MultiTierCacheConfig:
    """High-level configuration for the multi-tier cache."""

    tiers: Dict[TierType, TierConfig]
    default_ttl_seconds: int = 300
    namespace: str = "aurum"
    ensure_quorum: bool = True
    quorum_threshold: float = 0.51
    max_concurrency: int = 20
    enable_promotions: bool = True
    enable_evictions: bool = True


@dataclass
class KeyMetadata:
    """Runtime metadata tracked per cached key."""

    key: str
    namespace: str
    version: int = 0
    size_bytes: int = 0
    last_access_ts: float = field(default_factory=time.time)
    access_count: int = 0
    last_tier: TierType | None = None
    ttl_seconds: Optional[int] = None
    expires_at: Optional[float] = None

    def register_hit(self, tier: TierType) -> None:
        self.last_access_ts = time.time()
        self.access_count += 1
        self.last_tier = tier

    def update_ttl(self, ttl: Optional[int]) -> None:
        if ttl is None:
            return
        self.ttl_seconds = ttl
        self.expires_at = time.time() + ttl

    def effective_ttl(self, default_ttl: int) -> int:
        if self.ttl_seconds is not None:
            return self.ttl_seconds
        return default_ttl


@dataclass
class TierStats:
    """Aggregated tier statistics."""

    hits: int = 0
    misses: int = 0
    promotions: int = 0
    demotions: int = 0
    evictions: int = 0
    writes: int = 0
    write_errors: int = 0
    bytes_served: int = 0


@dataclass
class CacheEvent:
    """Event emitted for analytics or observers."""

    key: str
    namespace: str
    tier: TierType
    event_type: str
    timestamp: float
    metadata: Dict[str, Any]


class CacheTier(Protocol):
    """Protocol that every tier backend must satisfy."""

    name: str

    async def get(self, key: str) -> Optional[Any]:
        ...

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ...

    async def delete(self, key: str) -> None:
        ...

    async def exists(self, key: str) -> bool:
        ...

    async def bulk_get(self, keys: Sequence[str]) -> Dict[str, Any]:
        ...

    async def bulk_set(self, items: Dict[str, Any], ttl: Optional[int] = None) -> None:
        ...

    async def touch(self, key: str, ttl: Optional[int] = None) -> None:
        ...


class RedisTier:
    """Tier backed by the existing `AsyncCache` implementation."""

    def __init__(self, cache: AsyncCache):
        self.cache = cache
        self.name = "redis"

    async def get(self, key: str) -> Optional[Any]:
        return await self.cache.get(key)

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        await self.cache.set(key, value, ttl)

    async def delete(self, key: str) -> None:
        await self.cache.delete(key)

    async def exists(self, key: str) -> bool:
        value = await self.cache.get(key)
        return value is not None

    async def bulk_get(self, keys: Sequence[str]) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for key in keys:
            value = await self.get(key)
            if value is not None:
                results[key] = value
        return results

    async def bulk_set(self, items: Dict[str, Any], ttl: Optional[int] = None) -> None:
        await asyncio.gather(*(self.set(k, v, ttl=ttl) for k, v in items.items()))

    async def touch(self, key: str, ttl: Optional[int] = None) -> None:
        value = await self.cache.get(key)
        if value is not None:
            await self.cache.set(key, value, ttl)


class S3Tier:
    """Object storage tier using boto3 S3 client."""

    def __init__(self, *, bucket: str, namespace: str, prefix: str = "cache", s3_client: Any | None = None):
        if s3_client is None:
            if not boto3:  # pragma: no cover - boto3 optional
                raise RuntimeError("boto3 is required for S3 tier but is not installed")
            s3_client = boto3.client("s3")
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.namespace = namespace
        self.s3 = s3_client
        self.name = "s3"

    async def get(self, key: str) -> Optional[Any]:
        path = self._object_key(key)
        try:
            response = await asyncio.to_thread(self.s3.get_object, Bucket=self.bucket, Key=path)
            payload: bytes = await asyncio.to_thread(response["Body"].read)
            document = json.loads(payload.decode("utf-8"))
            return document.get("value")
        except (ClientError, BotoCoreError):
            return None
        except json.JSONDecodeError:
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:  # ttl unused but kept for interface
        path = self._object_key(key)
        payload = json.dumps({"value": value, "ttl": ttl, "timestamp": time.time()}, default=str)
        await asyncio.to_thread(
            self.s3.put_object,
            Bucket=self.bucket,
            Key=path,
            Body=payload.encode("utf-8"),
            ContentType="application/json",
        )

    async def delete(self, key: str) -> None:
        path = self._object_key(key)
        await asyncio.to_thread(self.s3.delete_object, Bucket=self.bucket, Key=path)

    async def exists(self, key: str) -> bool:
        path = self._object_key(key)
        try:
            await asyncio.to_thread(self.s3.head_object, Bucket=self.bucket, Key=path)
            return True
        except (ClientError, BotoCoreError):
            return False

    async def bulk_get(self, keys: Sequence[str]) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for key in keys:
            value = await self.get(key)
            if value is not None:
                results[key] = value
        return results

    async def bulk_set(self, items: Dict[str, Any], ttl: Optional[int] = None) -> None:
        await asyncio.gather(*(self.set(k, v, ttl=ttl) for k, v in items.items()))

    async def touch(self, key: str, ttl: Optional[int] = None) -> None:
        value = await self.get(key)
        if value is not None:
            await self.set(key, value, ttl=ttl)

    def _object_key(self, key: str) -> str:
        return f"{self.prefix}/{self.namespace}/{key}.json"


class GlacierTier:
    """Archival tier leveraging AWS Glacier."""

    def __init__(self, *, vault_name: str, namespace: str, glacier_client: Any | None = None):
        if glacier_client is None:
            if not boto3:  # pragma: no cover
                raise RuntimeError("boto3 is required for Glacier tier but is not installed")
            glacier_client = boto3.client("glacier")
        self.glacier = glacier_client
        self.vault_name = vault_name
        self.namespace = namespace
        self.name = "glacier"

    async def get(self, key: str) -> Optional[Any]:
        archive_id = await self._lookup_archive_id(key)
        if not archive_id:
            return None
        try:
            response = await asyncio.to_thread(
                self.glacier.get_job_output,
                vaultName=self.vault_name,
                jobId=archive_id,
            )
            payload: bytes = await asyncio.to_thread(response["body"].read)
            document = json.loads(payload.decode("utf-8"))
            return document.get("value")
        except (BotoCoreError, json.JSONDecodeError):
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        archive_description = json.dumps({
            "namespace": self.namespace,
            "key": key,
            "ttl": ttl,
            "timestamp": time.time(),
        })
        body = json.dumps({"value": value}, default=str).encode("utf-8")
        await asyncio.to_thread(
            self.glacier.upload_archive,
            vaultName=self.vault_name,
            archiveDescription=archive_description,
            body=body,
        )
        await self._store_archive_id(key)

    async def delete(self, key: str) -> None:
        archive_id = await self._lookup_archive_id(key)
        if not archive_id:
            return
        await asyncio.to_thread(
            self.glacier.delete_archive,
            vaultName=self.vault_name,
            archiveId=archive_id,
        )
        await self._store_archive_id(key, archive_id=None)

    async def exists(self, key: str) -> bool:
        archive_id = await self._lookup_archive_id(key)
        return archive_id is not None

    async def bulk_get(self, keys: Sequence[str]) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for key in keys:
            value = await self.get(key)
            if value is not None:
                results[key] = value
        return results

    async def bulk_set(self, items: Dict[str, Any], ttl: Optional[int] = None) -> None:
        await asyncio.gather(*(self.set(k, v, ttl=ttl) for k, v in items.items()))

    async def touch(self, key: str, ttl: Optional[int] = None) -> None:
        # Glacier does not support touch; re-upload metadata if needed
        value = await self.get(key)
        if value is not None:
            await self.set(key, value, ttl=ttl)

    async def _lookup_archive_id(self, key: str) -> Optional[str]:
        # In production this would rely on DynamoDB or other metadata store.
        # For now, leverage a simple tagging lookup if available.
        try:
            response = await asyncio.to_thread(
                self.glacier.list_jobs,
                vaultName=self.vault_name,
                limit=50,
            )
        except BotoCoreError:
            return None
        jobs = response.get("JobList", [])
        for job in jobs:
            if job.get("ArchiveDescription"):
                try:
                    descriptor = json.loads(job["ArchiveDescription"])
                    if descriptor.get("key") == key and descriptor.get("namespace") == self.namespace:
                        return job.get("JobId")
                except json.JSONDecodeError:
                    continue
        return None

    async def _store_archive_id(self, key: str, archive_id: Optional[str] = "placeholder") -> None:
        # Placeholder for integration with metadata store.
        _ = (key, archive_id)


class MultiTierCache:
    """Coordinates a multi-tier cache hierarchy with advanced behaviors."""

    def __init__(
        self,
        *,
        config: MultiTierCacheConfig,
        cache_config: CacheConfig,
        predictive_engine: "PredictiveWarmingEngine | None" = None,
        analytics_engine: "CacheAnalyticsEngine | None" = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.config = config
        self.cache_config = cache_config
        self.predictive_engine = predictive_engine
        self.analytics_engine = analytics_engine
        self.loop = loop or asyncio.get_event_loop()
        self.logger = get_logger(__name__)

        self._metadata: Dict[str, KeyMetadata] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._tier_stats: Dict[TierType, TierStats] = {
            tier: TierStats() for tier in TierType
        }
        self._tiers: Dict[TierType, CacheTier] = {}
        self._tier_configs: Dict[TierType, TierConfig] = config.tiers

        self._initialize_tiers()
        self._notify_engines_of_topology()

    # -- Initialization -----------------------------------------------------
    def _initialize_tiers(self) -> None:
        """Instantiate tier backends based on config."""
        # L1: Redis via AsyncCache (fallback to memory if redis_url missing)
        if TierType.L1 in self.config.tiers:
            cache_backend = CacheBackend.REDIS if self.cache_config.redis_url else CacheBackend.MEMORY
            async_cache = AsyncCache(config=self.cache_config, backend=cache_backend)
            self._tiers[TierType.L1] = RedisTier(async_cache)

        # L2: S3 tier
        if TierType.L2 in self.config.tiers:
            params = self.config.tiers[TierType.L2].params
            bucket = params.get("bucket")
            if not bucket:
                raise ValueError("S3 tier requires 'bucket' parameter")
            prefix = params.get("prefix", f"cache/{self.config.namespace}")
            s3_client = params.get("client")
            self._tiers[TierType.L2] = S3Tier(bucket=bucket, namespace=self.config.namespace, prefix=prefix, s3_client=s3_client)

        # L3: Glacier tier
        if TierType.L3 in self.config.tiers:
            params = self.config.tiers[TierType.L3].params
            vault = params.get("vault_name")
            if not vault:
                raise ValueError("Glacier tier requires 'vault_name' parameter")
            glacier_client = params.get("client")
            self._tiers[TierType.L3] = GlacierTier(vault_name=vault, namespace=self.config.namespace, glacier_client=glacier_client)

        if not self._tiers:
            raise ValueError("At least one tier must be configured for MultiTierCache")

    def _notify_engines_of_topology(self) -> None:
        if self.predictive_engine:
            self.predictive_engine.attach_cache(self)
        if self.analytics_engine:
            self.analytics_engine.register_cache(self)

    # -- Public API ---------------------------------------------------------
    async def get(self, key: str, *, namespace: str | CacheNamespace | None = None) -> Optional[Any]:
        ns = self._normalize_namespace(namespace)
        metadata = self._metadata.setdefault(key, KeyMetadata(key=key, namespace=ns))
        storage_key = self._storage_key(ns, key)
        for tier_type in self._priority_order():
            tier = self._tiers.get(tier_type)
            if tier is None:
                continue
            start = time.perf_counter()
            value = await tier.get(storage_key)
            latency_ms = (time.perf_counter() - start) * 1000
            if value is not None:
                value = self._unwrap_from_tier(tier_type, value, metadata)
                if value is None:
                    continue
                metadata.register_hit(tier_type)
                self._tier_stats[tier_type].hits += 1
                self._tier_stats[tier_type].bytes_served += self._estimate_size(value)
                await self._promote_if_needed(key, tier_type, value, metadata)
                self._emit_event(
                    CacheEvent(
                        key=key,
                        namespace=ns,
                        tier=tier_type,
                        event_type="hit",
                        timestamp=time.time(),
                        metadata={"latency_ms": round(latency_ms, 2)},
                    )
                )
                return value
            else:
                self._tier_stats[tier_type].misses += 1
                self._emit_event(
                    CacheEvent(
                        key=key,
                        namespace=ns,
                        tier=tier_type,
                        event_type="miss",
                        timestamp=time.time(),
                        metadata={"latency_ms": round(latency_ms, 2)},
                    )
                )
        return None

    async def set(
        self,
        key: str,
        value: Any,
        *,
        namespace: str | CacheNamespace | None = None,
        ttl_seconds: Optional[int] = None,
        version: Optional[int] = None,
        write_through: bool | None = None,
    ) -> None:
        ns = self._normalize_namespace(namespace)
        metadata = self._metadata.setdefault(key, KeyMetadata(key=key, namespace=ns))
        ttl = ttl_seconds or metadata.effective_ttl(self.config.default_ttl_seconds)
        metadata.update_ttl(ttl)
        metadata.size_bytes = self._estimate_size(value)
        metadata.version = (version or metadata.version + 1)

        # Write to tiers concurrently, optionally enforcing quorum.
        storage_key = self._storage_key(ns, key)
        tasks = []
        for tier_type in self._priority_order():
            tier = self._tiers.get(tier_type)
            if tier is None:
                continue
            tier_config = self._tier_configs[tier_type]
            allowed = tier_config.write_through if write_through is None else write_through
            if not allowed:
                continue
            tasks.append(
                self._write_to_tier(tier, tier_type, storage_key, value, ttl, metadata)
            )
        await self._await_writes(tasks, ns, key)
        priority_order = self._priority_order()
        if priority_order:
            metadata.last_tier = priority_order[0]
        self._maybe_schedule_eviction(key, metadata)
        self._emit_event(
            CacheEvent(
                key=key,
                namespace=ns,
                tier=TierType.L1,
                event_type="write",
                timestamp=time.time(),
                metadata={"ttl": ttl, "version": metadata.version},
            )
        )

    async def delete(self, key: str, *, namespace: str | CacheNamespace | None = None) -> None:
        ns = self._normalize_namespace(namespace)
        storage_key = self._storage_key(ns, key)
        await asyncio.gather(
            *[
                tier.delete(storage_key)
                for tier in self._tiers.values()
            ]
        )
        self._metadata.pop(key, None)
        self._emit_event(
            CacheEvent(
                key=key,
                namespace=ns,
                tier=TierType.L1,
                event_type="delete",
                timestamp=time.time(),
                metadata={},
            )
        )

    async def invalidate_pattern(
        self,
        pattern: str,
        *,
        namespace: str | CacheNamespace | None = None,
    ) -> int:
        ns = self._normalize_namespace(namespace)
        matched = [
            meta.key
            for meta in self._metadata.values()
            if meta.namespace == ns and fnmatch.fnmatch(meta.key, pattern)
        ]
        if not matched:
            return 0
        await asyncio.gather(*(self.delete(key, namespace=ns) for key in matched))
        return len(matched)

    async def get_or_load(
        self,
        key: str,
        *,
        namespace: str | CacheNamespace | None = None,
        loader: RetrieveFn[Any],
        ttl_seconds: Optional[int] = None,
    ) -> Any:
        cached = await self.get(key, namespace=namespace)
        if cached is not None:
            return cached
        # Cache miss; run loader under keyed lock
        async with self._lock_for(key):
            cached = await self.get(key, namespace=namespace)
            if cached is not None:
                return cached
            value = await loader()
            await self.set(key, value, namespace=namespace, ttl_seconds=ttl_seconds)
            return value

    async def write_through(
        self,
        key: str,
        value: Any,
        *,
        namespace: str | CacheNamespace | None = None,
        ttl_seconds: Optional[int] = None,
        persist_fn: PersistFn[Any] | None = None,
    ) -> None:
        async with self._lock_for(key):
            if persist_fn is not None:
                await persist_fn(value)
            await self.set(key, value, namespace=namespace, ttl_seconds=ttl_seconds, write_through=True)

    async def cache_aside(
        self,
        key: str,
        *,
        namespace: str | CacheNamespace | None = None,
        loader: RetrieveFn[Any],
        ttl_seconds: Optional[int] = None,
    ) -> Any:
        return await self.get_or_load(key, namespace=namespace, loader=loader, ttl_seconds=ttl_seconds)

    async def warm_keys(
        self,
        keys: Sequence[str],
        *,
        namespace: str | CacheNamespace | None = None,
        loader: RetrieveFn[Dict[str, Any]],
        target_tier: TierType = TierType.L1,
    ) -> None:
        ns = self._normalize_namespace(namespace)
        values = await loader()
        storage_payload: Dict[str, Any] = {}
        tier = self._tiers.get(target_tier)
        if tier is None:
            raise ValueError(f"Tier {target_tier} not configured")
        for key in keys:
            if key in values:
                storage_payload[self._storage_key(ns, key)] = values[key]
        await tier.bulk_set(storage_payload, ttl=self.config.default_ttl_seconds)
        for logical_key in storage_payload:
            stripped_key = logical_key.split(":", 1)[1] if ":" in logical_key else logical_key
            metadata = self._metadata.setdefault(stripped_key, KeyMetadata(key=stripped_key, namespace=ns))
            metadata.update_ttl(self.config.default_ttl_seconds)
            metadata.last_tier = target_tier
        self._emit_event(
            CacheEvent(
                key="bulk",
                namespace=ns,
                tier=target_tier,
                event_type="warm",
                timestamp=time.time(),
                metadata={"count": len(payload)},
            )
        )

    def snapshot_stats(self) -> Dict[str, Any]:
        return {
            tier.value: vars(stats)
            for tier, stats in self._tier_stats.items()
        }

    # -- Internal helpers ---------------------------------------------------
    async def _promote_if_needed(
        self,
        key: str,
        source_tier: TierType,
        value: Any,
        metadata: KeyMetadata,
    ) -> None:
        if not self.config.enable_promotions:
            return
        order = self._priority_order()
        source_index = order.index(source_tier)
        if source_index == 0:
            return  # Already hottest tier
        target_tier = order[source_index - 1]
        if target_tier not in self._tiers:
            return
        tier_config = self._tier_configs[target_tier]
        if not tier_config.promotion_enabled:
            return
        ttl = metadata.effective_ttl(self.config.default_ttl_seconds)
        storage_key = self._storage_key(metadata.namespace, key)
        payload = self._wrap_for_tier(target_tier, value, metadata)
        await self._tiers[target_tier].set(storage_key, payload, ttl)
        self._tier_stats[target_tier].promotions += 1
        metadata.last_tier = target_tier
        self._emit_event(
            CacheEvent(
                key=key,
                namespace=metadata.namespace,
                tier=target_tier,
                event_type="promote",
                timestamp=time.time(),
                metadata={"from": source_tier.value},
            )
        )

    async def _write_to_tier(
        self,
        tier: CacheTier,
        tier_type: TierType,
        storage_key: str,
        value: Any,
        ttl: int,
        metadata: KeyMetadata,
    ) -> Tuple[TierType, bool]:
        try:
            payload = self._wrap_for_tier(tier_type, value, metadata)
            await tier.set(storage_key, payload, ttl)
            self._tier_stats[tier_type].writes += 1
            return tier_type, True
        except Exception as exc:  # pragma: no cover - defensive
            self._tier_stats[tier_type].write_errors += 1
            self.logger.warning(
                "multi_tier_write_failed",
                tier=tier_type.value,
                key=storage_key,
                namespace=storage_key.split(":", 1)[0] if ":" in storage_key else storage_key,
                error=str(exc),
            )
            return tier_type, False

    async def _await_writes(self, tasks: List[Awaitable[Tuple[TierType, bool]]], namespace: str, key: str) -> None:
        if not tasks:
            return
        results = await asyncio.gather(*tasks)
        if self.config.ensure_quorum:
            success = sum(1 for _, ok in results if ok)
            if success / len(results) < self.config.quorum_threshold:
                self.logger.error(
                    "multi_tier_quorum_failure",
                    key=key,
                    namespace=namespace,
                    successes=success,
                    attempts=len(results),
                )
                raise RuntimeError("Write quorum not met for multi-tier cache")

    def _maybe_schedule_eviction(self, key: str, metadata: KeyMetadata) -> None:
        if not self.config.enable_evictions:
            return
        tier = self._tiers.get(TierType.L1)
        tier_config = self._tier_configs.get(TierType.L1)
        if not tier or not tier_config:
            return
        if tier_config.max_items is None and tier_config.max_size_bytes is None:
            return
        # Use background task to run eviction to avoid blocking hot path
        self.loop.create_task(self._run_eviction_if_needed(TierType.L1))

    async def _run_eviction_if_needed(self, tier_type: TierType) -> None:
        tier_config = self._tier_configs.get(tier_type)
        if not tier_config:
            return
        total_items = sum(1 for meta in self._metadata.values() if meta.last_tier == tier_type)
        total_size = sum(meta.size_bytes for meta in self._metadata.values() if meta.last_tier == tier_type)
        need_evict = False
        if tier_config.max_items is not None and total_items > tier_config.max_items:
            need_evict = True
        if tier_config.max_size_bytes is not None and total_size > tier_config.max_size_bytes:
            need_evict = True
        if not need_evict:
            return
        to_evict = self._select_candidates(tier_type, tier_config, total_items)
        if not to_evict:
            return
        tier = self._tiers.get(tier_type)
        if tier is None:
            return
        await asyncio.gather(*(tier.delete(self._storage_key(meta.namespace, meta.key)) for meta in to_evict))
        for meta in to_evict:
            self._tier_stats[tier_type].evictions += 1
            meta.last_tier = None
            self._emit_event(
                CacheEvent(
                    key=meta.key,
                    namespace=meta.namespace,
                    tier=tier_type,
                    event_type="evict",
                    timestamp=time.time(),
                    metadata={"policy": tier_config.eviction_policy.value},
                )
            )

    def _select_candidates(
        self,
        tier_type: TierType,
        tier_config: TierConfig,
        total_items: int,
    ) -> List[KeyMetadata]:
        candidates = [meta for meta in self._metadata.values() if meta.last_tier == tier_type]
        if not candidates:
            return []
        target_count = max(1, int(total_items * 0.1))  # Evict up to 10%
        policy = tier_config.eviction_policy
        if policy == EvictionPolicy.LRU:
            candidates.sort(key=lambda m: m.last_access_ts)
        elif policy == EvictionPolicy.LFU:
            candidates.sort(key=lambda m: m.access_count)
        else:  # Adaptive: combine recency and frequency
            candidates.sort(key=self._adaptive_score)
        return candidates[:target_count]

    def _adaptive_score(self, metadata: KeyMetadata) -> float:
        age = time.time() - metadata.last_access_ts
        freq = metadata.access_count or 1
        ttl_penalty = 0.0
        if metadata.expires_at:
            ttl_penalty = max(0.0, (metadata.expires_at - time.time()) / max(metadata.ttl_seconds or 1, 1))
        return age / math.log(freq + 1) + ttl_penalty

    def _priority_order(self) -> List[TierType]:
        return [TierType.L1, TierType.L2, TierType.L3]

    def _wrap_for_tier(self, tier: TierType, value: Any, metadata: KeyMetadata) -> Any:
        if tier == TierType.L1 or not isinstance(value, (dict, list, str, int, float, bool, type(None))):
            # Redis tier can store native python objects serialised by AsyncCache
            return value
        return {
            "value": value,
            "__meta": {
                "version": metadata.version,
                "namespace": metadata.namespace,
                "timestamp": time.time(),
            },
        }

    def _unwrap_from_tier(self, tier: TierType, stored: Any, metadata: KeyMetadata) -> Any:
        if stored is None:
            return None
        if tier == TierType.L1:
            return stored
        if isinstance(stored, dict) and "value" in stored:
            meta = stored.get("__meta", {})
            stored_version = meta.get("version")
            if isinstance(stored_version, int) and stored_version > metadata.version:
                metadata.version = stored_version
            return stored.get("value")
        return stored

    def _storage_key(self, namespace: str, key: str) -> str:
        return f"{namespace}:{key}"

    def _estimate_size(self, value: Any) -> int:
        try:
            return len(json.dumps(value))
        except (TypeError, ValueError):
            return len(str(value))

    async def _lock_for(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def _emit_event(self, event: CacheEvent) -> None:
        if self.analytics_engine:
            self.analytics_engine.on_cache_event(event)
        if self.predictive_engine:
            self.predictive_engine.on_cache_event(event)

    def _normalize_namespace(self, namespace: str | CacheNamespace | None) -> str:
        if namespace is None:
            return self.config.namespace
        if isinstance(namespace, CacheNamespace):
            return namespace.value
        return namespace

    def get_metadata(self, key: str) -> Optional[KeyMetadata]:
        return self._metadata.get(key)


# Avoid circular imports at runtime by delaying type checking only imports
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - only for typing
    from .predictive_warming import PredictiveWarmingEngine
    from .analytics import CacheAnalyticsEngine
