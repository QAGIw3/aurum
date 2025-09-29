"""Predictive cache warming based on usage patterns.

The `PredictiveWarmingEngine` consumes cache events emitted by
`MultiTierCache` and applies a lightweight statistical model to determine
which keys are likely to be needed soon. Selected keys are proactively
loaded into the hottest tier, reducing latency for repeated requests and
flattening load on downstream data providers.
"""

from __future__ import annotations

import asyncio
import contextlib
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from aurum.logging.structured_logger import get_logger

from .multi_tier import CacheEvent, TierType


LoaderFn = Callable[[Sequence[str]], Awaitable[Dict[str, Any]]]


@dataclass
class PredictiveWindowConfig:
    """Configuration for predictive warming."""

    history_minutes: int = 60
    evaluation_interval_seconds: int = 60
    min_hits: int = 3
    max_gap_seconds: int = 300
    top_k: int = 20
    min_confidence: float = 0.6
    target_tier: TierType = TierType.L1
    autostart: bool = True
    warm_batch_size: int = 10
    min_unique_clients: int = 1


class PredictiveWarmingEngine:
    """Learns cache access patterns and performs proactive warming."""

    def __init__(self, config: PredictiveWindowConfig | None = None, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.config = config or PredictiveWindowConfig()
        self.loop = loop or asyncio.get_event_loop()
        self.logger = get_logger(__name__)

        self._cache: "MultiTierCache | None" = None
        self._loaders: Dict[str, LoaderFn] = {}
        self._activity: Dict[str, Deque[Tuple[float, str]]] = defaultdict(deque)
        self._client_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._shutdown = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    def attach_cache(self, cache: "MultiTierCache") -> None:
        self._cache = cache
        if self.config.autostart and (self._task is None or self._task.done()):
            self.start()

    def register_loader(self, namespace: str, loader: LoaderFn) -> None:
        """Register an origin loader for a namespace."""
        self._loaders[namespace] = loader

    def deregister_loader(self, namespace: str) -> None:
        self._loaders.pop(namespace, None)

    def on_cache_event(self, event: CacheEvent) -> None:
        if event.event_type not in {"hit", "miss"}:
            return
        key = self._make_key(event.namespace, event.key)
        window_seconds = self.config.history_minutes * 60
        now = event.timestamp
        entries = self._activity[key]
        entries.append((now, event.tier.value))
        while entries and now - entries[0][0] > window_seconds:
            entries.popleft()
        client_id = event.metadata.get("client_id") if isinstance(event.metadata, dict) else None
        if client_id:
            self._client_counts[key][client_id] += 1

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._shutdown.clear()
        self._task = self.loop.create_task(self._run_loop())

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    # ------------------------------------------------------------------
    async def _run_loop(self) -> None:
        interval = max(5, self.config.evaluation_interval_seconds)
        try:
            while not self._shutdown.is_set():
                try:
                    await self._evaluate_and_warm()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # pragma: no cover - defensive
                    self.logger.warning("predictive_warming_cycle_error", error=str(exc))
                await asyncio.sleep(interval)
        finally:
            self._task = None

    async def _evaluate_and_warm(self) -> None:
        if not self._cache:
            return
        hot_candidates = await self._select_hot_keys()
        if not hot_candidates:
            return
        grouped: Dict[str, List[str]] = defaultdict(list)
        for ns, key, _confidence in hot_candidates:
            grouped[ns].append(key)
        for namespace, keys in grouped.items():
            loader = self._loaders.get(namespace)
            if not loader:
                continue
            for batch in self._batched(keys, self.config.warm_batch_size):
                try:
                    await self._cache.warm_keys(
                        batch,
                        namespace=namespace,
                        loader=lambda batch=batch, loader=loader: loader(batch),
                        target_tier=self.config.target_tier,
                    )
                    self.logger.debug(
                        "predictive_warming_executed",
                        namespace=namespace,
                        keys=list(batch),
                        tier=self.config.target_tier.value,
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    self.logger.warning(
                        "predictive_warming_failed",
                        namespace=namespace,
                        keys=list(batch),
                        error=str(exc),
                    )

    async def _select_hot_keys(self) -> List[Tuple[str, str, float]]:
        async with self._lock:
            snapshot = {key: list(events) for key, events in self._activity.items() if events}
        window_seconds = self.config.history_minutes * 60
        cutoff = time.time() - window_seconds
        selections: List[Tuple[str, str, float]] = []
        for composite_key, events in snapshot.items():
            filtered = [ts for ts, _tier in events if ts >= cutoff]
            if len(filtered) < self.config.min_hits:
                continue
            gaps = [filtered[i] - filtered[i - 1] for i in range(1, len(filtered))]
            if not gaps:
                continue
            mean_gap = statistics.mean(gaps)
            std_gap = statistics.pstdev(gaps) if len(gaps) > 1 else 0.0
            if mean_gap > self.config.max_gap_seconds:
                continue
            confidence = self._confidence(len(filtered), mean_gap, std_gap)
            namespace, logical_key = self._split_key(composite_key)
            client_counts = self._client_counts.get(composite_key, {})
            unique_clients = len(client_counts) if client_counts else 1
            if unique_clients < self.config.min_unique_clients:
                continue
            if confidence >= self.config.min_confidence:
                selections.append((namespace, logical_key, confidence))
        selections.sort(key=lambda item: item[2], reverse=True)
        return selections[: self.config.top_k]

    def _confidence(self, hits: int, mean_gap: float, std_gap: float) -> float:
        # Higher hits and lower variability increase confidence.
        normalized_hits = min(1.0, hits / (self.config.min_hits * 3))
        variability = std_gap / max(mean_gap, 1e-6)
        stability = max(0.0, 1.0 - min(1.0, variability))
        gap_score = max(0.0, 1.0 - (mean_gap / max(self.config.max_gap_seconds, 1)))
        return round((0.5 * normalized_hits + 0.3 * stability + 0.2 * gap_score), 3)

    def _make_key(self, namespace: str, key: str) -> str:
        return f"{namespace}:{key}"

    def _split_key(self, composite: str) -> Tuple[str, str]:
        namespace, _, key = composite.partition(":")
        return namespace, key

    def _batched(self, iterable: Iterable[str], size: int) -> Iterable[List[str]]:
        batch: List[str] = []
        for item in iterable:
            batch.append(item)
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch


# Avoid circular import at runtime
from typing import TYPE_CHECKING
if TYPE_CHECKING:  # pragma: no cover - typing only
    from .multi_tier import MultiTierCache
