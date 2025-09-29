"""Cache analytics and optimization recommendations for the multi-tier cache."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence

from aurum.logging.structured_logger import get_logger
from aurum.observability.metrics import get_metrics_client

from .multi_tier import CacheEvent, TierType


AlertHandler = Callable[[str, Dict[str, Any]], None]


@dataclass
class AnalyticsConfig:
    evaluation_interval_seconds: int = 60
    max_event_history: int = 10_000
    hit_rate_warning_threshold: float = 0.8
    hit_rate_critical_threshold: float = 0.6
    latency_warning_ms: float = 150.0
    latency_critical_ms: float = 300.0
    pressure_scale_threshold: float = 0.65
    recent_window_seconds: int = 900
    emit_metrics: bool = True


@dataclass
class CacheOptimizationAdvice:
    summary: str
    hit_rate: float
    tier_pressure: Dict[str, float]
    suggested_actions: List[str]
    alerts: List[str] = field(default_factory=list)
    telemetry: Dict[str, Any] = field(default_factory=dict)


class CacheAnalyticsEngine:
    """Collects cache telemetry and generates optimization advice."""

    def __init__(self, config: AnalyticsConfig | None = None) -> None:
        self.config = config or AnalyticsConfig()
        self.logger = get_logger(__name__)
        self.metrics = get_metrics_client()

        self._cache: "MultiTierCache | None" = None
        self._alert_handler: AlertHandler | None = None
        self._events: Deque[CacheEvent] = deque(maxlen=self.config.max_event_history)
        self._tier_latencies: Dict[TierType, Deque[float]] = defaultdict(lambda: deque(maxlen=500))
        self._tier_hits: Dict[TierType, int] = defaultdict(int)
        self._tier_misses: Dict[TierType, int] = defaultdict(int)
        self._tier_evictions: Dict[TierType, int] = defaultdict(int)
        self._last_evaluation_ts: float = 0.0
        self._latest_advice: CacheOptimizationAdvice | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    def register_cache(self, cache: "MultiTierCache") -> None:
        self._cache = cache

    def on_cache_event(self, event: CacheEvent) -> None:
        if event.event_type in {"hit", "miss", "evict", "promote", "write"}:
            self._events.append(event)
        if event.event_type == "hit":
            self._tier_hits[event.tier] += 1
        elif event.event_type == "miss":
            self._tier_misses[event.tier] += 1
        elif event.event_type == "evict":
            self._tier_evictions[event.tier] += 1

        latency = 0.0
        if isinstance(event.metadata, dict) and "latency_ms" in event.metadata:
            latency = float(event.metadata["latency_ms"])
            self._tier_latencies[event.tier].append(latency)
            if self.config.emit_metrics and self.metrics:
                self.metrics.histogram(
                    "cache_event_latency_seconds",
                    latency / 1000.0,
                    labels={"tier": event.tier.value, "event": event.event_type},
                )
        if self.config.emit_metrics and self.metrics:
            self.metrics.counter(
                "cache_event_total",
                labels={"tier": event.tier.value, "event": event.event_type},
            )

        now = time.time()
        if now - self._last_evaluation_ts >= self.config.evaluation_interval_seconds:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._evaluate_async())
            except RuntimeError:
                # No running loop, evaluate synchronously
                advice = self._evaluate_state()
                self._latest_advice = advice
                if advice.alerts:
                    self._emit_alerts(advice.alerts, advice.telemetry)
            self._last_evaluation_ts = now

    async def _evaluate_async(self) -> None:
        async with self._lock:
            advice = self._evaluate_state()
            self._latest_advice = advice
            if advice.alerts:
                self._emit_alerts(advice.alerts, advice.telemetry)

    def _evaluate_state(self) -> CacheOptimizationAdvice:
        snapshot_events = list(self._events)
        total_hits = sum(self._tier_hits.values())
        total_misses = sum(self._tier_misses.values())
        hit_rate = total_hits / max(total_hits + total_misses, 1)
        tier_pressure: Dict[str, float] = {}
        for tier in TierType:
            hits = self._tier_hits.get(tier, 0)
            misses = self._tier_misses.get(tier, 0)
            tier_total = hits + misses
            pressure = misses / max(tier_total, 1)
            tier_pressure[tier.value] = round(pressure, 3)
        alerts: List[str] = []
        if hit_rate < self.config.hit_rate_warning_threshold:
            severity = "critical" if hit_rate < self.config.hit_rate_critical_threshold else "warning"
            alerts.append(f"overall hit rate {hit_rate:.2%} below {severity} threshold")
        for tier, latencies in self._tier_latencies.items():
            if not latencies:
                continue
            p95 = self._percentile(latencies, 95)
            if p95 > self.config.latency_warning_ms:
                severity = "critical" if p95 > self.config.latency_critical_ms else "warning"
                alerts.append(f"{tier.value} latency p95 {p95:.1f}ms exceeds {severity} threshold")
        actions = self._derive_actions(tier_pressure, hit_rate, snapshot_events)
        summary = self._summarize(hit_rate, tier_pressure, actions)
        telemetry = {
            "hit_rate": round(hit_rate, 4),
            "tier_pressure": tier_pressure,
            "total_events": len(snapshot_events),
        }
        return CacheOptimizationAdvice(
            summary=summary,
            hit_rate=round(hit_rate, 4),
            tier_pressure=tier_pressure,
            suggested_actions=actions,
            alerts=alerts,
            telemetry=telemetry,
        )

    def _derive_actions(self, tier_pressure: Dict[str, float], hit_rate: float, events: Sequence[CacheEvent]) -> List[str]:
        actions: List[str] = []
        l1_pressure = tier_pressure.get(TierType.L1.value, 0.0)
        l2_pressure = tier_pressure.get(TierType.L2.value, 0.0)
        if l1_pressure > self.config.pressure_scale_threshold:
            actions.append("Increase L1 capacity or reduce TTL for cold namespaces")
        if l2_pressure > self.config.pressure_scale_threshold:
            actions.append("Enable aggressive promotion from L2 to L1 for recurring keys")
        if hit_rate < self.config.hit_rate_warning_threshold:
            actions.append("Expand predictive warming coverage or review cache-aside loaders")
        recent_misses = [e for e in events if e.event_type == "miss" and (time.time() - e.timestamp) < self.config.recent_window_seconds]
        if len(recent_misses) > 50:
            actions.append("Investigate upstream data freshness or adjust write-through strategy")
        return actions

    def _summarize(self, hit_rate: float, tier_pressure: Dict[str, float], actions: Sequence[str]) -> str:
        summary = f"Hit rate {hit_rate:.1%}; L1 pressure {tier_pressure.get(TierType.L1.value, 0.0):.1%}"
        if actions:
            summary += f"; next steps: {actions[0]}"
        return summary

    def register_alert_handler(self, handler: AlertHandler) -> None:
        self._alert_handler = handler

    def _emit_alerts(self, alerts: Sequence[str], telemetry: Dict[str, Any]) -> None:
        if not alerts:
            return
        self.logger.warning("cache_alert", alerts=list(alerts), telemetry=telemetry)
        if self._alert_handler:
            try:
                self._alert_handler("cache_alert", {"alerts": list(alerts), **telemetry})
            except Exception:  # pragma: no cover - user provided handler
                self.logger.exception("cache_alert_handler_failed")

    def latest_recommendation(self) -> CacheOptimizationAdvice | None:
        return self._latest_advice

    def metrics_snapshot(self) -> Dict[str, Any]:
        return {
            "hit_rate": self._latest_advice.hit_rate if self._latest_advice else None,
            "tier_pressure": self._latest_advice.tier_pressure if self._latest_advice else {},
            "l1_p95_latency": self._p95_latency(TierType.L1),
            "l2_p95_latency": self._p95_latency(TierType.L2),
            "l3_p95_latency": self._p95_latency(TierType.L3),
        }

    def _p95_latency(self, tier: TierType) -> Optional[float]:
        latencies = self._tier_latencies.get(tier)
        if not latencies:
            return None
        return self._percentile(latencies, 95)

    def _percentile(self, values: Sequence[float], percentile: float) -> float:
        ordered = sorted(values)
        if not ordered:
            return 0.0
        if len(ordered) == 1:
            return float(ordered[0])
        k = max(0, min(len(ordered) - 1, int(round((percentile / 100) * (len(ordered) - 1)))))
        return float(ordered[k])


from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .multi_tier import MultiTierCache
