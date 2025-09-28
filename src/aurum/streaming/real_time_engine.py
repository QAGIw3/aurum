"""Real-time market data engine with interpolation, reconciliation, and alerts."""
from __future__ import annotations

import asyncio
import math
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

import numpy as np
import pandas as pd

try:  # Import lightweight ML helpers if available
    from aurum.ml import SimpleExpSmoothingForecaster, detect_anomalies
except Exception:  # pragma: no cover - optional import safety
    SimpleExpSmoothingForecaster = None  # type: ignore[assignment]
    detect_anomalies = None  # type: ignore[assignment]


@dataclass(frozen=True)
class CurvePoint:
    """Represents a price point on a curve."""

    tenor: str
    price: float
    timestamp: datetime
    volume: float | None = None
    source: str = "realtime"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tenor": self.tenor,
            "price": self.price,
            "timestamp": self.timestamp.isoformat(),
            "volume": self.volume,
            "source": self.source,
        }


@dataclass(frozen=True)
class MarketDataEvent:
    """Incoming event representing vendor-provided market data."""

    curve_id: str
    tenor: str
    price: float
    timestamp: datetime
    vendor: str | None = None
    volume: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "curve_id": self.curve_id,
            "tenor": self.tenor,
            "price": self.price,
            "timestamp": self.timestamp.isoformat(),
            "vendor": self.vendor,
            "volume": self.volume,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class MarketAlert:
    """Alert emitted by the engine when a rule is triggered."""

    name: str
    level: str
    message: str
    details: Mapping[str, Any]
    triggered_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "level": self.level,
            "message": self.message,
            "details": dict(self.details),
            "triggered_at": self.triggered_at.isoformat(),
        }


@dataclass(frozen=True)
class MarketAlertRule:
    """Definition for computing alerts from market events."""

    name: str
    level: str
    description: str
    evaluator: Callable[[MarketDataEvent, "RealTimeSnapshot", Mapping[str, Any]], Optional[MarketAlert]]

    def evaluate(
        self,
        event: MarketDataEvent,
        snapshot: "RealTimeSnapshot",
        diagnostics: Mapping[str, Any],
    ) -> Optional[MarketAlert]:
        alert = self.evaluator(event, snapshot, diagnostics)
        if alert is None:
            return None
        return alert


@dataclass(frozen=True)
class ReconciliationItem:
    tenor: str
    realtime_price: float
    historical_price: float
    delta: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tenor": self.tenor,
            "realtime_price": self.realtime_price,
            "historical_price": self.historical_price,
            "delta": self.delta,
        }


@dataclass(frozen=True)
class ReconciliationReport:
    """Comparison of real-time data against historical baselines."""

    curve_id: str
    items: Sequence[ReconciliationItem]
    max_delta: float
    mean_delta: float
    generated_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "curve_id": self.curve_id,
            "generated_at": self.generated_at.isoformat(),
            "max_delta": self.max_delta,
            "mean_delta": self.mean_delta,
            "items": [item.to_dict() for item in self.items],
        }


@dataclass(frozen=True)
class RealTimeSnapshot:
    """Snapshot of the curve state after an event is ingested."""

    curve_id: str
    points: Sequence[CurvePoint]
    interpolated: Sequence[CurvePoint]
    statistics: Mapping[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "curve_id": self.curve_id,
            "points": [point.to_dict() for point in self.points],
            "interpolated": [point.to_dict() for point in self.interpolated],
            "statistics": dict(self.statistics),
        }


@dataclass(frozen=True)
class RealTimeIngestReport:
    """Outcome of processing an incoming market data event."""

    event: MarketDataEvent
    snapshot: RealTimeSnapshot
    alerts: Sequence[MarketAlert]
    reconciliation: ReconciliationReport | None
    diagnostics: Mapping[str, Any]
    inference: Mapping[str, Any] | None = None

    def to_payload(self) -> Dict[str, Any]:
        return {
            "event": self.event.to_dict(),
            "snapshot": self.snapshot.to_dict(),
            "alerts": [alert.to_dict() for alert in self.alerts],
            "reconciliation": self.reconciliation.to_dict() if self.reconciliation else None,
            "diagnostics": dict(self.diagnostics),
            "inference": dict(self.inference) if isinstance(self.inference, Mapping) else None,
        }


@dataclass
class _CurveState:
    curve_id: str
    points: Dict[str, CurvePoint] = field(default_factory=dict)
    historical: Dict[str, CurvePoint] = field(default_factory=dict)
    update_count: int = 0
    last_event_at: datetime | None = None
    last_latency_ms: float = 0.0
    last_price_by_tenor: Dict[str, float] = field(default_factory=dict)


Listener = Callable[[RealTimeIngestReport], Awaitable[None]]


class RealTimeMarketDataEngine:
    """In-memory processing engine for streaming curve updates."""

    def __init__(
        self,
        *,
        alert_rules: Iterable[MarketAlertRule] | None = None,
    ) -> None:
        self._lock = asyncio.Lock()
        self._curves: MutableMapping[str, _CurveState] = {}
        self._listeners: Dict[str, Listener] = {}
        self._curve_listeners: Dict[str, Dict[str, Listener]] = defaultdict(dict)
        self._alert_rules: list[MarketAlertRule] = list(alert_rules or _default_alert_rules())

    async def ingest_event(self, event: MarketDataEvent) -> RealTimeIngestReport:
        """Ingest an event, update state, and notify listeners."""
        async with self._lock:
            state = self._curves.setdefault(event.curve_id, _CurveState(curve_id=event.curve_id))
            previous_point = state.points.get(event.tenor)
            state.update_count += 1

            timestamp = _ensure_utc(event.timestamp)
            now = datetime.now(timezone.utc)
            latency_ms = max(0.0, (now - timestamp).total_seconds() * 1000.0)
            state.last_latency_ms = latency_ms
            state.last_event_at = now

            point = CurvePoint(
                tenor=event.tenor,
                price=event.price,
                timestamp=timestamp,
                volume=event.volume,
                source="realtime",
            )
            state.points[event.tenor] = point
            state.last_price_by_tenor[event.tenor] = event.price

            snapshot = self._build_snapshot(state)
            reconciliation = self._build_reconciliation(state)

            diagnostics: Dict[str, Any] = {
                "latency_ms": latency_ms,
                "previous_point": previous_point.to_dict() if previous_point else None,
                "update_count": state.update_count,
            }

        alerts = self._evaluate_alerts(event, snapshot, diagnostics)
        inference = self._compute_inference(state)

        report = RealTimeIngestReport(
            event=event,
            snapshot=snapshot,
            alerts=alerts,
            reconciliation=reconciliation,
            diagnostics=diagnostics,
            inference=inference,
        )

        await self._notify_listeners(report)
        return report

    async def add_historical_curve(self, curve_id: str, points: Sequence[CurvePoint | Mapping[str, Any]]) -> None:
        """Seed historical baseline data for reconciliation computations."""
        async with self._lock:
            state = self._curves.setdefault(curve_id, _CurveState(curve_id=curve_id))
            state.historical = {point.tenor: _coerce_point(point, source="historical") for point in points}

    async def clear_historical_curve(self, curve_id: str) -> None:
        async with self._lock:
            state = self._curves.get(curve_id)
            if state:
                state.historical.clear()

    def register_listener(self, listener: Listener) -> str:
        """Register a global listener invoked for all events."""
        token = uuid.uuid4().hex
        self._listeners[token] = listener
        return token

    def unregister_listener(self, token: str) -> None:
        self._listeners.pop(token, None)

    def subscribe_curve(self, curve_id: str, listener: Listener) -> str:
        token = uuid.uuid4().hex
        self._curve_listeners[curve_id][token] = listener
        return token

    def unsubscribe_curve(self, curve_id: str, token: str) -> None:
        listeners = self._curve_listeners.get(curve_id)
        if listeners is not None:
            listeners.pop(token, None)
            if not listeners:
                self._curve_listeners.pop(curve_id, None)

    def add_alert_rule(self, rule: MarketAlertRule) -> None:
        self._alert_rules.append(rule)

    def remove_alert_rule(self, name: str) -> None:
        self._alert_rules = [rule for rule in self._alert_rules if rule.name != name]

    async def get_snapshot(self, curve_id: str) -> RealTimeSnapshot | None:
        async with self._lock:
            state = self._curves.get(curve_id)
            if state is None:
                return None
            return self._build_snapshot(state)

    async def get_reconciliation(self, curve_id: str) -> ReconciliationReport | None:
        async with self._lock:
            state = self._curves.get(curve_id)
            if state is None:
                return None
            return self._build_reconciliation(state)

    async def _notify_listeners(self, report: RealTimeIngestReport) -> None:
        listeners = list(self._listeners.values())
        listeners.extend(self._curve_listeners.get(report.event.curve_id, {}).values())

        if not listeners:
            return

        coroutines = [listener(report) for listener in listeners]
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):  # pragma: no cover - defensive guard
                # Re-raise? Here we log via print to avoid logging dependency.
                print(f"RealTimeMarketDataEngine listener failure: {result}")

    def _build_snapshot(self, state: _CurveState) -> RealTimeSnapshot:
        points = tuple(sorted(state.points.values(), key=lambda p: self._sort_key(p.tenor)))
        interpolated = self._compute_interpolated_points(state, points)
        statistics = {
            "update_count": float(state.update_count),
            "latency_ms": float(state.last_latency_ms),
            "last_event_age_ms": self._compute_event_age_ms(state.last_event_at),
            "tenor_count": float(len(points)),
        }
        return RealTimeSnapshot(curve_id=state.curve_id, points=points, interpolated=interpolated, statistics=statistics)

    def _build_reconciliation(self, state: _CurveState) -> ReconciliationReport | None:
        if not state.historical:
            return None
        items: list[ReconciliationItem] = []
        for tenor, historical_point in state.historical.items():
            realtime_point = state.points.get(tenor)
            if not realtime_point:
                continue
            delta = realtime_point.price - historical_point.price
            items.append(
                ReconciliationItem(
                    tenor=tenor,
                    realtime_price=realtime_point.price,
                    historical_price=historical_point.price,
                    delta=delta,
                )
            )
        if not items:
            return None
        deltas = [abs(item.delta) for item in items]
        return ReconciliationReport(
            curve_id=state.curve_id,
            items=tuple(items),
            max_delta=max(deltas),
            mean_delta=float(sum(deltas) / len(deltas)),
            generated_at=datetime.now(timezone.utc),
        )

    def _compute_inference(self, state: _CurveState) -> Mapping[str, Any] | None:
        """Compute online inference signals: short-horizon forecast and anomalies."""
        try:
            points = tuple(sorted(state.points.values(), key=lambda p: self._sort_key(p.tenor)))
            if not points:
                return None

            series, is_time = self._build_series(points)

            forecast_payload: list[Mapping[str, Any]] = []
            if SimpleExpSmoothingForecaster is not None and len(series) >= 5:
                try:
                    model = SimpleExpSmoothingForecaster(alpha=0.3)
                    model.fit(series)
                    # infer frequency if available; default to 'D' for datetime index
                    freq = None
                    if isinstance(series.index, pd.DatetimeIndex):
                        inferred = series.index.inferred_freq
                        freq = inferred or "D"
                    res = model.forecast(steps=3, freq=freq)
                    for ts, val in res.predictions.items():
                        ts_iso = ts.isoformat() if isinstance(ts, pd.Timestamp) else None
                        forecast_payload.append(
                            {
                                "tenor": ts_iso or str(ts),
                                "timestamp": ts_iso,
                                "price": float(val),
                                "model": res.model_name,
                            }
                        )
                except Exception:
                    forecast_payload = []

            anomaly_payload: list[Mapping[str, Any]] = []
            if detect_anomalies is not None and len(series) >= 6:
                try:
                    window = max(6, min(24, len(series) // 2))
                    anomalies_df = detect_anomalies(series, window=window, z_threshold=3.0)
                    if not anomalies_df.empty:
                        # Focus on anomalies that involve the latest point
                        last_idx = series.index[-1]
                        latest = anomalies_df[anomalies_df["timestamp"] == last_idx]
                        if latest.empty:
                            latest = anomalies_df.tail(1)
                        for _, row in latest.iterrows():
                            ts = row["timestamp"]
                            anomaly_payload.append(
                                {
                                    "timestamp": ts.isoformat() if isinstance(ts, pd.Timestamp) else str(ts),
                                    "value": float(row.get("value", float("nan"))) if not pd.isna(row.get("value")) else None,
                                    "z_score": float(row.get("z_score", float("nan"))),
                                    "side": row.get("side", "neutral"),
                                }
                            )
                except Exception:
                    anomaly_payload = []

            return {
                "forecast": forecast_payload,
                "anomalies": anomaly_payload,
            }
        except Exception:
            return None

    def _build_series(self, points: Sequence[CurvePoint]) -> tuple[pd.Series, bool]:
        """Build a pandas Series from ordered points, preferring time-based indexing."""
        # Attempt time-based index first
        numeric_ts: list[tuple[pd.Timestamp, float]] = []
        for p in points:
            key = self._tenor_key(p.tenor)
            if key is None:
                continue
            # Heuristic: treat numeric key as epoch seconds and build UTC timestamps
            try:
                ts = datetime.fromtimestamp(float(key), tz=timezone.utc)
            except Exception:
                continue
            numeric_ts.append((ts, float(p.price)))

        if len(numeric_ts) >= 3:
            numeric_ts.sort(key=lambda kv: kv[0])
            idx = [kv[0] for kv in numeric_ts]
            vals = [kv[1] for kv in numeric_ts]
            s = pd.Series(vals, index=pd.DatetimeIndex(idx))
            return s, True

        # Fallback: simple positional series
        s = pd.Series([float(p.price) for p in points])
        return s, False

    def _compute_interpolated_points(
        self,
        state: _CurveState,
        ordered_points: Sequence[CurvePoint],
    ) -> tuple[CurvePoint, ...]:
        # Build interpolation anchors from live points and, if necessary,
        # supplement with historical anchors when only a single live point exists.

        anchors: dict[float, float] = {}
        for point in ordered_points:
            numeric = self._tenor_key(point.tenor)
            if numeric is None:
                continue
            anchors[numeric] = point.price

        if len(anchors) < 2 and state.historical:
            for hist in state.historical.values():
                numeric = self._tenor_key(hist.tenor)
                if numeric is None or numeric in anchors:
                    continue
                anchors[numeric] = hist.price
                if len(anchors) >= 2:
                    break

        if len(anchors) < 2:
            return tuple()

        source_keys = sorted(anchors.keys())
        source_values = [anchors[key] for key in source_keys]

        target_tenors = set(point.tenor for point in ordered_points)
        target_tenors.update(state.historical.keys())

        interpolated: list[CurvePoint] = []
        for tenor in sorted(target_tenors, key=self._sort_key):
            numeric = self._tenor_key(tenor)
            if numeric is None:
                continue
            estimate = float(np.interp(numeric, source_keys, source_values))
            interpolated.append(
                CurvePoint(
                    tenor=tenor,
                    price=estimate,
                    timestamp=datetime.now(timezone.utc),
                    source="interpolated",
                )
            )
        return tuple(interpolated)

    def _evaluate_alerts(
        self,
        event: MarketDataEvent,
        snapshot: RealTimeSnapshot,
        diagnostics: Mapping[str, Any],
    ) -> tuple[MarketAlert, ...]:
        alerts: list[MarketAlert] = []
        for rule in self._alert_rules:
            alert = rule.evaluate(event, snapshot, diagnostics)
            if alert is not None:
                alerts.append(alert)
        return tuple(alerts)

    @staticmethod
    def _compute_event_age_ms(last_event_at: datetime | None) -> float:
        if last_event_at is None:
            return math.inf
        return max(0.0, (datetime.now(timezone.utc) - last_event_at).total_seconds() * 1000.0)

    @staticmethod
    def _tenor_key(tenor: str) -> float | None:
        try:
            return float(tenor)
        except (TypeError, ValueError):
            pass
        text = str(tenor)
        for pattern in ("%Y-%m-%d", "%Y-%m", "%Y%m%d"):
            try:
                dt = datetime.strptime(text, pattern)
            except ValueError:
                continue
            else:
                return dt.replace(tzinfo=timezone.utc).timestamp()
        return None

    @staticmethod
    def _sort_key(tenor: str) -> tuple[int, Any]:
        numeric = RealTimeMarketDataEngine._tenor_key(tenor)
        if numeric is not None:
            return (0, numeric)
        return (1, str(tenor))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coerce_point(point: CurvePoint | Mapping[str, Any], *, source: str) -> CurvePoint:
    if isinstance(point, CurvePoint):
        return point
    tenor = str(point["tenor"])
    price = float(point["price"])
    timestamp = point.get("timestamp")
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    if not isinstance(timestamp, datetime):
        timestamp = datetime.now(timezone.utc)
    else:
        timestamp = _ensure_utc(timestamp)
    volume = point.get("volume")
    if volume is not None:
        volume = float(volume)
    return CurvePoint(tenor=tenor, price=price, timestamp=timestamp, volume=volume, source=source)


def _default_alert_rules() -> list[MarketAlertRule]:
    def price_spike_rule(event: MarketDataEvent, snapshot: RealTimeSnapshot, diagnostics: Mapping[str, Any]) -> Optional[MarketAlert]:
        previous = diagnostics.get("previous_point")
        if not previous:
            return None
        previous_price = float(previous.get("price", 0.0))
        if previous_price == 0.0:
            return None
        change_pct = (event.price - previous_price) / abs(previous_price)
        if abs(change_pct) < 0.05:
            return None
        return MarketAlert(
            name="price_spike",
            level="warning" if abs(change_pct) < 0.15 else "critical",
            message=f"Price changed by {change_pct:.1%}",
            details={
                "change_pct": change_pct,
                "previous_price": previous_price,
                "current_price": event.price,
                "tenor": event.tenor,
            },
            triggered_at=datetime.now(timezone.utc),
        )

    return [
        MarketAlertRule(
            name="price_spike",
            level="warning",
            description="Detects sudden price shifts larger than 5%",
            evaluator=price_spike_rule,
        )
    ]


__all__ = [
    "CurvePoint",
    "MarketAlert",
    "MarketAlertRule",
    "MarketDataEvent",
    "RealTimeIngestReport",
    "RealTimeMarketDataEngine",
    "ReconciliationReport",
]
