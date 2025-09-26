"""Base adapter and utilities for cross-ISO ingestion.

This layer builds upon aurum.external.collect to add:
  - Circuit breaker protection
  - Idempotent paging via CheckpointStore
  - Chunking helpers for time-bounded windows
  - Minimal ISO-agnostic metrics bundle
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, date
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple
from pathlib import Path
import json
import os
import hashlib
from aurum.iso.base import IsoDataType

try:  # Python 3.9+
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover - fallback
    ZoneInfo = None  # type: ignore

from ..collect import (
    Checkpoint,
    CheckpointStore,
    CollectorConfig,
    CollectorContext,
    CollectorMetrics,
    ExternalCollector,
    HttpRequest,
    HttpRequestError,
    PostgresCheckpointStore,
    RateLimitConfig,
    RetryConfig,
)


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """Circuit breaker tuning parameters."""

    error_threshold: int = 5
    window_seconds: int = 60
    open_seconds: int = 120


class CircuitOpenError(RuntimeError):
    pass


class _CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig) -> None:
        self.config = config
        self._first_error_at: Optional[float] = None
        self._error_count: int = 0
        self._opened_at: Optional[float] = None

    def allow(self, now: float) -> bool:
        if self._opened_at is None:
            return True
        if now - self._opened_at >= self.config.open_seconds:
            # half-open
            self._opened_at = None
            self._error_count = 0
            self._first_error_at = None
            return True
        return False

    def record_success(self) -> None:
        self._first_error_at = None
        self._error_count = 0
        # keep open state as-is (only allow() changes it)

    def record_error(self, now: float) -> None:
        if self._first_error_at is None or (now - self._first_error_at) > self.config.window_seconds:
            self._first_error_at = now
            self._error_count = 1
            return
        self._error_count += 1
        if self._error_count >= self.config.error_threshold:
            self._opened_at = now


@dataclass(frozen=True)
class IsoAdapterConfig:
    provider: str
    base_url: str
    kafka_topic: str
    schema_registry_url: Optional[str] = None
    kafka_bootstrap_servers: Optional[str] = None
    default_headers: Mapping[str, str] = field(default_factory=dict)
    retry: RetryConfig = field(default_factory=RetryConfig)
    rate_limit: Optional[RateLimitConfig] = None
    http_timeout: float = 30.0
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    checkpoint_namespace: str = "aurum:collect:checkpoint"


@dataclass(frozen=True)
class IsoRequestChunk:
    """Time-bounded chunk request descriptor."""

    start: datetime
    end: datetime
    cursor: Optional[str] = None
    params: Mapping[str, Any] = field(default_factory=dict)


class IsoIngestMetrics:
    """Small convenience wrapper around CollectorMetrics."""

    def __init__(self, metrics: CollectorMetrics) -> None:
        self._m = metrics

    def http_snapshot(self) -> Mapping[str, Any]:
        return self._m.snapshot()


class IdempotentPager:
    """Manages paging based on last page token and/or last timestamp per series."""

    def __init__(self, store: CheckpointStore, provider: str, series_id: str) -> None:
        self.store = store
        self.provider = provider
        self.series_id = series_id
        self._state: Optional[Checkpoint] = None

    def load(self) -> Optional[Checkpoint]:
        self._state = self.store.get(self.provider, self.series_id)
        return self._state

    def last_cursor(self) -> Optional[str]:
        if not self._state:
            self.load()
        return self._state.last_page if self._state else None

    def last_ts(self) -> Optional[datetime]:
        if not self._state:
            self.load()
        return self._state.last_timestamp if self._state else None

    def commit(self, *, last_page: Optional[str], last_ts: Optional[datetime], metadata: Optional[Mapping[str, Any]] = None) -> None:
        cp = Checkpoint(
            provider=self.provider,
            series_id=self.series_id,
            last_timestamp=last_ts,
            last_page=last_page,
            metadata=dict(metadata or {}),
        )
        self.store.set(cp)


class IsoAdapter:
    """Composable base adapter with circuit breaker and idempotent paging.

    Subclasses must implement:
      - build_request(chunk: IsoRequestChunk) -> HttpRequest
      - parse_page(payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]
        returning (records, next_cursor)
    """

    def __init__(
        self,
        config: IsoAdapterConfig,
        *,
        series_id: str,
        checkpoint_store: Optional[CheckpointStore] = None,
        context: Optional[CollectorContext] = None,
        collector_factory: Optional[Callable[[CollectorConfig, CollectorContext], ExternalCollector]] = None,
        value_schema_name: str = "iso.lmp.v1.avsc",
    ) -> None:
        self.config = config
        # If a schema registry is configured, load the default value schema unless provided externally
        value_schema: Optional[Mapping[str, Any]] = None
        if config.schema_registry_url:
            try:
                schema_dir_env = os.getenv("AURUM_SCHEMA_DIR")
                schema_dir = Path(schema_dir_env) if schema_dir_env else Path(__file__).resolve().parents[3] / "kafka" / "schemas"
                schema_path = schema_dir / value_schema_name
                if schema_path.exists():
                    value_schema = json.loads(schema_path.read_text(encoding="utf-8"))
            except Exception:
                value_schema = None

        self.collector_config = CollectorConfig(
            provider=config.provider,
            base_url=config.base_url,
            kafka_topic=config.kafka_topic,
            kafka_bootstrap_servers=(
                config.kafka_bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
            ),
            schema_registry_url=config.schema_registry_url or os.getenv("SCHEMA_REGISTRY_URL"),
            value_schema=value_schema,
            default_headers=config.default_headers,
            retry=config.retry,
            rate_limit=config.rate_limit,
            http_timeout=config.http_timeout,
        )
        self.context = context or CollectorContext()
        self.collector = (
            collector_factory(self.collector_config, self.context)
            if collector_factory
            else ExternalCollector(self.collector_config, context=self.context)
        )
        self.series_id = series_id
        self.metrics = IsoIngestMetrics(self.collector.metrics)
        self.circuit = _CircuitBreaker(config.circuit_breaker)
        self.pager = IdempotentPager(
            checkpoint_store or PostgresCheckpointStore(ensure_schema=True),
            provider=config.provider,
            series_id=series_id,
        )

    # --- Hooks to override per adapter ---
    def build_request(self, chunk: IsoRequestChunk) -> HttpRequest:  # pragma: no cover - abstract
        raise NotImplementedError

    def parse_page(self, payload: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], Optional[str]]:  # pragma: no cover - abstract
        raise NotImplementedError

    def value_transform(self, record: Mapping[str, Any]) -> Mapping[str, Any]:
        """Default transform: project canonical ISO fields to IsoLmpRecord Avro shape.

        If a record already resembles the Avro shape (has iso_code, market, interval_start,
        location_id, price_total, ingest_ts, record_hash), the function returns it as-is.
        Otherwise it attempts a best-effort mapping from canonicalized fields.
        """
        # If it already looks like an IsoLmpRecord, passthrough
        if (
            record.get("iso_code")
            and record.get("interval_start") is not None
            and record.get("location_id")
            and record.get("price_total") is not None
        ):
            out = dict(record)
        else:
            out = {}

        # Canonical contract fields
        metadata = dict(record.get("metadata") or {})
        iso_code = record.get("iso_code") or metadata.get("iso_code") or (record.get("iso_contract") or {}).get("iso_code")
        market_raw = (
            record.get("market")
            or record.get("iso_market")
            or metadata.get("iso_market")
            or metadata.get("market")
        )
        tz = (
            record.get("timezone")
            or record.get("iso_timezone")
            or metadata.get("iso_timezone")
            or metadata.get("timezone")
        )
        # Timestamps
        interval_start = _to_micros(record.get("interval_start") or record.get("timestamp") or record.get("begin"))
        interval_end = _to_micros(record.get("interval_end") or record.get("end"))
        interval_minutes = _to_int(record.get("interval_minutes") or metadata.get("interval_minutes"))

        # Location
        location_id = (
            record.get("location_id")
            or record.get("iso_location_id")
            or metadata.get("iso_location_id")
            or metadata.get("location_id")
        )
        location_name = (
            record.get("location_name")
            or record.get("iso_location_name")
            or metadata.get("iso_location_name")
        )
        location_type_raw = (
            record.get("location_type")
            or record.get("iso_location_type")
            or metadata.get("iso_location_type")
            or metadata.get("location_type")
        )
        location_type = _normalize_location_type(location_type_raw)

        # Optional zone/hub passthroughs
        zone = record.get("zone")
        hub = record.get("hub")

        # Prices
        price_total = _to_float(record.get("price_total") or record.get("lmp"))
        price_energy = _to_opt_float(record.get("price_energy") or record.get("energy") or metadata.get("price_energy"))
        price_congestion = _to_opt_float(record.get("price_congestion") or record.get("congestion") or metadata.get("price_congestion"))
        price_loss = _to_opt_float(record.get("price_loss") or record.get("loss") or metadata.get("price_loss"))

        currency = str(record.get("currency") or metadata.get("currency") or "USD")
        uom = str(record.get("uom") or metadata.get("uom") or _uom_from_unit(record.get("iso_unit") or metadata.get("iso_unit")) or "MWh")

        settlement_point = record.get("settlement_point")
        source_run_id = record.get("source_run_id") or metadata.get("source_run_id")

        ingest_ts = _to_micros(record.get("ingest_ts") or datetime.now(tz=timezone.utc))
        avro_metadata = _stringify_map(metadata)

        market = _map_market_enum(market_raw)
        delivery_date = _derive_delivery_date(interval_start, tz)

        out.update(
            {
                "iso_code": str(iso_code or "UNKNOWN"),
                "market": market,
                "delivery_date": delivery_date,
                "interval_start": interval_start,
                "interval_end": interval_end,
                "interval_minutes": interval_minutes,
                "location_id": str(location_id or ""),
                "location_name": (str(location_name) if location_name is not None else None),
                "location_type": location_type,
                "zone": (str(zone) if zone is not None else None),
                "hub": (str(hub) if hub is not None else None),
                "timezone": (str(tz) if tz is not None else None),
                "price_total": float(price_total or 0.0),
                "price_energy": price_energy,
                "price_congestion": price_congestion,
                "price_loss": price_loss,
                "currency": currency,
                "uom": uom,
                "settlement_point": (str(settlement_point) if settlement_point is not None else None),
                "source_run_id": (str(source_run_id) if source_run_id is not None else None),
                "ingest_ts": ingest_ts,
                "metadata": avro_metadata,
            }
        )

        # Ensure record_hash exists and is stable across retries
        if not out.get("record_hash"):
            out["record_hash"] = _compute_record_hash(
                out["iso_code"], out["location_id"], out["interval_start"], out["price_total"]
            )

        return out

    def key_fn(self, record: Mapping[str, Any]) -> Optional[str]:
        return None

    def headers_fn(self, record: Mapping[str, Any]) -> Optional[Sequence[Tuple[str, str]]]:
        return None

    # --- Core execution ---
    def run_window(self, chunk: IsoRequestChunk, *, flush: bool = True) -> int:
        """Run ingestion for a single request chunk, paging idempotently."""
        emitted = 0
        # short-circuit via circuit breaker
        now = self.collector._monotonic()
        if not self.circuit.allow(now):
            raise CircuitOpenError("circuit is open; refusing to call upstream")

        # resume cursor if present
        cursor = chunk.cursor or self.pager.last_cursor()
        last_ts = self.pager.last_ts() or chunk.start

        while True:
            request = self.build_request(
                IsoRequestChunk(start=last_ts, end=chunk.end, cursor=cursor, params=chunk.params)
            )
            try:
                response = self.collector.request(request)
                records, next_cursor = self.parse_page(response.json() or {})
                if not records:
                    self.circuit.record_success()
                    break
                emitted += self.collector.emit_records(
                    records,
                    value_transform=self.value_transform,
                    key_fn=self.key_fn,
                    headers_fn=self.headers_fn,
                    flush=False,
                )
                # advance state
                max_ts = max((r.get("interval_end") or r.get("interval_start") or last_ts) for r in records)
                if isinstance(max_ts, (int, float)):
                    # ts in micros
                    max_ts = datetime.fromtimestamp(max_ts / 1_000_000.0, tz=timezone.utc)
                last_ts = max_ts or last_ts
                cursor = next_cursor
                # persist checkpoint every page
                self.pager.commit(last_page=cursor, last_ts=last_ts, metadata={"window_end": chunk.end.isoformat()})
                self.circuit.record_success()
                if not next_cursor:
                    break
            except HttpRequestError as exc:  # upstream failure with retry exhaustion
                self.circuit.record_error(self.collector._monotonic())
                raise

        if flush and emitted:
            self.collector.flush()
        return emitted

    # --- Chunking helpers ---
    @staticmethod
    def hourly_chunks(start: datetime, end: datetime, *, step_hours: int = 1) -> Iterator[IsoRequestChunk]:
        cur = start
        while cur < end:
            nxt = min(end, cur + timedelta(hours=step_hours))
            yield IsoRequestChunk(start=cur, end=nxt)
            cur = nxt

    @staticmethod
    def daily_chunks(start: datetime, end: datetime, *, step_days: int = 1) -> Iterator[IsoRequestChunk]:
        cur = start
        while cur < end:
            nxt = min(end, cur + timedelta(days=step_days))
            yield IsoRequestChunk(start=cur, end=nxt)
            cur = nxt


# --- Helper utilities for Avro projection ---
def _to_micros(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # assume micros if large, else seconds
        if isinstance(value, float):
            return int(value)
        if value > 10_000_000_000:  # already micros (roughly after 1970)
            return int(value)
        # seconds -> micros
        return int(value * 1_000_000)
    if isinstance(value, str):
        try:
            # Support ISO 8601 strings
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1_000_000)
    return None


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def _to_opt_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def _uom_from_unit(unit: Any) -> Optional[str]:
    if not unit:
        return None
    try:
        unit_str = str(unit).upper()
        if "/" in unit_str:
            return unit_str.split("/")[-1]
        return unit_str
    except Exception:
        return None


def _map_market_enum(market: Any) -> str:
    if not market:
        return "UNKNOWN"
    m = str(market).strip().upper()
    if m in ("DA", "DAM", "DAY-AHEAD", "DAY_AHEAD"):
        return "DAY_AHEAD"
    if m in ("RT", "RTM", "REALTIME", "REAL-TIME", "REAL_TIME"):
        return "REAL_TIME"
    if m in ("RTPD", "FIFTEEN", "FIFTEEN_MINUTE", "15MIN", "15_MIN"):
        return "FIFTEEN_MINUTE"
    return "UNKNOWN"


def _normalize_location_type(value: Any) -> str:
    allowed = {"NODE", "ZONE", "HUB", "SYSTEM", "AGGREGATE", "INTERFACE", "RESOURCE", "OTHER"}
    if not value:
        return "OTHER"
    v = str(value).strip().upper()
    return v if v in allowed else "OTHER"


def _derive_delivery_date(interval_start_micros: Optional[int], tz_name: Optional[str]) -> int:
    # Avro logical date: days since epoch
    epoch = date(1970, 1, 1)
    if interval_start_micros is None:
        # Default to today in UTC
        return (datetime.now(timezone.utc).date() - epoch).days
    seconds = interval_start_micros / 1_000_000.0
    dt_utc = datetime.fromtimestamp(seconds, tz=timezone.utc)
    if tz_name and ZoneInfo:
        try:
            local = dt_utc.astimezone(ZoneInfo(str(tz_name)))
            return (local.date() - epoch).days
        except Exception:
            pass
    return (dt_utc.date() - epoch).days


def _compute_record_hash(iso_code: str, location_id: str, interval_start_micros: int, price_total: float) -> str:
    base = f"{iso_code}|{location_id}|{interval_start_micros}|{price_total}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _stringify_map(metadata: Mapping[str, Any]) -> Mapping[str, str]:
    out: Dict[str, str] = {}
    for k, v in metadata.items():
        if v is None:
            continue
        if isinstance(v, str):
            out[k] = v
        elif isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        else:
            try:
                import json

                out[k] = json.dumps(v, sort_keys=True, default=str)
            except Exception:
                out[k] = str(v)
    return out
