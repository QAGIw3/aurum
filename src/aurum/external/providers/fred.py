"""FRED provider collectors built on the external ingestion framework."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional

from ..collect import (
    Checkpoint,
    CheckpointStore,
    ExternalCollector,
    HttpRequest,
)

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "fred_ingest_datasets.json"


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text == ".":
        return None
    try:
        return float(text)
    except ValueError:
        return None


@dataclass(frozen=True)
class FredDatasetConfig:
    """Static configuration for a single FRED series."""

    source_name: str
    series_id: str
    description: Optional[str]
    frequency: Optional[str]
    default_unit: Optional[str]
    seasonal_adjustment: Optional[str]
    window_hours: Optional[int]
    window_days: Optional[int]
    window_months: Optional[int]
    window_years: Optional[int]
    page_limit: int = 1000

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "FredDatasetConfig":
        return FredDatasetConfig(
            source_name=str(payload.get("source_name")),
            series_id=str(payload.get("series_id")),
            description=_clean(payload.get("description")),
            frequency=_clean(payload.get("frequency")),
            default_unit=_clean(payload.get("default_units")),
            seasonal_adjustment=_clean(payload.get("seasonal_adjustment")),
            window_hours=payload.get("window_hours"),
            window_days=payload.get("window_days"),
            window_months=payload.get("window_months"),
            window_years=payload.get("window_years"),
            page_limit=int(payload.get("page_limit", 1000)),
        )


def load_fred_dataset_configs(path: Path | None = None) -> List[FredDatasetConfig]:
    config_path = path or DEFAULT_CONFIG_PATH
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    datasets = payload.get("datasets", [])
    return [FredDatasetConfig.from_dict(entry) for entry in datasets]


class FredRateLimiter:
    """Enforces maximum requests per second with adaptive rate limiting for FRED API."""

    def __init__(
        self,
        rate_per_sec: float = 2.0,
        *,
        monotonic: Optional[callable] = None,
        sleep: Optional[callable] = None,
        burst_limit: int = 5,
        adaptive_backoff: bool = True
    ) -> None:
        self.rate = rate_per_sec
        self._interval = 1.0 / rate_per_sec
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or time.sleep
        self._next_allowed = self._monotonic()
        self.burst_limit = burst_limit
        self.adaptive_backoff = adaptive_backoff
        self._consecutive_errors = 0
        self._current_rate = rate_per_sec

    def acquire(self) -> None:
        now = self._monotonic()
        if now < self._next_allowed:
            delay = self._next_allowed - now
            self._sleep(delay)
            now = self._monotonic()

        # Adaptive rate limiting based on recent errors
        if self.adaptive_backoff and self._consecutive_errors > 0:
            adaptive_rate = max(self.rate * (1.0 / (1.0 + self._consecutive_errors)), 0.1)
            interval = 1.0 / adaptive_rate
        else:
            interval = self._interval

        self._next_allowed = max(self._next_allowed + interval, now)

    def record_success(self) -> None:
        """Record a successful request."""
        self._consecutive_errors = max(0, self._consecutive_errors - 1)

    def record_error(self) -> None:
        """Record an error and potentially trigger rate limiting."""
        self._consecutive_errors += 1
        if self._consecutive_errors > 3:
            self._next_allowed = self._monotonic() + (2 ** (self._consecutive_errors - 3))


class FredApiClient:
    """Enhanced wrapper for interacting with the FRED API with retry logic."""

    def __init__(
        self,
        collector: ExternalCollector,
        *,
        api_key: str,
        rate_limiter: Optional[FredRateLimiter] = None,
        max_retries: int = 5,
        timeout_seconds: int = 60,
        backoff_multiplier: float = 2.0,
        max_backoff_seconds: int = 120,
    ) -> None:
        self.collector = collector
        self.api_key = api_key
        self.rate_limiter = rate_limiter or FredRateLimiter()
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.backoff_multiplier = backoff_multiplier
        self.max_backoff_seconds = max_backoff_seconds
        self._circuit_breaker_tripped = False
        self._consecutive_failures = 0

    def _request(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        """Make a request with enhanced retry logic and error handling."""
        if self._circuit_breaker_tripped:
            raise RuntimeError("FRED API circuit breaker is tripped due to consecutive failures")

        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                self.rate_limiter.acquire()

                query: Dict[str, Any] = {"api_key": self.api_key, "file_type": "json"}
                if params:
                    query.update(params)

                response = self.collector.request(
                    HttpRequest(
                        method="GET",
                        path=path,
                        params=query,
                        timeout=self.timeout_seconds,
                    )
                )

                # Validate response
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", "60")
                    wait_time = min(int(retry_after), self.max_backoff_seconds)
                    print(f"⏱️ FRED rate limited. Waiting {wait_time}s before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(wait_time)
                    continue

                if response.status_code >= 500:
                    backoff_time = min(self.backoff_multiplier ** attempt, self.max_backoff_seconds)
                    print(f"🌐 FRED server error ({response.status_code}). Retrying in {backoff_time}s...")
                    time.sleep(backoff_time)
                    continue

                if response.status_code != 200:
                    raise RuntimeError(f"FRED API error: {response.status_code} - {response.text}")

                payload = response.json() or {}

                # Success - record it
                self.rate_limiter.record_success()
                self._consecutive_failures = 0

                return payload

            except Exception as e:
                last_exception = e
                self.rate_limiter.record_error()
                self._consecutive_failures += 1

                if attempt < self.max_retries:
                    backoff_time = min(self.backoff_multiplier ** attempt, self.max_backoff_seconds)
                    print(f"❌ FRED API request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                    print(f"⏱️ Retrying in {backoff_time}s...")
                    time.sleep(backoff_time)
                else:
                    if self._consecutive_failures >= 10:
                        self._circuit_breaker_tripped = True
                        print(f"🔴 Circuit breaker tripped after {self._consecutive_failures} consecutive failures")

        raise RuntimeError(f"FRED API request failed after {self.max_retries + 1} attempts. Last error: {last_exception}")

    def reset_circuit_breaker(self) -> None:
        """Reset the circuit breaker to allow new requests."""
        self._circuit_breaker_tripped = False
        self._consecutive_failures = 0
        print("✅ FRED API circuit breaker reset")

    def get_series(self, series_id: str) -> Optional[Mapping[str, Any]]:
        payload = self._request("fred/series", {"series_id": series_id})
        series = payload.get("seriess") or payload.get("series") or []
        if isinstance(series, Mapping):
            return series
        if isinstance(series, list) and series:
            return series[0]
        return None

    def iter_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        realtime_start: Optional[str] = None,
        realtime_end: Optional[str] = None,
        page_size: int,
    ) -> Iterator[List[Mapping[str, Any]]]:
        offset = 0
        total = None
        while True:
            params: Dict[str, Any] = {
                "series_id": series_id,
                "limit": page_size,
                "offset": offset,
                "sort_order": "asc",
            }
            if observation_start:
                params["observation_start"] = observation_start
            if observation_end:
                params["observation_end"] = observation_end
            if realtime_start:
                params["realtime_start"] = realtime_start
            if realtime_end:
                params["realtime_end"] = realtime_end

            payload = self._request("fred/series/observations", params)
            observations = payload.get("observations") or []
            if not isinstance(observations, list) or not observations:
                break
            yield observations
            count = payload.get("count")
            if total is None and isinstance(count, int):
                total = count
            offset += len(observations)
            if total is not None and offset >= total:
                break


class FredCollector:
    """Collector that syncs FRED series metadata and observations."""

    def __init__(
        self,
        dataset: FredDatasetConfig,
        *,
        api_client: FredApiClient,
        catalog_collector: ExternalCollector,
        observation_collector: ExternalCollector,
        checkpoint_store: CheckpointStore,
        now: Optional[callable] = None,
    ) -> None:
        self.dataset = dataset
        self.api_client = api_client
        self.catalog_collector = catalog_collector
        self.observation_collector = observation_collector
        self.checkpoint_store = checkpoint_store
        self._now = now or (lambda: datetime.now(timezone.utc))

    def sync_catalog(self) -> int:
        series = self.api_client.get_series(self.dataset.series_id)
        if not series:
            return 0
        record = self._map_catalog(series)
        return self.catalog_collector.emit_records([record]) if record else 0

    def ingest_observations(
        self,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        realtime_start: Optional[datetime] = None,
        realtime_end: Optional[datetime] = None,
    ) -> int:
        start_dt = start or self._default_window_start()
        end_dt = end or self._now()
        realtime_start_str = self._format_date(realtime_start) if realtime_start else None
        realtime_end_str = self._format_date(realtime_end) if realtime_end else None

        checkpoint = self.checkpoint_store.get("FRED", self.dataset.series_id)
        max_timestamp: Optional[datetime] = checkpoint.last_timestamp if checkpoint else None

        emitted = 0
        batch: List[Mapping[str, Any]] = []
        for page in self.api_client.iter_observations(
            self.dataset.series_id,
            observation_start=self._format_date(start_dt),
            observation_end=self._format_date(end_dt),
            realtime_start=realtime_start_str,
            realtime_end=realtime_end_str,
            page_size=self.dataset.page_limit,
        ):
            for raw in page:
                mapped = self._map_observation(raw)
                if mapped is None:
                    continue
                ts_micros = mapped["ts"]
                ts_dt = datetime.fromtimestamp(ts_micros / 1_000_000, tz=timezone.utc)
                if max_timestamp and ts_dt <= max_timestamp:
                    continue
                batch.append(mapped)
                max_timestamp = ts_dt if max_timestamp is None or ts_dt > max_timestamp else max_timestamp
            if batch:
                emitted += self.observation_collector.emit_records(batch)
                batch.clear()

        if max_timestamp:
            self.checkpoint_store.set(
                Checkpoint(
                    provider="FRED",
                    series_id=self.dataset.series_id,
                    last_timestamp=max_timestamp,
                    metadata={"seasonal_adjustment": self.dataset.seasonal_adjustment} if self.dataset.seasonal_adjustment else {},
                )
            )
        return emitted

    def _map_catalog(self, series: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        series_id = _clean(series.get("id")) or self.dataset.series_id
        now_ts = int(self._now().timestamp() * 1_000_000)
        record: Dict[str, Any] = {
            "provider": "FRED",
            "series_id": series_id,
            "dataset_code": series_id,
            "title": _clean(series.get("title")) or self.dataset.description or series_id,
            "description": _clean(series.get("notes")) or self.dataset.description,
            "unit_code": _clean(series.get("units")) or self.dataset.default_unit,
            "frequency_code": self.dataset.frequency,
            "geo_id": None,
            "status": _clean(series.get("seasonal_adjustment")),
            "category": _clean(series.get("group")) or _clean(series.get("parent_id")),
            "source_url": _clean(series.get("link")),
            "notes": _clean(series.get("notes")),
            "start_ts": self._parse_date(series.get("observation_start")),
            "end_ts": self._parse_date(series.get("observation_end")),
            "last_observation_ts": self._parse_datetime(series.get("last_updated")),
            "asof_date": None,
            "created_at": None,
            "updated_at": None,
            "ingest_ts": now_ts,
            "tags": None,
            "metadata": self._build_catalog_metadata(series),
            "version": None,
        }
        return record

    def _map_observation(self, raw: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        date_value = _clean(raw.get("date"))
        if not date_value:
            return None
        timestamp = self._parse_date(date_value)
        if timestamp is None:
            return None
        value = _to_float(raw.get("value"))
        value_raw = None
        if value is None:
            raw_value = _clean(raw.get("value"))
            if raw_value and raw_value != ".":
                value_raw = raw_value
        realtime_end = _clean(raw.get("realtime_end"))
        asof_date = self._asof_days(realtime_end) if realtime_end else self._timestamp_to_days(timestamp)
        metadata = {
            key: val
            for key, val in {
                "realtime_start": _clean(raw.get("realtime_start")),
                "realtime_end": realtime_end,
                "seasonal_adjustment": self.dataset.seasonal_adjustment,
            }.items()
            if val is not None
        }
        record = {
            "provider": "FRED",
            "series_id": self.dataset.series_id,
            "ts": timestamp,
            "asof_date": asof_date,
            "value": value,
            "value_raw": value_raw,
            "unit_code": self.dataset.default_unit,
            "geo_id": None,
            "dataset_code": self.dataset.series_id,
            "frequency_code": self.dataset.frequency,
            "status": _clean(raw.get("status")),
            "quality_flag": _clean(raw.get("value_status")),
            "ingest_ts": int(self._now().timestamp() * 1_000_000),
            "source_event_id": self._event_id(date_value, realtime_end),
            "metadata": metadata or None,
        }
        return record

    def _build_catalog_metadata(self, series: Mapping[str, Any]) -> Optional[Dict[str, str]]:
        metadata: Dict[str, str] = {}
        if self.dataset.seasonal_adjustment:
            metadata["seasonal_adjustment"] = self.dataset.seasonal_adjustment
        for key in ("popularity", "units_short", "seasonal_adjustment", "frequency_short"):
            value = series.get(key)
            if value is not None:
                metadata[key] = str(value)
        return metadata or None

    def _parse_date(self, value: Any) -> Optional[int]:
        text = _clean(value)
        if not text:
            return None
        try:
            dt = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
        return int(dt.timestamp() * 1_000_000)

    def _parse_datetime(self, value: Any) -> Optional[int]:
        text = _clean(value)
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            return None

    def _timestamp_to_days(self, timestamp_micros: int) -> int:
        dt = datetime.fromtimestamp(timestamp_micros / 1_000_000, tz=timezone.utc)
        epoch = date(1970, 1, 1)
        return (dt.date() - epoch).days

    def _asof_days(self, iso_date: str) -> int:
        dt = datetime.strptime(iso_date, "%Y-%m-%d").date()
        epoch = date(1970, 1, 1)
        return (dt - epoch).days

    def _event_id(self, date_value: str, realtime_end: Optional[str]) -> str:
        if realtime_end:
            return f"{self.dataset.series_id}:{date_value}:{realtime_end}"
        return f"{self.dataset.series_id}:{date_value}"

    def _format_date(self, value: datetime) -> str:
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d")

    def _default_window_start(self) -> datetime:
        now = self._now()
        if self.dataset.window_hours:
            return now - timedelta(hours=self.dataset.window_hours)
        if self.dataset.window_days:
            return now - timedelta(days=self.dataset.window_days)
        if self.dataset.window_months:
            return now - timedelta(days=30 * self.dataset.window_months)
        if self.dataset.window_years:
            return now - timedelta(days=365 * self.dataset.window_years)
        return now - timedelta(days=7)


__all__ = [
    "FredApiClient",
    "FredCollector",
    "FredDatasetConfig",
    "load_fred_dataset_configs",
]
