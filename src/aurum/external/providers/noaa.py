"""NOAA CDO collectors with rate limiting and quota enforcement."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional

from ..collect import (
    Checkpoint,
    CheckpointStore,
    ExternalCollector,
    HttpRequest,
)

DEFAULT_BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "noaa_ingest_datasets.json"


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class NoaaDatasetConfig:
    """Configuration for a NOAA dataset ingest job."""

    dataset_id: str
    dataset: str
    stations: List[str] = field(default_factory=list)
    datatypes: List[str] = field(default_factory=list)
    location_category: Optional[str] = None
    locations: List[str] = field(default_factory=list)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    window_days: int = 7
    page_limit: int = 1000

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "NoaaDatasetConfig":
        return NoaaDatasetConfig(
            dataset_id=str(payload.get("dataset_id")),
            dataset=str(payload.get("dataset")),
            stations=list(payload.get("stations", [])),
            datatypes=list(payload.get("datatypes", [])),
            location_category=_clean(payload.get("location_category")),
            locations=list(payload.get("locations", [])),
            start_date=_clean(payload.get("start_date")),
            end_date=_clean(payload.get("end_date")),
            window_days=int(payload.get("window_days", 7)),
            page_limit=int(payload.get("page_limit", 1000)),
        )


def load_noaa_dataset_configs(path: Path | None = None) -> List[NoaaDatasetConfig]:
    import json

    config_path = path or DEFAULT_CONFIG_PATH
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return [NoaaDatasetConfig.from_dict(entry) for entry in payload.get("datasets", [])]


class NoaaRateLimiter:
    """Enforces maximum requests per second with adaptive rate limiting."""

    def __init__(
        self,
        rate_per_sec: float = 5.0,
        *,
        monotonic: Optional[callable] = None,
        sleep: Optional[callable] = None,
        burst_limit: int = 10,
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
            # Reduce rate when experiencing errors
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
        # Exponential backoff for the next request
        if self._consecutive_errors > 3:
            self._next_allowed = self._monotonic() + (2 ** (self._consecutive_errors - 3))

    @property
    def current_rate(self) -> float:
        """Get current effective rate."""
        return self._current_rate


class DailyQuota:
    """Enhanced daily quota tracker with alerts and burst handling."""

    def __init__(
        self,
        limit: int,
        *,
        today_fn: Optional[callable] = None,
        warning_threshold: float = 0.8,
        critical_threshold: float = 0.95,
        enable_burst: bool = True,
        burst_size: int = 100
    ) -> None:
        self.limit = limit
        self._today_fn = today_fn or (lambda: date.today())
        self._today = self._today_fn()
        self._count = 0
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.enable_burst = enable_burst
        self.burst_size = burst_size
        self._burst_used = 0
        self._alerts_sent = set()

    def consume(self) -> None:
        today = self._today_fn()
        if today != self._today:
            self._today = today
            self._count = 0
            self._burst_used = 0
            self._alerts_sent.clear()

        if not self.limit:
            return  # No limit set

        # Check if we're within burst allowance
        if self.enable_burst and self._burst_used < self.burst_size:
            self._burst_used += 1
            return

        # Regular quota consumption
        if self._count >= self.limit:
            raise RuntimeError(f"NOAA daily quota exceeded: {self._count}/{self.limit}")

        self._count += 1

        # Send alerts based on thresholds
        usage_ratio = self._count / self.limit

        if usage_ratio >= self.critical_threshold and "critical" not in self._alerts_sent:
            print(f"🚨 CRITICAL: NOAA quota usage at {usage_ratio:.1%} ({self._count}/{self.limit})")
            self._alerts_sent.add("critical")

        elif usage_ratio >= self.warning_threshold and "warning" not in self._alerts_sent:
            print(f"⚠️ WARNING: NOAA quota usage at {usage_ratio:.1%} ({self._count}/{self.limit})")
            self._alerts_sent.add("warning")

    @property
    def remaining(self) -> Optional[int]:
        if not self.limit:
            return None
        return max(0, self.limit - self._count)

    @property
    def usage_ratio(self) -> float:
        if not self.limit:
            return 0.0
        return self._count / self.limit

    @property
    def is_critical(self) -> bool:
        return self.usage_ratio >= self.critical_threshold

    @property
    def is_warning(self) -> bool:
        return self.usage_ratio >= self.warning_threshold


class NoaaApiClient:
    """Enhanced wrapper around :class:`ExternalCollector` with auth, throttling, quota, and robust error handling."""

    def __init__(
        self,
        collector: ExternalCollector,
        *,
        token: str,
        rate_limiter: Optional[NoaaRateLimiter] = None,
        quota: Optional[DailyQuota] = None,
        base_url: str = DEFAULT_BASE_URL,
        max_retries: int = 5,
        timeout_seconds: int = 45,
        backoff_multiplier: float = 2.0,
        max_backoff_seconds: int = 120,
    ) -> None:
        self.collector = collector
        self.token = token
        self.rate_limiter = rate_limiter or NoaaRateLimiter()
        self.quota = quota
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.backoff_multiplier = backoff_multiplier
        self.max_backoff_seconds = max_backoff_seconds
        self._circuit_breaker_tripped = False
        self._consecutive_failures = 0

    def request(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        """Make a request with enhanced retry logic and error handling."""
        if self._circuit_breaker_tripped:
            raise RuntimeError("NOAA API circuit breaker is tripped due to consecutive failures")

        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                # Rate limiting
                self.rate_limiter.acquire()

                # Quota management
                if self.quota:
                    try:
                        self.quota.consume()
                    except RuntimeError as quota_error:
                        print(f"🚫 NOAA quota exceeded: {quota_error}")
                        raise quota_error

                headers = {"token": self.token}

                # Make the request
                response = self.collector.request(
                    HttpRequest(
                        method="GET",
                        path=f"{self.base_url}{path}",
                        params=params,
                        headers=headers,
                        timeout=self.timeout_seconds,
                    )
                )

                # Validate response
                if response.status_code == 429:
                    # Rate limited
                    retry_after = response.headers.get("Retry-After", "60")
                    wait_time = min(int(retry_after), self.max_backoff_seconds)
                    print(f"⏱️ NOAA rate limited. Waiting {wait_time}s before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(wait_time)
                    continue

                if response.status_code >= 500:
                    # Server error - exponential backoff
                    backoff_time = min(self.backoff_multiplier ** attempt, self.max_backoff_seconds)
                    print(f"🌐 NOAA server error ({response.status_code}). Retrying in {backoff_time}s...")
                    time.sleep(backoff_time)
                    continue

                if response.status_code != 200:
                    raise RuntimeError(f"NOAA API error: {response.status_code} - {response.text}")

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
                    print(f"❌ NOAA API request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                    print(f"⏱️ Retrying in {backoff_time}s...")
                    time.sleep(backoff_time)
                else:
                    # Check if we should trip circuit breaker
                    if self._consecutive_failures >= 10:
                        self._circuit_breaker_tripped = True
                        print(f"🔴 Circuit breaker tripped after {self._consecutive_failures} consecutive failures")

        # All retries exhausted
        raise RuntimeError(f"NOAA API request failed after {self.max_retries + 1} attempts. Last error: {last_exception}")

    def reset_circuit_breaker(self) -> None:
        """Reset the circuit breaker to allow new requests."""
        self._circuit_breaker_tripped = False
        self._consecutive_failures = 0
        print("✅ NOAA API circuit breaker reset")

    def iterate(self, path: str, params: Mapping[str, Any], *, page_limit: int) -> Iterator[List[Mapping[str, Any]]]:
        offset = int(params.get("offset", 0))
        while True:
            query = dict(params)
            query.update({"offset": offset, "limit": page_limit})
            payload = self.request(path, query)
            results = payload.get("results") or []
            if not isinstance(results, list) or not results:
                break
            yield results
            metadata = payload.get("metadata", {})
            resultset = metadata.get("resultset", {})
            count = resultset.get("count")
            offset += len(results)
            if count is None or offset >= count:
                break


class NoaaCollector:
    """Collects NOAA observations with station/datatype discovery."""

    def __init__(
        self,
        dataset: NoaaDatasetConfig,
        *,
        api_client: NoaaApiClient,
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

    def discover_stations(self) -> List[str]:
        if self.dataset.stations:
            return self.dataset.stations
        params = {"datasetid": self.dataset.dataset}
        if self.dataset.location_category:
            params["locationcategoryid"] = self.dataset.location_category
        if self.dataset.locations:
            params["locationid"] = self.dataset.locations
        stations: List[str] = []
        for page in self.api_client.iterate("/stations", params, page_limit=1000):
            for item in page:
                station_id = _clean(item.get("id"))
                if station_id:
                    stations.append(station_id)
        return stations

    def discover_datatypes(self) -> List[str]:
        if self.dataset.datatypes:
            return self.dataset.datatypes
        params = {"datasetid": self.dataset.dataset}
        datatypes: List[str] = []
        for page in self.api_client.iterate("/datatypes", params, page_limit=1000):
            for item in page:
                dtype = _clean(item.get("id"))
                if dtype:
                    datatypes.append(dtype)
        return datatypes

    def sync_catalog(self) -> int:
        stations = self.discover_stations()
        datatypes = self.discover_datatypes()
        records: List[Mapping[str, Any]] = []
        now_ts = int(self._now().timestamp() * 1_000_000)
        for station in stations:
            for dtype in datatypes:
                series_id = f"{self.dataset.dataset}:{station}:{dtype}"
                records.append(
                    {
                        "provider": "NOAA",
                        "series_id": series_id,
                        "dataset_code": self.dataset.dataset,
                        "title": series_id,
                        "description": f"NOAA {dtype} measurements at {station}",
                        "unit_code": None,
                        "frequency_code": None,
                        "geo_id": station,
                        "status": None,
                        "category": dtype,
                        "source_url": None,
                        "notes": None,
                        "start_ts": None,
                        "end_ts": None,
                        "last_observation_ts": None,
                        "asof_date": None,
                        "created_at": None,
                        "updated_at": None,
                        "ingest_ts": now_ts,
                        "tags": None,
                        "metadata": {"station": station, "datatype": dtype},
                        "version": None,
                    }
                )
        if not records:
            return 0
        return self.catalog_collector.emit_records(records)

    def ingest_observations(self, *, start: Optional[datetime] = None, end: Optional[datetime] = None) -> int:
        stations = self.discover_stations()
        datatypes = self.discover_datatypes()
        if not stations or not datatypes:
            return 0

        start_dt = start or self._default_window_start()
        end_dt = min(end or self._now(), self._derive_end_bound())
        emitted = 0

        for station in stations:
            for dtype in datatypes:
                series_id = f"{self.dataset.dataset}:{station}:{dtype}"
                checkpoint = self.checkpoint_store.get("NOAA", series_id)
                max_ts = checkpoint.last_timestamp if checkpoint else None
                params = {
                    "datasetid": self.dataset.dataset,
                    "stationid": station,
                    "datatypeid": dtype,
                    "startdate": self._format_date(start_dt),
                    "enddate": self._format_date(end_dt),
                    "units": "metric",
                    "includemetadata": "false",
                }
                batch: List[Mapping[str, Any]] = []
                for page in self.api_client.iterate("/data", params, page_limit=self.dataset.page_limit):
                    for record in page:
                        mapped = self._map_observation(series_id, station, dtype, record)
                        if mapped is None:
                            continue
                        ts_dt = datetime.fromtimestamp(mapped["ts"] / 1_000_000, tz=timezone.utc)
                        if max_ts and ts_dt <= max_ts:
                            continue
                        batch.append(mapped)
                        if max_ts is None or ts_dt > max_ts:
                            max_ts = ts_dt
                    if batch:
                        emitted += self.observation_collector.emit_records(batch)
                        batch.clear()
                if max_ts:
                    self.checkpoint_store.set(
                        Checkpoint(
                            provider="NOAA",
                            series_id=series_id,
                            last_timestamp=max_ts,
                            metadata={"station": station, "datatype": dtype},
                        )
                    )
        return emitted

    def _map_observation(self, series_id: str, station: str, datatype: str, raw: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        value = raw.get("value")
        date_value = _clean(raw.get("date"))
        if value is None or date_value is None:
            return None
        ts = self._parse_iso(date_value)
        if ts is None:
            return None
        ingest_ts = int(self._now().timestamp() * 1_000_000)
        return {
            "provider": "NOAA",
            "series_id": series_id,
            "ts": ts,
            "asof_date": self._timestamp_to_days(ts),
            "value": float(value) if isinstance(value, (int, float)) else None,
            "value_raw": None if isinstance(value, (int, float)) else str(value),
            "unit_code": None,
            "geo_id": station,
            "dataset_code": self.dataset.dataset,
            "frequency_code": None,
            "status": _clean(raw.get("attributes")),
            "quality_flag": None,
            "ingest_ts": ingest_ts,
            "source_event_id": f"{series_id}:{date_value}",
            "metadata": None,
        }

    def _default_window_start(self) -> datetime:
        return self._now() - timedelta(days=self.dataset.window_days)

    def _derive_end_bound(self) -> datetime:
        if self.dataset.end_date:
            dt = datetime.strptime(self.dataset.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return dt
        return self._now()

    def _format_date(self, dt: datetime) -> str:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")

    def _parse_iso(self, value: str) -> Optional[int]:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except ValueError:
            return None

    def _timestamp_to_days(self, timestamp_micros: int) -> int:
        dt = datetime.fromtimestamp(timestamp_micros / 1_000_000, tz=timezone.utc)
        epoch = date(1970, 1, 1)
        return (dt.date() - epoch).days


__all__ = [
    "DailyQuota",
    "NoaaApiClient",
    "NoaaCollector",
    "NoaaDatasetConfig",
    "NoaaRateLimiter",
    "load_noaa_dataset_configs",
]
