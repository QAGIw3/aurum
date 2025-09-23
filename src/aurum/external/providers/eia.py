"""EIA provider collectors built on the external ingestion framework."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence

from ..collect import (
    Checkpoint,
    CheckpointStore,
    ExternalCollector,
    HttpRequest,
    HttpResponse,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "eia_ingest_datasets.generated.json"
FALLBACK_CONFIG_PATH = PROJECT_ROOT / "config" / "eia_ingest_datasets.json"


def _is_simple_identifier(value: str | None) -> Optional[str]:
    if not value:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    if all(ch.isalnum() or ch == "_" for ch in stripped) and stripped[0].isalpha():
        return stripped
    return None


def _clean_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class EiaDatasetConfig:
    """Runtime configuration describing how to fetch and map a dataset."""

    source_name: str
    data_path: str
    catalog_path: str
    series_id_field: str = "series_id"
    period_field: str = "period"
    value_field: str = "value"
    unit_field: Optional[str] = "unit"
    title_field: Optional[str] = "name"
    description_field: Optional[str] = "description"
    dataset_code: Optional[str] = None
    frequency_code: Optional[str] = None
    default_unit: Optional[str] = None
    default_frequency: Optional[str] = None
    metadata_fields: Sequence[str] = field(default_factory=tuple)
    filter_params: Mapping[str, str] = field(default_factory=dict)
    page_limit: int = 1000
    window_days: Optional[int] = None
    window_months: Optional[int] = None
    window_years: Optional[int] = None

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "EiaDatasetConfig":
        series_field = _is_simple_identifier(payload.get("series_id_expr")) or "series_id"
        metadata_fields: list[str] = []
        for key in (payload.get("metadata_expr") or "").split(","):
            cleaned = key.strip().strip("{}")
            if cleaned:
                metadata_fields.append(cleaned)
        window_days = payload.get("window_days")
        window_months = payload.get("window_months")
        window_years = payload.get("window_years")
        return EiaDatasetConfig(
            source_name=str(payload.get("source_name")),
            data_path=str(payload.get("data_path")),
            catalog_path=str(payload.get("path")) + "/series",
            series_id_field=series_field,
            period_field=str(payload.get("period_column", "period")),
            value_field="value",
            dataset_code=_clean_string(payload.get("path")),
            frequency_code=_clean_string(payload.get("frequency")),
            default_unit=_clean_string(payload.get("default_units")),
            default_frequency=_clean_string(payload.get("frequency")),
            metadata_fields=tuple(metadata_fields),
            page_limit=int(payload.get("page_limit", 1000)),
            filter_params={k: v for override in payload.get("param_overrides", []) for k, v in override.items()},
            window_days=window_days,
            window_months=window_months,
            window_years=window_years,
        )


def load_eia_dataset_configs(path: Path | None = None) -> List[EiaDatasetConfig]:
    config_path = path or DEFAULT_CONFIG_PATH
    if not config_path.exists():
        # Generated dataset manifest is optional in local dev; fall back to the
        # checked-in specification so callers (tests, utilities) still work.
        config_path = FALLBACK_CONFIG_PATH
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return [EiaDatasetConfig.from_dict(entry) for entry in payload.get("datasets", [])]


class EiaRateLimiter:
    """Enforces maximum requests per second with adaptive rate limiting for EIA API."""

    def __init__(
        self,
        rate_per_sec: float = 3.0,
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


class EiaApiClient:
    """Enhanced wrapper around :class:`ExternalCollector` for paginated EIA queries with retry logic."""

    def __init__(
        self,
        collector: ExternalCollector,
        *,
        api_key: str,
        rate_limiter: Optional[EiaRateLimiter] = None,
        max_retries: int = 5,
        timeout_seconds: int = 60,
        backoff_multiplier: float = 2.0,
        max_backoff_seconds: int = 120,
    ) -> None:
        self.collector = collector
        self.api_key = api_key
        self.rate_limiter = rate_limiter or EiaRateLimiter()
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.backoff_multiplier = backoff_multiplier
        self.max_backoff_seconds = max_backoff_seconds
        self._circuit_breaker_tripped = False
        self._consecutive_failures = 0

    def request(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        """Make a request with enhanced retry logic and error handling."""
        if self._circuit_breaker_tripped:
            raise RuntimeError("EIA API circuit breaker is tripped due to consecutive failures")

        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                self.rate_limiter.acquire()

                query = {"api_key": self.api_key}
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
                    print(f"⏱️ EIA rate limited. Waiting {wait_time}s before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(wait_time)
                    continue

                if response.status_code >= 500:
                    backoff_time = min(self.backoff_multiplier ** attempt, self.max_backoff_seconds)
                    print(f"🌐 EIA server error ({response.status_code}). Retrying in {backoff_time}s...")
                    time.sleep(backoff_time)
                    continue

                if response.status_code != 200:
                    raise RuntimeError(f"EIA API error: {response.status_code} - {response.text}")

                payload = self._unwrap(response)

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
                    print(f"❌ EIA API request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                    print(f"⏱️ Retrying in {backoff_time}s...")
                    time.sleep(backoff_time)
                else:
                    if self._consecutive_failures >= 10:
                        self._circuit_breaker_tripped = True
                        print(f"🔴 Circuit breaker tripped after {self._consecutive_failures} consecutive failures")

        raise RuntimeError(f"EIA API request failed after {self.max_retries + 1} attempts. Last error: {last_exception}")

    def reset_circuit_breaker(self) -> None:
        """Reset the circuit breaker to allow new requests."""
        self._circuit_breaker_tripped = False
        self._consecutive_failures = 0
        print("✅ EIA API circuit breaker reset")

    def paginate(
        self,
        path: str,
        base_params: Optional[Mapping[str, Any]] = None,
        *,
        page_size: int,
    ) -> Iterator[List[Dict[str, Any]]]:
        params = dict(base_params or {})
        offset = int(params.pop("offset", 0))
        while True:
            page_params = dict(params)
            page_params.update({"offset": offset, "length": page_size})
            payload = self.request(path, page_params)
            data = payload.get("data") or []
            if not data:
                break
            yield data
            next_info = payload.get("next") or {}
            next_offset = next_info.get("offset")
            if next_offset is None or next_offset == offset:
                break
            offset = int(next_offset)

    @staticmethod
    def _unwrap(response: HttpResponse) -> Dict[str, Any]:
        payload = response.json() or {}
        if "response" in payload:
            return payload["response"] or {}
        return payload


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class EiaCollector:
    """Coordinates EIA catalog + observation ingestion with checkpointing."""

    def __init__(
        self,
        dataset: EiaDatasetConfig,
        *,
        api_client: EiaApiClient,
        catalog_collector: ExternalCollector,
        observation_collector: ExternalCollector,
        checkpoint_store: CheckpointStore,
        now: Callable[[], datetime] = _now_utc,
    ) -> None:
        self.dataset = dataset
        self.api_client = api_client
        self.catalog_collector = catalog_collector
        self.observation_collector = observation_collector
        self.checkpoint_store = checkpoint_store
        self._now = now

    def sync_catalog(self) -> int:
        records: List[Mapping[str, Any]] = []
        for page in self.api_client.paginate(
            self.dataset.catalog_path,
            base_params=self.dataset.filter_params,
            page_size=self.dataset.page_limit,
        ):
            for raw in page:
                mapped = self._map_catalog_record(raw)
                if mapped:
                    records.append(mapped)
        if not records:
            return 0
        emitted = self.catalog_collector.emit_records(records)
        return emitted

    def ingest_observations(
        self,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> int:
        start = start or self._default_window_start()
        end = end or self._now()
        params = dict(self.dataset.filter_params)
        params.update({
            "start": self._format_period(start),
            "end": self._format_period(end),
            "sort": "period",
        })
        per_series_max: Dict[str, datetime] = {}
        emitted = 0
        checkpoints: Dict[str, Optional[Checkpoint]] = {}

        for page in self.api_client.paginate(
            self.dataset.data_path,
            base_params=params,
            page_size=self.dataset.page_limit,
        ):
            batch: List[Mapping[str, Any]] = []
            for raw in page:
                series_id = _clean_string(raw.get(self.dataset.series_id_field))
                if not series_id:
                    continue
                if series_id not in checkpoints:
                    checkpoints[series_id] = self.checkpoint_store.get("EIA", series_id)
                checkpoint = checkpoints[series_id]
                mapped = self._map_observation_record(raw, series_id)
                if mapped is None:
                    continue
                ts = mapped["ts"]
                ts_dt = datetime.fromtimestamp(ts / 1_000_000, tz=timezone.utc)
                if checkpoint and checkpoint.last_timestamp and ts_dt <= checkpoint.last_timestamp:
                    continue
                batch.append(mapped)
                per_series_max[series_id] = max(per_series_max.get(series_id, datetime.min.replace(tzinfo=timezone.utc)), ts_dt)
            if batch:
                emitted += self.observation_collector.emit_records(batch)

        for series_id, max_ts in per_series_max.items():
            self.checkpoint_store.set(
                Checkpoint(
                    provider="EIA",
                    series_id=series_id,
                    last_timestamp=max_ts,
                    metadata={"dataset": self.dataset.dataset_code or self.dataset.source_name},
                )
            )
        return emitted

    def _map_catalog_record(self, raw: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        series_id = _clean_string(raw.get(self.dataset.series_id_field))
        if not series_id:
            return None
        now_ts = int(self._now().timestamp() * 1_000_000)
        title = _clean_string(raw.get(self.dataset.title_field or "name")) or series_id
        record: Dict[str, Any] = {
            "provider": "EIA",
            "series_id": series_id,
            "dataset_code": self.dataset.dataset_code,
            "title": title,
            "description": _clean_string(raw.get(self.dataset.description_field or "description")),
            "unit_code": _clean_string(raw.get(self.dataset.unit_field or "unit")) or self.dataset.default_unit,
            "frequency_code": self.dataset.frequency_code or self.dataset.default_frequency,
            "geo_id": _clean_string(raw.get("geography")),
            "status": _clean_string(raw.get("status")),
            "category": _clean_string(raw.get("category")),
            "source_url": _clean_string(raw.get("source")),
            "notes": _clean_string(raw.get("notes")),
            "start_ts": self._parse_optional_timestamp(raw.get("start")),
            "end_ts": self._parse_optional_timestamp(raw.get("end")),
            "last_observation_ts": self._parse_optional_timestamp(raw.get("lastUpdated")),
            "asof_date": None,
            "created_at": None,
            "updated_at": None,
            "ingest_ts": now_ts,
            "tags": None,
            "metadata": self._extract_metadata(raw),
            "version": None,
        }
        return record

    def _map_observation_record(
        self,
        raw: Mapping[str, Any],
        series_id: str,
    ) -> Optional[Dict[str, Any]]:
        period_value = _clean_string(raw.get(self.dataset.period_field))
        if not period_value:
            return None
        timestamp = self._parse_period(period_value)
        if timestamp is None:
            return None
        ingest_ts = int(self._now().timestamp() * 1_000_000)
        value_raw = raw.get(self.dataset.value_field)
        value_cast = self._coerce_float(value_raw)
        record = {
            "provider": "EIA",
            "series_id": series_id,
            "ts": timestamp,
            "asof_date": self._timestamp_to_days(timestamp),
            "value": value_cast,
            "value_raw": None if value_cast is not None else _clean_string(value_raw),
            "unit_code": _clean_string(raw.get(self.dataset.unit_field or "unit")) or self.dataset.default_unit,
            "geo_id": _clean_string(raw.get("geography")),
            "dataset_code": self.dataset.dataset_code,
            "frequency_code": self.dataset.frequency_code or self.dataset.default_frequency,
            "status": _clean_string(raw.get("status")),
            "quality_flag": _clean_string(raw.get("quality")),
            "ingest_ts": ingest_ts,
            "source_event_id": f"{series_id}:{period_value}",
            "metadata": self._extract_metadata(raw),
        }
        return record

    def _extract_metadata(self, raw: Mapping[str, Any]) -> Optional[Dict[str, str]]:
        metadata: Dict[str, str] = {}
        for key in self.dataset.metadata_fields:
            value = raw.get(key)
            if value is not None:
                metadata[key] = str(value)
        if not metadata:
            return None
        return metadata

    def _parse_optional_timestamp(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        cleaned = _clean_string(value)
        if not cleaned:
            return None
        dt = self._parse_period(cleaned)
        if dt is None:
            return None
        return dt

    def _parse_period(self, value: str) -> Optional[int]:
        value = value.strip()
        # Direct ISO timestamp support
        try:
            if "T" in value:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            elif len(value) == 4 and value.isdigit():
                dt = datetime(int(value), 1, 1, tzinfo=timezone.utc)
            elif len(value) == 7 and value[4] in {"-", "_"}:
                dt = datetime(int(value[:4]), int(value[5:7]), 1, tzinfo=timezone.utc)
            elif len(value) == 8 and value.isdigit():
                dt = datetime(int(value[0:4]), int(value[4:6]), int(value[6:8]), tzinfo=timezone.utc)
            elif len(value) == 10 and value[4] == value[7] == "-":
                dt = datetime(int(value[0:4]), int(value[5:7]), int(value[8:10]), tzinfo=timezone.utc)
            else:
                dt = datetime.fromisoformat(value)
            return int(dt.timestamp() * 1_000_000)
        except (ValueError, TypeError):
            return None

    def _timestamp_to_days(self, timestamp_micros: int) -> int:
        dt = datetime.fromtimestamp(timestamp_micros / 1_000_000, tz=timezone.utc)
        epoch = date(1970, 1, 1)
        return (dt.date() - epoch).days

    def _coerce_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _default_window_start(self) -> datetime:
        now = self._now()
        if self.dataset.window_days:
            return now - timedelta(days=self.dataset.window_days)
        if self.dataset.window_months:
            return now - timedelta(days=30 * self.dataset.window_months)
        if self.dataset.window_years:
            return now - timedelta(days=365 * self.dataset.window_years)
        return now - timedelta(days=1)

    def _format_period(self, dt: datetime) -> str:
        fmt = self.dataset.default_frequency or "DAILY"
        if fmt.upper().startswith("HOUR"):
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        if fmt.upper() == "MONTHLY":
            return dt.strftime("%Y-%m")
        if fmt.upper() == "ANNUAL":
            return dt.strftime("%Y")
        return dt.strftime("%Y-%m-%d")


__all__ = [
    "EiaApiClient",
    "EiaCollector",
    "EiaDatasetConfig",
    "load_eia_dataset_configs",
]
