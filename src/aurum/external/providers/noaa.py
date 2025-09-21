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
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "noaa_ingest_datasets.json"


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
    """Enforces maximum requests per second."""

    def __init__(self, rate_per_sec: float = 5.0, *, monotonic: Optional[callable] = None, sleep: Optional[callable] = None) -> None:
        self.rate = rate_per_sec
        self._interval = 1.0 / rate_per_sec
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or time.sleep
        self._next_allowed = self._monotonic()

    def acquire(self) -> None:
        now = self._monotonic()
        if now < self._next_allowed:
            delay = self._next_allowed - now
            self._sleep(delay)
            now = self._monotonic()
        self._next_allowed = max(self._next_allowed + self._interval, now)


class DailyQuota:
    """Simple daily quota tracker."""

    def __init__(self, limit: int, *, today_fn: Optional[callable] = None) -> None:
        self.limit = limit
        self._today_fn = today_fn or (lambda: date.today())
        self._today = self._today_fn()
        self._count = 0

    def consume(self) -> None:
        today = self._today_fn()
        if today != self._today:
            self._today = today
            self._count = 0
        if self.limit and self._count >= self.limit:
            raise RuntimeError("NOAA daily quota exceeded")
        self._count += 1

    @property
    def remaining(self) -> Optional[int]:
        if not self.limit:
            return None
        return max(0, self.limit - self._count)


class NoaaApiClient:
    """Wrapper around :class:`ExternalCollector` with auth, throttling, and quota."""

    def __init__(
        self,
        collector: ExternalCollector,
        *,
        token: str,
        rate_limiter: Optional[NoaaRateLimiter] = None,
        quota: Optional[DailyQuota] = None,
        base_url: str = DEFAULT_BASE_URL,
    ) -> None:
        self.collector = collector
        self.token = token
        self.rate_limiter = rate_limiter or NoaaRateLimiter()
        self.quota = quota
        self.base_url = base_url.rstrip("/")

    def request(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        self.rate_limiter.acquire()
        if self.quota:
            self.quota.consume()
        headers = {"token": self.token}
        response = self.collector.request(
            HttpRequest(
                method="GET",
                path=f"{self.base_url}{path}",
                params=params,
                headers=headers,
            )
        )
        payload = response.json() or {}
        return payload

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
