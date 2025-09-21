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


class FredApiClient:
    """Thin wrapper for interacting with the FRED API."""

    def __init__(self, collector: ExternalCollector, *, api_key: str) -> None:
        self.collector = collector
        self.api_key = api_key

    def _request(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        query: Dict[str, Any] = {"api_key": self.api_key, "file_type": "json"}
        if params:
            query.update(params)
        response = self.collector.request(HttpRequest(method="GET", path=path, params=query))
        return response.json() or {}

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
