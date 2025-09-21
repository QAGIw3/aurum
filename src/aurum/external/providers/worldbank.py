"""World Bank collectors built on the external ingestion framework."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from ..collect import (
    Checkpoint,
    CheckpointStore,
    ExternalCollector,
    HttpRequest,
)

DEFAULT_BASE_URL = "https://api.worldbank.org/v2"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "worldbank_ingest_datasets.json"


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class WorldBankDatasetConfig:
    """Static configuration for World Bank indicators."""

    source_name: str
    indicator_id: str
    description: Optional[str] = None
    countries: Sequence[str] = field(default_factory=tuple)
    frequency: str = "ANNUAL"
    default_unit: Optional[str] = None
    per_page: int = 1000
    start_year: Optional[int] = None
    end_year: Optional[int] = None

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "WorldBankDatasetConfig":
        return WorldBankDatasetConfig(
            source_name=str(payload.get("source_name")),
            indicator_id=str(payload.get("indicator_id")),
            description=_clean(payload.get("description")),
            countries=tuple(payload.get("countries", [])),
            frequency=_clean(payload.get("frequency")) or "ANNUAL",
            default_unit=_clean(payload.get("default_unit")),
            per_page=int(payload.get("per_page", 1000)),
            start_year=payload.get("start_year"),
            end_year=payload.get("end_year"),
        )


def load_worldbank_dataset_configs(path: Path | None = None) -> List[WorldBankDatasetConfig]:
    config_path = path or DEFAULT_CONFIG_PATH
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return [WorldBankDatasetConfig.from_dict(item) for item in payload.get("datasets", [])]


class WorldBankApiClient:
    """Minimal World Bank API client built on :class:`ExternalCollector`."""

    def __init__(self, collector: ExternalCollector, *, base_url: str = DEFAULT_BASE_URL) -> None:
        self.collector = collector
        self.base_url = base_url.rstrip("/")

    def get_indicator_metadata(self, indicator_id: str) -> Mapping[str, Any]:
        response = self.collector.request(
            HttpRequest(
                method="GET",
                path=f"{self.base_url}/indicator/{indicator_id}",
                params={"format": "json"},
            )
        )
        payload = response.json()
        if isinstance(payload, list) and len(payload) >= 2:
            data = payload[1] or []
            if data:
                return data[0]
        return {}

    def iter_indicator_observations(
        self,
        indicator_id: str,
        *,
        countries: Sequence[str],
        start_year: Optional[int],
        end_year: Optional[int],
        per_page: int,
    ) -> Iterator[List[Mapping[str, Any]]]:
        country_param = ";".join(countries) if countries else "all"
        page = 1
        while True:
            params: Dict[str, Any] = {
                "format": "json",
                "per_page": per_page,
                "page": page,
            }
            if start_year or end_year:
                start_part = str(start_year) if start_year else ""
                end_part = str(end_year) if end_year else ""
                params["date"] = f"{start_part}:{end_part}".strip(":")
            response = self.collector.request(
                HttpRequest(
                    method="GET",
                    path=f"{self.base_url}/country/{country_param}/indicator/{indicator_id}",
                    params=params,
                )
            )
            payload = response.json()
            if not isinstance(payload, list) or len(payload) < 2:
                break
            meta, data = payload[0], payload[1]
            yield data or []
            total_pages = meta.get("pages") if isinstance(meta, Mapping) else None
            if not total_pages or page >= int(total_pages):
                break
            page += 1


class WorldBankCollector:
    """Collects World Bank indicator metadata and observations."""

    def __init__(
        self,
        dataset: WorldBankDatasetConfig,
        *,
        api_client: WorldBankApiClient,
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
        indicator_meta = self.api_client.get_indicator_metadata(self.dataset.indicator_id)
        title = _clean(indicator_meta.get("name")) or self.dataset.description or self.dataset.indicator_id
        unit = _clean(indicator_meta.get("unit")) or self.dataset.default_unit
        source = _clean(indicator_meta.get("sourceOrganization"))
        topics = indicator_meta.get("topics") or []
        now_ts = int(self._now().timestamp() * 1_000_000)
        records: List[Mapping[str, Any]] = []
        for country in self.dataset.countries:
            series_id = f"{self.dataset.indicator_id}:{country}"
            metadata = {"indicator": self.dataset.indicator_id}
            if source:
                metadata["source"] = source
            if topics:
                topic_names = [topic.get("value") for topic in topics if isinstance(topic, Mapping) and topic.get("value")]
                if topic_names:
                    metadata["topics"] = ",".join(topic_names)
            records.append(
                {
                    "provider": "WorldBank",
                    "series_id": series_id,
                    "dataset_code": self.dataset.indicator_id,
                    "title": title,
                    "description": self.dataset.description or title,
                    "unit_code": unit,
                    "frequency_code": self.dataset.frequency,
                    "geo_id": country,
                    "status": None,
                    "category": self.dataset.indicator_id,
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
                    "metadata": metadata,
                    "version": None,
                }
            )
        if not records:
            return 0
        return self.catalog_collector.emit_records(records)

    def ingest_observations(self) -> int:
        checkpoints: Dict[str, Optional[Checkpoint]] = {}
        max_timestamps: Dict[str, datetime] = {}
        emitted = 0
        target_countries = tuple(self.dataset.countries)
        start_year = self.dataset.start_year
        end_year = self.dataset.end_year
        for page in self.api_client.iter_indicator_observations(
            self.dataset.indicator_id,
            countries=target_countries,
            start_year=start_year,
            end_year=end_year,
            per_page=self.dataset.per_page,
        ):
            per_series_records: Dict[str, List[Mapping[str, Any]]] = {}
            for entry in page:
                iso3 = _clean(entry.get("countryiso3code"))
                if not iso3:
                    country_info = entry.get("country") or {}
                    iso3 = _clean(country_info.get("id"))
                if not iso3:
                    continue
                if target_countries and iso3 not in target_countries:
                    continue
                series_id = f"{self.dataset.indicator_id}:{iso3}"
                if series_id not in checkpoints:
                    checkpoints[series_id] = self.checkpoint_store.get("WorldBank", series_id)
                record = self._map_observation(series_id, iso3, entry)
                if record is None:
                    continue
                ts_dt = datetime.fromtimestamp(record["ts"] / 1_000_000, tz=timezone.utc)
                checkpoint = checkpoints[series_id]
                if checkpoint and checkpoint.last_timestamp and ts_dt <= checkpoint.last_timestamp:
                    continue
                per_series_records.setdefault(series_id, []).append(record)
                current_max = max_timestamps.get(series_id)
                if current_max is None or ts_dt > current_max:
                    max_timestamps[series_id] = ts_dt
            for records in per_series_records.values():
                emitted += self.observation_collector.emit_records(records)
        for series_id, ts in max_timestamps.items():
            self.checkpoint_store.set(
                Checkpoint(
                    provider="WorldBank",
                    series_id=series_id,
                    last_timestamp=ts,
                    metadata={"indicator": self.dataset.indicator_id},
                )
            )
        return emitted

    def _map_observation(self, series_id: str, iso3: str, entry: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        year = _clean(entry.get("date"))
        if not year or not year.isdigit():
            return None
        try:
            dt = datetime(int(year), 12, 31, tzinfo=timezone.utc)
        except ValueError:
            return None
        value = entry.get("value")
        numeric_value = None
        value_raw = None
        if value is not None:
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                value_raw = str(value)
        ingest_ts = int(self._now().timestamp() * 1_000_000)
        metadata = {
            key: _clean(entry.get(key))
            for key in ("obs_status", "decimal")
            if entry.get(key) is not None
        }
        indicator_info = entry.get("indicator") or {}
        if indicator_info:
            code = _clean(indicator_info.get("id"))
            if code:
                metadata["indicator"] = code
        country_info = entry.get("country") or {}
        country_name = _clean(country_info.get("value"))
        if country_name:
            metadata["country_name"] = country_name
        if not metadata:
            metadata = None
        return {
            "provider": "WorldBank",
            "series_id": series_id,
            "ts": int(dt.timestamp() * 1_000_000),
            "asof_date": self._timestamp_to_days(int(dt.timestamp() * 1_000_000)),
            "value": numeric_value,
            "value_raw": value_raw,
            "unit_code": self.dataset.default_unit,
            "geo_id": iso3,
            "dataset_code": self.dataset.indicator_id,
            "frequency_code": self.dataset.frequency,
            "status": metadata.get("obs_status") if metadata else None,
            "quality_flag": None,
            "ingest_ts": ingest_ts,
            "source_event_id": f"{series_id}:{year}",
            "metadata": metadata,
        }

    def _timestamp_to_days(self, timestamp_micros: int) -> int:
        dt = datetime.fromtimestamp(timestamp_micros / 1_000_000, tz=timezone.utc)
        epoch = date(1970, 1, 1)
        return (dt.date() - epoch).days


__all__ = [
    "WorldBankApiClient",
    "WorldBankCollector",
    "WorldBankDatasetConfig",
    "load_worldbank_dataset_configs",
]
