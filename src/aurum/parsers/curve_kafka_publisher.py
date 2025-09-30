"""Kafka publisher for canonical curve observations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, MutableMapping, Optional

import json
import os

import pandas as pd


def _default_schema_dir() -> Path:
    override = os.getenv("AURUM_SCHEMA_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[3] / "kafka" / "schemas"


def _date_to_days(value: Any) -> Optional[int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        value = value.date()
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        epoch = date(1970, 1, 1)
        return (value - epoch).days
    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        return _date_to_days(parsed.to_pydatetime())
    except Exception:
        return None


def _timestamp_to_micros(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.utcnow().astimezone().tzinfo)
        return int(value.timestamp() * 1_000_000)
    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        return _timestamp_to_micros(parsed.to_pydatetime())
    except Exception:
        return None


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


@dataclass
class CurveKafkaPublisher:
    """Publish canonical curve observations to Kafka with Avro encoding."""

    topic: str = "aurum.curve.observation.v1"
    subject: str = "aurum.curve.observation.v1-value"
    schema_path: Optional[Path] = None
    bootstrap_servers: Optional[str] = None
    schema_registry_url: Optional[str] = None
    producer_factory: Optional[Callable[[Mapping[str, Any], Mapping[str, Any]], Any]] = None

    def __post_init__(self) -> None:
        schema_dir = self.schema_path.parent if self.schema_path else _default_schema_dir()
        schema_file = self.schema_path if self.schema_path else schema_dir / "curve.observation.v1.avsc"
        if not schema_file.exists():
            raise FileNotFoundError(f"Curve observation schema not found: {schema_file}")
        self._schema_dict: MutableMapping[str, Any] = json.loads(schema_file.read_text(encoding="utf-8"))
        self._bootstrap = self.bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self._schema_registry = self.schema_registry_url or os.getenv("SCHEMA_REGISTRY_URL")
        if not self._bootstrap or not self._schema_registry:
            raise RuntimeError(
                "KAFKA_BOOTSTRAP_SERVERS and SCHEMA_REGISTRY_URL must be configured for curve publishing"
            )
        self._producer_factory = self.producer_factory or self._build_producer
        self._producer: Optional[Any] = None

    def publish_dataframe(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        producer = self._ensure_producer()
        records = self._normalise_dataframe(df)
        for record in records:
            producer.produce(topic=self.topic, value=record)
        producer.flush()
        return len(records)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _ensure_producer(self) -> Any:
        if self._producer is None:
            self._producer = self._producer_factory(
                {
                    "bootstrap.servers": self._bootstrap,
                    "schema.registry.url": self._schema_registry,
                },
                {"value_schema": self._schema_dict},
            )
        return self._producer

    @staticmethod
    def _build_producer(config: Mapping[str, Any], schemas: Mapping[str, Any]) -> Any:
        try:
            from confluent_kafka.avro import AvroProducer  # type: ignore
        except Exception as exc:  # pragma: no cover - import guard
            raise RuntimeError("confluent-kafka[avro] is required for curve publishing") from exc
        return AvroProducer(dict(config), default_value_schema=schemas["value_schema"])

    def _normalise_dataframe(self, df: pd.DataFrame) -> Iterable[Mapping[str, Any]]:
        working = df.copy()
        for column in working.columns:
            if pd.api.types.is_datetime64_any_dtype(working[column]):
                working[column] = working[column].dt.tz_localize(None)
        records = []
        for raw in working.to_dict(orient="records"):
            record: Dict[str, Any] = {}
            for field in self._schema_dict["fields"]:
                name = field["name"]
                value = raw.get(name)
                if name in {"asof_date", "contract_month"}:
                    record[name] = _date_to_days(value)
                    continue
                if name == "_ingest_ts":
                    record[name] = _timestamp_to_micros(value) or _timestamp_to_micros(datetime.utcnow())
                    continue
                cleaned = _clean_value(value)
                record[name] = cleaned
            records.append(record)
        return records


__all__ = ["CurveKafkaPublisher"]
