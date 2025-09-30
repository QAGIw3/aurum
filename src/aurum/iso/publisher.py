"""ISO Kafka publisher utilities with shared Avro codecs.

This module centralises the mapping of ISO data types to Kafka subjects and
provides a lightweight publisher that reuses a single Avro producer per ISO.
It supports both real-time and daily ingestion flows: callers simply
specify the cadence when emitting batches so operational code can surface the
context in logs/metrics if desired.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional

import json
import os

from .base import IsoDataType

try:  # Optional dependency; only required when actually producing to Kafka
    from confluent_kafka.avro import AvroProducer  # type: ignore
except Exception:  # pragma: no cover - import-time guard for environments without Kafka libs
    AvroProducer = None  # type: ignore


@dataclass(frozen=True)
class IsoSubjectInfo:
    """Metadata describing how a single ISO data type is published."""

    topic: str
    subject: str
    schema_file: str


def _default_schema_dir() -> Path:
    env_override = os.getenv("AURUM_SCHEMA_DIR")
    if env_override:
        return Path(env_override)
    return Path(__file__).resolve().parents[2] / "kafka" / "schemas"


# Mapping iso -> data type -> publishing metadata. The schema file is resolved
# relative to ``AURUM_SCHEMA_DIR`` (when set) or the repo's ``kafka/schemas``
# directory. Topics/subjects mirror ``kafka/schemas/subjects.json``.
ISO_SUBJECTS: Mapping[str, Mapping[IsoDataType, IsoSubjectInfo]] = {
    "iso.caiso": {
        IsoDataType.LMP: IsoSubjectInfo(
            topic="aurum.iso.caiso.lmp.v1",
            subject="aurum.iso.caiso.lmp.v1-value",
            schema_file="iso.lmp.v1.avsc",
        ),
        IsoDataType.LOAD: IsoSubjectInfo(
            topic="aurum.iso.caiso.load.v1",
            subject="aurum.iso.caiso.load.v1-value",
            schema_file="iso.load.v1.avsc",
        ),
        IsoDataType.GENERATION_MIX: IsoSubjectInfo(
            topic="aurum.iso.caiso.genmix.v1",
            subject="aurum.iso.caiso.genmix.v1-value",
            schema_file="iso.genmix.v1.avsc",
        ),
        IsoDataType.ANCILLARY_SERVICES: IsoSubjectInfo(
            topic="aurum.iso.caiso.asm.v1",
            subject="aurum.iso.caiso.asm.v1-value",
            schema_file="iso.asm.v1.avsc",
        ),
        IsoDataType.PRICE_NODES: IsoSubjectInfo(
            topic="aurum.iso.caiso.pnode.v1",
            subject="aurum.iso.caiso.pnode.v1-value",
            schema_file="iso.pnode.v1.avsc",
        ),
    },
    "iso.pjm": {
        IsoDataType.LMP: IsoSubjectInfo(
            topic="aurum.iso.pjm.lmp.v1",
            subject="aurum.iso.pjm.lmp.v1-value",
            schema_file="iso.lmp.v1.avsc",
        ),
        IsoDataType.LOAD: IsoSubjectInfo(
            topic="aurum.iso.pjm.load.v1",
            subject="aurum.iso.pjm.load.v1-value",
            schema_file="iso.load.v1.avsc",
        ),
        IsoDataType.GENERATION_MIX: IsoSubjectInfo(
            topic="aurum.iso.pjm.genmix.v1",
            subject="aurum.iso.pjm.genmix.v1-value",
            schema_file="iso.genmix.v1.avsc",
        ),
        IsoDataType.PRICE_NODES: IsoSubjectInfo(
            topic="aurum.iso.pjm.pnode.v1",
            subject="aurum.iso.pjm.pnode.v1-value",
            schema_file="iso.pnode.v1.avsc",
        ),
    },
    "iso.miso": {
        IsoDataType.LMP: IsoSubjectInfo(
            topic="aurum.iso.miso.lmp.v1",
            subject="aurum.iso.miso.lmp.v1-value",
            schema_file="iso.lmp.v1.avsc",
        ),
        IsoDataType.LOAD: IsoSubjectInfo(
            topic="aurum.iso.miso.load.v1",
            subject="aurum.iso.miso.load.v1-value",
            schema_file="iso.load.v1.avsc",
        ),
        IsoDataType.GENERATION_MIX: IsoSubjectInfo(
            topic="aurum.iso.miso.genmix.v1",
            subject="aurum.iso.miso.genmix.v1-value",
            schema_file="iso.genmix.v1.avsc",
        ),
        IsoDataType.ANCILLARY_SERVICES: IsoSubjectInfo(
            topic="aurum.iso.miso.asm.v1",
            subject="aurum.iso.miso.asm.v1-value",
            schema_file="iso.asm.v1.avsc",
        ),
    },
    "iso.isone": {
        IsoDataType.LMP: IsoSubjectInfo(
            topic="aurum.iso.isone.lmp.v1",
            subject="aurum.iso.isone.lmp.v1-value",
            schema_file="iso.lmp.v1.avsc",
        ),
        IsoDataType.LOAD: IsoSubjectInfo(
            topic="aurum.iso.isone.load.v1",
            subject="aurum.iso.isone.load.v1-value",
            schema_file="iso.load.v1.avsc",
        ),
        IsoDataType.GENERATION_MIX: IsoSubjectInfo(
            topic="aurum.iso.isone.genmix.v1",
            subject="aurum.iso.isone.genmix.v1-value",
            schema_file="iso.genmix.v1.avsc",
        ),
        IsoDataType.ANCILLARY_SERVICES: IsoSubjectInfo(
            topic="aurum.iso.isone.asm.v1",
            subject="aurum.iso.isone.asm.v1-value",
            schema_file="iso.asm.v1.avsc",
        ),
    },
    "iso.nyiso": {
        IsoDataType.LMP: IsoSubjectInfo(
            topic="aurum.iso.nyiso.lmp.v1",
            subject="aurum.iso.nyiso.lmp.v1-value",
            schema_file="iso.lmp.v1.avsc",
        ),
    },
    "iso.spp": {
        IsoDataType.LMP: IsoSubjectInfo(
            topic="aurum.iso.spp.lmp.v1",
            subject="aurum.iso.spp.lmp.v1-value",
            schema_file="iso.lmp.v1.avsc",
        ),
        IsoDataType.LOAD: IsoSubjectInfo(
            topic="aurum.iso.spp.load.v1",
            subject="aurum.iso.spp.load.v1-value",
            schema_file="iso.load.v1.avsc",
        ),
        IsoDataType.GENERATION_MIX: IsoSubjectInfo(
            topic="aurum.iso.spp.genmix.v1",
            subject="aurum.iso.spp.genmix.v1-value",
            schema_file="iso.genmix.v1.avsc",
        ),
    },
    "iso.aeso": {
        IsoDataType.LMP: IsoSubjectInfo(
            topic="aurum.iso.aeso.lmp.v1",
            subject="aurum.iso.aeso.lmp.v1-value",
            schema_file="iso.lmp.v1.avsc",
        ),
        IsoDataType.LOAD: IsoSubjectInfo(
            topic="aurum.iso.aeso.load.v1",
            subject="aurum.iso.aeso.load.v1-value",
            schema_file="iso.load.v1.avsc",
        ),
        IsoDataType.GENERATION_MIX: IsoSubjectInfo(
            topic="aurum.iso.aeso.genmix.v1",
            subject="aurum.iso.aeso.genmix.v1-value",
            schema_file="iso.genmix.v1.avsc",
        ),
    },
    "iso.ercot": {
        IsoDataType.LMP: IsoSubjectInfo(
            topic="aurum.iso.ercot.lmp.v1",
            subject="aurum.iso.ercot.lmp.v1-value",
            schema_file="iso.lmp.v1.avsc",
        ),
    },
}


class IsoKafkaPublisher:
    """Publish ISO observations to Kafka using a single Avro producer per ISO.

    The publisher loads Avro schemas for each supported data type once and
    reuses them across both real-time and daily ingestion cadences. The
    underlying producer is instantiated lazily to keep unit tests fast and to
    avoid requiring Kafka libraries unless emission is requested.
    """

    def __init__(
        self,
        iso_code: str,
        *,
        bootstrap_servers: str,
        schema_registry_url: str,
        schema_dir: Optional[Path] = None,
        producer_factory: Optional[Callable[[Dict[str, str]], Any]] = None,
    ) -> None:
        if iso_code not in ISO_SUBJECTS:
            raise ValueError(f"Unsupported ISO '{iso_code}'. Known: {sorted(ISO_SUBJECTS)}")
        self.iso_code = iso_code
        self._subjects = ISO_SUBJECTS[iso_code]
        self._schema_dir = schema_dir or _default_schema_dir()
        self._producer_factory = producer_factory or self._default_producer_factory
        self._producer: Optional[Any] = None
        self._bootstrap_servers = bootstrap_servers
        self._schema_registry_url = schema_registry_url
        self._schemas: MutableMapping[IsoDataType, Mapping[str, Any]] = {}

        for data_type, info in self._subjects.items():
            schema_path = self._schema_dir / info.schema_file
            if not schema_path.exists():
                raise FileNotFoundError(f"Schema file not found for {iso_code}:{data_type.value}: {schema_path}")
            self._schemas[data_type] = json.loads(schema_path.read_text(encoding="utf-8"))

    def supported_data_types(self) -> List[IsoDataType]:
        """Return the ISO data types that have a configured Kafka subject."""

        return list(self._subjects.keys())

    def emit(
        self,
        data_type: IsoDataType,
        records: Iterable[Mapping[str, Any]],
        *,
        cadence: str = "realtime",
        flush: bool = True,
    ) -> int:
        """Emit observation records for a given ISO data type.

        Args:
            data_type: ISO data type to emit (LMP, load, etc.).
            records: Iterable of Avro-compatible dictionaries.
            cadence: Logical cadence of the load (``"realtime"`` or ``"daily"``)
                used for logging/metrics by callers. The publisher itself
                treats both cadences identically but records the parameter in
                the returned metadata for observability.
            flush: Whether to flush the underlying producer once the batch has
                been written.

        Returns:
            Number of records emitted.
        """

        if data_type not in self._subjects:
            raise ValueError(
                f"ISO {self.iso_code} does not configure Kafka subjects for {data_type.value}"
            )

        materialized = list(records)
        if not materialized:
            return 0

        producer = self._ensure_producer()
        info = self._subjects[data_type]
        schema = self._schemas[data_type]

        for record in materialized:
            producer.produce(topic=info.topic, value=record, value_schema=schema)

        if flush:
            producer.flush()

        return len(materialized)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _ensure_producer(self) -> Any:
        if self._producer is not None:
            return self._producer

        config = {
            "bootstrap.servers": self._bootstrap_servers,
            "schema.registry.url": self._schema_registry_url,
            # Client ID conveys ISO for easier tracing in Kafka metrics
            "client.id": f"aurum-{self.iso_code}-publisher",
        }
        self._producer = self._producer_factory(config)
        return self._producer

    @staticmethod
    def _default_producer_factory(config: Dict[str, str]) -> Any:
        if AvroProducer is None:  # pragma: no cover - handled in environments without Kafka
            raise RuntimeError(
                "confluent-kafka[avro] is required to publish ISO data but is not installed"
            )
        return AvroProducer(config)


__all__ = ["IsoKafkaPublisher", "IsoSubjectInfo", "ISO_SUBJECTS"]
