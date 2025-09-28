"""Coordinator that connects Kafka ingestion with the real-time engine."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
import os
from datetime import datetime, timezone
from typing import Any, Mapping, MutableMapping, Optional

from .kafka_processor import KafkaMessage, KafkaProcessor, KafkaProcessorConfig
from .real_time_engine import MarketDataEvent, RealTimeIngestReport, RealTimeMarketDataEngine
from ..telemetry.context import get_request_id
import uuid

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketDataStreamingConfig:
    """Configuration for :class:`MarketDataStreamingService`."""

    curve_topic: str = "market.curves"
    alert_topic: str | None = "market.alerts"
    enable_alert_topic: bool = True
    enable_inference_alerts: bool = True
    publish_reconciliation: bool = True


@dataclass
class MarketDataStreamingMetrics:
    """Aggregated metrics for the streaming service."""

    events_ingested: int = 0
    alerts_emitted: int = 0
    last_ingested_at: datetime | None = None
    last_curve_id: str | None = None
    last_reconciliation_delta: float | None = None


class MarketDataStreamingService:
    """High-level orchestration of Kafka ingestion and real-time processing."""

    def __init__(
        self,
        config: MarketDataStreamingConfig | None = None,
        *,
        kafka_config: KafkaProcessorConfig | None = None,
        kafka_processor: KafkaProcessor | None = None,
        engine: RealTimeMarketDataEngine | None = None,
    ) -> None:
        self.config = config or MarketDataStreamingConfig()
        self.engine = engine or RealTimeMarketDataEngine()
        if kafka_processor is not None:
            self.kafka = kafka_processor
        else:
            self.kafka = KafkaProcessor(kafka_config or KafkaProcessorConfig(in_memory=True))
        self.metrics = MarketDataStreamingMetrics()
        self._started = False

        self.kafka.register_handler(self.config.curve_topic, self._handle_curve_message)

    async def start(self) -> None:
        if self._started:
            return
        await self.kafka.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        await self.kafka.stop()
        self._started = False

    async def publish_event(self, event: MarketDataEvent) -> None:
        """Publish a market data event onto the ingestion topic."""
        headers = self._build_headers(event_type="curve_event", schema="aurum.market.curve_event.v1")
        await self.kafka.publish(self.config.curve_topic, event.to_dict(), key=event.curve_id, headers=headers)

    async def ingest_event(self, event: MarketDataEvent) -> RealTimeIngestReport:
        """Ingest an event directly through the engine (bypassing Kafka)."""
        report = await self.engine.ingest_event(event)
        self._update_metrics(event, report)
        if self.config.enable_alert_topic and self.config.alert_topic:
            # Publish rule-based alerts
            if report.alerts:
                await self._publish_alerts(report)
            # Publish inference-derived alerts (forecast/anomaly)
            if self.config.enable_inference_alerts:
                await self._publish_inference(report)
        return report

    async def _handle_curve_message(self, message: KafkaMessage) -> None:
        try:
            payload = message.value
            event = self._coerce_event(payload)
            report = await self.engine.ingest_event(event)
            self._update_metrics(event, report)
            if self.config.enable_alert_topic and self.config.alert_topic:
                if report.alerts:
                    await self._publish_alerts(report)
                if self.config.enable_inference_alerts:
                    await self._publish_inference(report)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Failed to process market data message: %s", exc)
            raise

    async def _publish_alerts(self, report: RealTimeIngestReport) -> None:
        data = {
            "event": report.event.to_dict(),
            "alerts": [alert.to_dict() for alert in report.alerts],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        headers = self._build_headers(event_type="alert_event", schema="aurum.market.alert_event.v1")
        await self.kafka.publish(self.config.alert_topic, data, key=report.event.curve_id, headers=headers)
        self.metrics.alerts_emitted += len(report.alerts)

    async def _publish_inference(self, report: RealTimeIngestReport) -> None:
        """Publish inference outputs (anomalies, forecast) as structured alerts."""
        inf = report.inference or {}
        curve_id = report.event.curve_id
        ts = datetime.now(timezone.utc).isoformat()

        # Publish anomalies individually for alert streams/consumers
        anomalies = inf.get("anomalies") or []
        for a in anomalies:
            try:
                z = float(a.get("z_score", 0.0))
            except Exception:
                z = 0.0
            sev = self._classify_severity(abs(z))
            message = {
                "type": "inference.anomaly",
                "curve_id": curve_id,
                "generated_at": ts,
                "payload": {
                    "timestamp": a.get("timestamp"),
                    "value": a.get("value"),
                    "z_score": z,
                    "side": a.get("side", "neutral"),
                    "severity": sev,
                },
            }
            headers = self._build_headers(event_type="inference_event", schema="aurum.market.inference_event.v1")
            await self.kafka.publish(self.config.alert_topic, message, key=curve_id, headers=headers)
            self.metrics.alerts_emitted += 1

        # Publish forecast bundle as a single alert message for downstream consumers
        forecast = inf.get("forecast") or []
        if forecast:
            model = None
            for item in forecast:
                if item.get("model"):
                    model = item.get("model")
                    break
            message = {
                "type": "inference.forecast",
                "curve_id": curve_id,
                "generated_at": ts,
                "payload": {
                    "model": model,
                    "horizon": forecast,
                },
            }
            headers = self._build_headers(event_type="inference_event", schema="aurum.market.inference_event.v1")
            await self.kafka.publish(self.config.alert_topic, message, key=curve_id, headers=headers)
            self.metrics.alerts_emitted += 1

    def _update_metrics(self, event: MarketDataEvent, report: RealTimeIngestReport) -> None:
        self.metrics.events_ingested += 1
        self.metrics.last_ingested_at = datetime.now(timezone.utc)
        self.metrics.last_curve_id = event.curve_id
        if report.reconciliation and report.reconciliation.items:
            self.metrics.last_reconciliation_delta = max(
                (abs(item.delta) for item in report.reconciliation.items),
                default=None,
            )

    @staticmethod
    def _classify_severity(z_abs: float) -> str:
        # Standard z-score based severity
        if z_abs >= 4.5:
            return "critical"
        if z_abs >= 3.5:
            return "high"
        if z_abs >= 2.5:
            return "medium"
        if z_abs >= 2.0:
            return "low"
        return "info"

    @staticmethod
    def _coerce_event(payload: Any) -> MarketDataEvent:
        if isinstance(payload, MarketDataEvent):
            return payload
        if isinstance(payload, (bytes, bytearray)):
            try:
                payload = json.loads(payload.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError("Kafka payload is not valid JSON") from exc
        elif isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, Mapping):
            raise ValueError("Kafka payload must be a mapping")

        timestamp = payload.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        if not isinstance(timestamp, datetime):
            timestamp = datetime.now(timezone.utc)
        else:
            timestamp = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)

        metadata = payload.get("metadata") or {}
        if not isinstance(metadata, Mapping):
            metadata = dict(metadata)

        volume = payload.get("volume")
        if volume is not None:
            volume = float(volume)

        return MarketDataEvent(
            curve_id=str(payload["curve_id"]),
            tenor=str(payload["tenor"]),
            price=float(payload["price"]),
            timestamp=timestamp,
            vendor=payload.get("vendor"),
            volume=volume,
            metadata=dict(metadata),
        )

    # --- Header helpers ---------------------------------------------------------
    def _build_headers(self, *, event_type: str, schema: str) -> Mapping[str, Any]:
        """Standard headers for Kafka messages.

        Values are normalized to bytes by the Kafka processor for Kafka producers,
        but we keep them as Python types here.
        """
        trace_id = get_request_id() or uuid.uuid4().hex
        return {
            "content-type": "application/json; charset=utf-8",
            "schema": schema,
            "event-type": event_type,
            "trace-id": trace_id,
            "emitted-at": datetime.now(timezone.utc).isoformat(),
            "producer": "aurum.streaming.service",
        }


# --- Helpers to configure from environment ------------------------------------

def _get_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_streaming_configs_from_env() -> tuple[MarketDataStreamingConfig, KafkaProcessorConfig]:
    """Create streaming config and Kafka config from environment variables.

    Env vars:
    - AURUM_STREAMING_ENABLED
    - AURUM_STREAMING_IN_MEMORY
    - AURUM_STREAMING_CURVE_TOPIC
    - AURUM_STREAMING_ALERT_TOPIC
    - AURUM_STREAMING_KAFKA_BOOTSTRAP_SERVERS
    - AURUM_STREAMING_KAFKA_GROUP_ID
    - AURUM_STREAMING_KAFKA_CLIENT_ID
    - AURUM_STREAMING_KAFKA_SECURITY_PROTOCOL
    - AURUM_STREAMING_KAFKA_SASL_MECHANISM
    - AURUM_STREAMING_KAFKA_SASL_USERNAME
    - AURUM_STREAMING_KAFKA_SASL_PASSWORD
    - AURUM_STREAMING_KAFKA_SSL_CAFILE
    - AURUM_STREAMING_KAFKA_SSL_CERTFILE
    - AURUM_STREAMING_KAFKA_SSL_KEYFILE
    """

    curve_topic = os.getenv("AURUM_STREAMING_CURVE_TOPIC", "market.curves")
    alert_topic = os.getenv("AURUM_STREAMING_ALERT_TOPIC", "market.alerts")

    stream_cfg = MarketDataStreamingConfig(
        curve_topic=curve_topic,
        alert_topic=alert_topic,
        enable_alert_topic=_get_bool(os.getenv("AURUM_STREAMING_ENABLE_ALERT_TOPIC", "true"), True),
        publish_reconciliation=_get_bool(os.getenv("AURUM_STREAMING_PUBLISH_RECONCILIATION", "true"), True),
        enable_inference_alerts=_get_bool(os.getenv("AURUM_STREAMING_ENABLE_INFERENCE_ALERTS", "true"), True),
    )

    in_memory_default = not _get_bool(os.getenv("AURUM_STREAMING_ENABLED"), False)
    bootstrap = os.getenv("AURUM_STREAMING_KAFKA_BOOTSTRAP_SERVERS") or None
    enabled = _get_bool(os.getenv("AURUM_STREAMING_ENABLED"), False)
    in_memory_env = _get_bool(os.getenv("AURUM_STREAMING_IN_MEMORY"), not enabled)
    if not in_memory_env and not bootstrap:
        bootstrap = "localhost:9092"

    kafka_cfg = KafkaProcessorConfig(
        bootstrap_servers=bootstrap,
        group_id=os.getenv("AURUM_STREAMING_KAFKA_GROUP_ID", "aurum-market-stream"),
        input_topics=(curve_topic,),
        in_memory=in_memory_env,
        commit_strategy=os.getenv("AURUM_STREAMING_KAFKA_COMMIT_STRATEGY", "auto"),
        commit_batch_size=int(os.getenv("AURUM_STREAMING_KAFKA_COMMIT_BATCH_SIZE", "100")),
        commit_interval=float(os.getenv("AURUM_STREAMING_KAFKA_COMMIT_INTERVAL", "5.0")),
        use_confluent_producer=(os.getenv("AURUM_STREAMING_KAFKA_USE_CONFLUENT", "0").lower() in ("1", "true", "yes")),
        schema_registry_url=os.getenv("AURUM_STREAMING_SCHEMA_REGISTRY_URL") or None,
        avro_value_subject=os.getenv("AURUM_STREAMING_AVRO_VALUE_SUBJECT") or None,
        use_confluent_consumer=(os.getenv("AURUM_STREAMING_KAFKA_USE_CONFLUENT_CONSUMER", "0").lower() in ("1", "true", "yes")),
        schema_registry_basic_auth=os.getenv("AURUM_STREAMING_SCHEMA_REGISTRY_AUTH") or None,
        client_id=os.getenv("AURUM_STREAMING_KAFKA_CLIENT_ID") or None,
        security_protocol=os.getenv("AURUM_STREAMING_KAFKA_SECURITY_PROTOCOL") or None,
        sasl_mechanism=os.getenv("AURUM_STREAMING_KAFKA_SASL_MECHANISM") or None,
        sasl_plain_username=os.getenv("AURUM_STREAMING_KAFKA_SASL_USERNAME") or None,
        sasl_plain_password=os.getenv("AURUM_STREAMING_KAFKA_SASL_PASSWORD") or None,
        ssl_cafile=os.getenv("AURUM_STREAMING_KAFKA_SSL_CAFILE") or None,
        ssl_certfile=os.getenv("AURUM_STREAMING_KAFKA_SSL_CERTFILE") or None,
        ssl_keyfile=os.getenv("AURUM_STREAMING_KAFKA_SSL_KEYFILE") or None,
    )

    return stream_cfg, kafka_cfg


__all__ = [
    "MarketDataStreamingConfig",
    "MarketDataStreamingMetrics",
    "MarketDataStreamingService",
]
