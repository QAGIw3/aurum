"""Kafka handler template: price anomaly detection and signal publishing.

Usage:
  from aurum.streaming.kafka_processor import KafkaProcessor, KafkaProcessorConfig
  from aurum.streaming.handlers.ml_anomaly import register_price_anomaly_detector

  proc = KafkaProcessor(KafkaProcessorConfig(input_topics=("prices",), in_memory=True))
  register_price_anomaly_detector(proc, input_topic="prices", output_topic="price_anomalies", window=24, z_threshold=3.0)
  await proc.start()
"""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque

import pandas as pd

from ..kafka_processor import KafkaProcessor, KafkaMessage
from ...ml.anomaly_detection import detect_anomalies


def register_price_anomaly_detector(
    processor: KafkaProcessor,
    *,
    input_topic: str,
    output_topic: str,
    window: int = 24,
    z_threshold: float = 3.0,
    buffer_size: int = 240,
) -> None:
    """Register an async handler that detects anomalies and publishes signals.

    The handler expects message values to be either numeric or dicts with a
    numeric `price` field.
    """

    buffer: Deque[float] = deque(maxlen=buffer_size)

    async def _handle(msg: KafkaMessage) -> None:
        try:
            value = float(msg.value["price"]) if isinstance(msg.value, dict) else float(msg.value)
        except Exception:
            return  # skip malformed
        buffer.append(value)
        if len(buffer) < max(10, window):
            return
        s = pd.Series(list(buffer), index=pd.RangeIndex(start=0, stop=len(buffer)))
        df = detect_anomalies(s, window=window, z_threshold=z_threshold)
        if not df.empty and int(df.iloc[-1]["index"]) == len(buffer) - 1:
            await processor.publish(
                output_topic,
                {
                    "value": value,
                    "z": float(df.iloc[-1]["z_score"]),
                    "side": str(df.iloc[-1]["side"]),
                    "timestamp": msg.timestamp.isoformat(),
                    "source_topic": msg.topic,
                },
            )

    processor.register_handler(input_topic, _handle)


__all__ = ["register_price_anomaly_detector"]

