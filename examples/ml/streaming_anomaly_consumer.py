"""Example: Stream price ticks and emit anomaly events using KafkaProcessor.

This example uses the in-memory broker by default for simplicity.
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Deque

import pandas as pd

from aurum.streaming.kafka_processor import KafkaProcessor, KafkaProcessorConfig, KafkaMessage
from aurum.ml.anomaly_detection import detect_anomalies


PRICE_TOPIC = "prices"
ANOMALY_TOPIC = "price_anomalies"


async def main() -> None:
    cfg = KafkaProcessorConfig(input_topics=(PRICE_TOPIC,), in_memory=True)
    proc = KafkaProcessor(cfg)

    buffer: Deque[float] = deque(maxlen=240)  # last 240 ticks

    async def handle_price(msg: KafkaMessage) -> None:
        value = float(msg.value["price"]) if isinstance(msg.value, dict) else float(msg.value)
        buffer.append(value)
        if len(buffer) >= 30:  # wait for enough context
            s = pd.Series(list(buffer), index=pd.RangeIndex(start=0, stop=len(buffer)))
            df = detect_anomalies(s, window=24, z_threshold=3.0)
            if not df.empty and int(df.iloc[-1]["index"]) == len(buffer) - 1:
                # Latest point is anomalous → publish event
                await proc.publish(
                    ANOMALY_TOPIC,
                    {
                        "timestamp": msg.timestamp.isoformat(),
                        "value": value,
                        "z": float(df.iloc[-1]["z_score"]),
                        "side": str(df.iloc[-1]["side"]),
                    },
                )

    proc.register_handler(PRICE_TOPIC, handle_price)
    await proc.start()

    # Demo: Publish some prices and a spike
    prices = [50.0 + (i % 24) * 0.1 for i in range(48)] + [100.0]
    for p in prices:
        await proc.publish(PRICE_TOPIC, {"price": p})
        await asyncio.sleep(0.01)

    # Drain any anomaly messages
    # In-memory path: consume directly from internal broker
    if proc._broker is not None:  # type: ignore[attr-defined]
        while proc._broker.qsize(ANOMALY_TOPIC) > 0:  # type: ignore[attr-defined]
            msg = await proc._broker.consume(ANOMALY_TOPIC, timeout=0.1)  # type: ignore[attr-defined]
            if msg:
                print("Anomaly:", msg.value)

    await proc.stop()


if __name__ == "__main__":
    asyncio.run(main())

