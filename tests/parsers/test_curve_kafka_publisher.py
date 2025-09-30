from __future__ import annotations

from datetime import datetime

import pandas as pd

from pathlib import Path

from aurum.parsers.curve_kafka_publisher import CurveKafkaPublisher


class _StubProducer:
    def __init__(self) -> None:
        self.records = []
        self.flush_calls = 0

    def produce(self, *, topic: str, value: dict) -> None:
        self.records.append((topic, value))

    def flush(self) -> None:
        self.flush_calls += 1


def test_curve_kafka_publisher_normalizes_dataframe(tmp_path) -> None:
    producer_holder = {}

    def factory(config, schemas):  # noqa: ANN001
        stub = _StubProducer()
        producer_holder["producer"] = stub
        return stub

    publisher = CurveKafkaPublisher(
        producer_factory=factory,
        schema_path=Path("kafka/schemas/curve.observation.v1.avsc"),
        bootstrap_servers="kafka:9092",
        schema_registry_url="http://schema:8081",
    )

    df = pd.DataFrame(
        {
            "asof_date": [pd.Timestamp("2025-01-01")],
            "source_file": ["demo.xlsx"],
            "sheet_name": ["Fixed Prices - Mid"],
            "asset_class": ["power"],
            "region": ["US"],
            "iso": ["PJM"],
            "location": ["AECO"],
            "market": ["DA"],
            "product": ["power"],
            "block": ["ON_PEAK"],
            "spark_location": [None],
            "price_type": ["MID"],
            "units_raw": ["USD/MWh"],
            "currency": ["USD"],
            "per_unit": ["MWh"],
            "tenor_type": ["MONTHLY"],
            "contract_month": [pd.Timestamp("2025-02-01")],
            "tenor_label": ["2025-02"],
            "value": [None],
            "bid": [42.0],
            "ask": [43.0],
            "mid": [42.5],
            "curve_key": ["ck"],
            "version_hash": ["hash"],
            "_ingest_ts": [datetime.utcnow()],
        }
    )

    published = publisher.publish_dataframe(df)

    assert published == 1
    stub = producer_holder["producer"]
    topic, payload = stub.records[0]
    assert topic == "aurum.curve.observation.v1"
    assert payload["asof_date"] == 19723
    assert payload["contract_month"] == 19754
    assert payload["bid"] == 42.0
    assert stub.flush_calls == 1
