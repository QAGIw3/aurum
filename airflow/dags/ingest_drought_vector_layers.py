"""Ingest GeoJSON vector overlays from Drought.gov into Kafka."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import os

from airflow.decorators import dag, task, get_current_context
from airflow.utils.trigger_rule import TriggerRule

from aurum.drought.pipeline import (
    KafkaConfig,
    discover_vector_assets,
    process_vector_asset,
)

CATALOG_PATH = Path(__file__).resolve().parents[2] / "config" / "droughtgov_catalog.json"
SCHEMA_FILE = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "aurum.drought.vector_event.v1.avsc"
DEFAULT_TOPIC = "aurum.drought.vector_event.v1"

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
}


@dag(
    dag_id="ingest_drought_vector_layers",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["drought", "vector"],
)
def drought_vector_layers():
    @task
    def discover(execution_date: datetime | None = None) -> List[Dict[str, Any]]:
        logical = (execution_date or datetime.now(timezone.utc)).date()
        return discover_vector_assets(logical, CATALOG_PATH)

    @task(map_index_template="%d")
    def process(asset: Dict[str, Any]) -> Dict[str, Any]:
        context = get_current_context()
        run_id = context["dag_run"].run_id if context.get("dag_run") else "manual"
        kafka_cfg = KafkaConfig(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            topic=os.environ.get("AURUM_DROUGHT_VECTOR_TOPIC", DEFAULT_TOPIC),
            schema_file=SCHEMA_FILE,
        )
        return process_vector_asset(
            asset=asset,
            kafka_config=kafka_cfg,
            job_id=run_id,
            tenant_id=os.environ.get("AURUM_TENANT_ID", "aurum"),
            schema_version=os.environ.get("AURUM_DROUGHT_SCHEMA_VERSION", "1"),
        )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(results: List[Dict[str, Any]]) -> None:
        total_records = sum(item.get("records", 0) for item in results if isinstance(item, dict))
        print(f"vector ingestion complete – records={total_records}")

    assets = discover()
    processed = process.expand(asset=assets)
    summarize(processed)


drought_vector_layers()
