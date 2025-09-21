"""Daily ingestion of Drought.gov raster indices into Kafka."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import os
import tempfile

from airflow.decorators import dag, task, get_current_context
from airflow.utils.trigger_rule import TriggerRule

from aurum.api.config import TrinoConfig
from aurum.drought.pipeline import KafkaConfig, discover_raster_workload, process_raster_asset

CATALOG_PATH = Path(__file__).resolve().parents[2] / "config" / "droughtgov_catalog.json"
SCHEMA_FILE = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "aurum.drought.index.v1.avsc"
DEFAULT_TOPIC = "aurum.drought.index.v1"

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
}


@dag(
    dag_id="ingest_drought_raster_indices",
    schedule="0 9 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["drought", "kafka"],
)
def drought_raster_indices():
    @task
    def discover(execution_date: datetime | None = None) -> List[Dict[str, Any]]:
        logical = (execution_date or datetime.now(timezone.utc)).date()
        workload = discover_raster_workload(logical, CATALOG_PATH)
        return workload

    @task(map_index_template="%d")
    def process(task_item: Dict[str, Any]) -> Dict[str, Any]:
        context = get_current_context()
        run_id = context["dag_run"].run_id if context.get("dag_run") else "manual"
        kafka_cfg = KafkaConfig(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            topic=os.environ.get("AURUM_DROUGHT_INDEX_TOPIC", DEFAULT_TOPIC),
            schema_file=SCHEMA_FILE,
        )
        base_workdir = Path(os.environ.get("AURUM_DROUGHT_WORKDIR", "/tmp/aurum_drought"))
        base_workdir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="raster_", dir=base_workdir) as tmpdir:
            result = process_raster_asset(
                task=task_item,
                catalog_path=CATALOG_PATH,
                trino_config=TrinoConfig.from_env(),
                kafka_config=kafka_cfg,
                workdir=Path(tmpdir),
                job_id=run_id,
                tenant_id=os.environ.get("AURUM_TENANT_ID", "aurum"),
                schema_version=os.environ.get("AURUM_DROUGHT_SCHEMA_VERSION", "1"),
            )
        return result

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(results: List[Dict[str, Any]]) -> None:
        total_records = sum(item.get("records", 0) for item in results if isinstance(item, dict))
        total_bytes = sum(item.get("bytes_downloaded", 0) for item in results if isinstance(item, dict))
        print(f"drought raster ingestion complete – records={total_records} bytes={total_bytes}")

    items = discover()
    processed = process.expand(task_item=items)
    summarize(processed)


drought_raster_indices()
