"""Weekly ingestion of U.S. Drought Monitor area fractions."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import os

from airflow.decorators import dag, task, get_current_context

from aurum.api.config import TrinoConfig
from aurum.drought.pipeline import KafkaConfig, process_usdm_snapshot

CATALOG_PATH = Path(__file__).resolve().parents[2] / "config" / "droughtgov_catalog.json"
SCHEMA_FILE = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "aurum.drought.usdm_area.v1.avsc"
DEFAULT_TOPIC = "aurum.drought.usdm_area.v1"

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
}


@dag(
    dag_id="ingest_drought_usdm_weekly",
    schedule="0 12 * * THU",
    start_date=datetime(2024, 1, 4, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["drought", "usdm"],
)
def drought_usdm_weekly():
    @task
    def ingest(execution_date: datetime | None = None) -> Dict[str, Any]:
        logical_date = (execution_date or datetime.now(timezone.utc)).date()
        context = get_current_context()
        run_id = context["dag_run"].run_id if context.get("dag_run") else "manual"
        kafka_cfg = KafkaConfig(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            topic=os.environ.get("AURUM_DROUGHT_USDM_TOPIC", DEFAULT_TOPIC),
            schema_file=SCHEMA_FILE,
        )
        result = process_usdm_snapshot(
            logical_date=logical_date,
            catalog_path=CATALOG_PATH,
            trino_config=TrinoConfig.from_env(),
            kafka_config=kafka_cfg,
            job_id=run_id,
            tenant_id=os.environ.get("AURUM_TENANT_ID", "aurum"),
            schema_version=os.environ.get("AURUM_DROUGHT_SCHEMA_VERSION", "1"),
        )
        return result

    ingest()


drought_usdm_weekly()
