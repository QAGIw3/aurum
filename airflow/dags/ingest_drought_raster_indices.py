"""Daily ingestion of Drought.gov raster indices into Kafka."""
from __future__ import annotations

import os
import sys

SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if SRC_PATH and SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

try:
    import pandas as pd  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency in scheduler image
    pd = None  # type: ignore
try:
    import trino  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency in scheduler image
    trino = None  # type: ignore
from airflow.decorators import dag, task
try:
    from airflow.decorators import get_current_context  # type: ignore
except ImportError:  # Airflow <2.4
    from airflow.operators.python import get_current_context  # type: ignore
from airflow.utils.trigger_rule import TriggerRule

from aurum.api.config import TrinoConfig
from aurum.dq import enforce_expectation_suite
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

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def validate() -> None:
        if trino is None or pd is None:
            print("Skipping drought index validation: pandas/trino not installed")
            return
        context = get_current_context()
        logical_dt: datetime = context.get("logical_date") or datetime.now(timezone.utc)
        start_date = (logical_dt - timedelta(days=7)).date()
        config = TrinoConfig.from_env()
        catalog = os.getenv("AURUM_TRINO_CATALOG", "iceberg")
        table = f"{catalog}.environment.drought_index"

        conn = trino.dbapi.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            http_scheme=config.http_scheme,
        )
        query = f"""
            SELECT
              tenant_id,
              schema_version,
              CAST(ingest_ts AS BIGINT) AS ingest_ts,
              ingest_job_id,
              series_id,
              region_type,
              region_id,
              dataset,
              "index",
              timescale,
              CAST(valid_date AS DATE) AS valid_date,
              CAST(as_of AS BIGINT) AS as_of,
              value,
              unit,
              poc,
              source_url,
              metadata
            FROM {table}
            WHERE valid_date >= DATE '{start_date}'
            LIMIT 5000
        """
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        if df.empty:
            print("No drought index rows available for GE validation window; skipping.")
            return
        suite_path = Path(__file__).resolve().parents[2] / "ge" / "expectations" / "drought_index.json"
        enforce_expectation_suite(df, suite_path, suite_name="drought_index")

    items = discover()
    processed = process.expand(task_item=items)
    validation = validate()
    summarize(processed)
    processed >> validation


drought_raster_indices()
