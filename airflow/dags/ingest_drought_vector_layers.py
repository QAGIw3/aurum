"""Ingest GeoJSON vector overlays from Drought.gov into Kafka."""
from __future__ import annotations

import os
import sys

SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if SRC_PATH and SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
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

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def validate() -> None:
        if trino is None or pd is None:
            print("Skipping vector validation: pandas/trino not installed")
            return
        context = get_current_context()
        logical_dt: datetime = context.get("logical_date") or datetime.now(timezone.utc)
        start_time = logical_dt - timedelta(days=2)
        config = TrinoConfig.from_env()
        conn = trino.dbapi.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            http_scheme=config.http_scheme,
        )
        catalog = os.getenv("AURUM_TRINO_CATALOG", "iceberg")
        table = f"{catalog}.environment.vector_events"
        query = f"""
            SELECT
              tenant_id,
              schema_version,
              CAST(ingest_ts AS BIGINT) AS ingest_ts,
              ingest_job_id,
              layer,
              event_id,
              region_type,
              region_id,
              CAST(valid_start AS BIGINT) AS valid_start,
              CAST(valid_end AS BIGINT) AS valid_end,
              value,
              unit,
              category,
              severity,
              source_url,
              geometry_wkt,
              properties
            FROM {table}
            WHERE ingest_ts >= CAST({int(start_time.timestamp() * 1_000_000)} AS BIGINT)
            LIMIT 5000
        """
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        if df.empty:
            print("No vector records available for GE validation window; skipping.")
            return
        suite_path = Path(__file__).resolve().parents[2] / "ge" / "expectations" / "drought_vector_event.json"
        enforce_expectation_suite(df, suite_path, suite_name="drought_vector_event")

    assets = discover()
    processed = process.expand(asset=assets)
    validation = validate()
    summarize(processed)
    processed >> validation


drought_vector_layers()
