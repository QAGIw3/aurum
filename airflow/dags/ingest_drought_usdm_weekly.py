"""Weekly ingestion of U.S. Drought Monitor area fractions."""
from __future__ import annotations

import os
import sys

SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if SRC_PATH and SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

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

from aurum.api.config import TrinoConfig
from aurum.dq import enforce_expectation_suite
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

    @task
    def validate() -> None:
        if trino is None or pd is None:
            print("Skipping USDM validation: pandas/trino not installed")
            return
        context = get_current_context()
        logical_dt: datetime = context.get("logical_date") or datetime.now(timezone.utc)
        start_date = (logical_dt - timedelta(days=21)).date()
        config = TrinoConfig.from_env()
        catalog = os.getenv("AURUM_TRINO_CATALOG", "iceberg")
        table = f"{catalog}.environment.usdm_area"
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
              region_type,
              region_id,
              CAST(valid_date AS DATE) AS valid_date,
              CAST(as_of AS BIGINT) AS as_of,
              d0_frac,
              d1_frac,
              d2_frac,
              d3_frac,
              d4_frac,
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
            print("No USDM rows available for GE validation window; skipping.")
            return
        suite_path = Path(__file__).resolve().parents[2] / "ge" / "expectations" / "drought_usdm_area.json"
        enforce_expectation_suite(df, suite_path, suite_name="drought_usdm_area")

    ingest_task = ingest()
    validate_task = validate()
    ingest_task >> validate_task


drought_usdm_weekly()
