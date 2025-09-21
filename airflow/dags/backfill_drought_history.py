"""Ad-hoc backfill DAG for drought datasets."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import os
import sys

SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if SRC_PATH and SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
import tempfile

from airflow.decorators import dag, task
try:
    from airflow.decorators import get_current_context  # type: ignore
except ImportError:  # Airflow <2.4
    from airflow.operators.python import get_current_context  # type: ignore

try:
    from aurum.api.config import TrinoConfig  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - lightweight scheduler fallback
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class TrinoConfig:
        host: str = 'localhost'
        port: int = 8080
        user: str = 'airflow'
        http_scheme: str = 'http'

        @classmethod
        def from_env(cls) -> 'TrinoConfig':
            host = os.getenv('AURUM_API_TRINO_HOST', 'trino')
            port = int(os.getenv('AURUM_API_TRINO_PORT', '8080') or 8080)
            user = os.getenv('AURUM_API_TRINO_USER', 'airflow')
            scheme = os.getenv('AURUM_API_TRINO_SCHEME', 'http')
            return cls(host=host, port=port, user=user, http_scheme=scheme)
from aurum.drought.pipeline import (
    KafkaConfig,
    discover_raster_workload,
    discover_vector_assets,
    process_raster_asset,
    process_usdm_snapshot,
    process_vector_asset,
)

CATALOG_PATH = Path(__file__).resolve().parents[2] / "config" / "droughtgov_catalog.json"
INDEX_SCHEMA = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "aurum.drought.index.v1.avsc"
USDM_SCHEMA = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "aurum.drought.usdm_area.v1.avsc"
VECTOR_SCHEMA = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "aurum.drought.vector_event.v1.avsc"


@dag(
    dag_id="backfill_drought_history",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["drought", "backfill"],
)
def drought_backfill():
    @task
    def expand_dates() -> List[str]:
        context = get_current_context()
        conf = context.get("dag_run").conf if context.get("dag_run") else {}
        start_raw = conf.get("start_date") or conf.get("start")
        end_raw = conf.get("end_date") or conf.get("end") or start_raw
        if not start_raw:
            raise ValueError("start_date is required in DAG run configuration")
        start = datetime.fromisoformat(str(start_raw)).date()
        end = datetime.fromisoformat(str(end_raw)).date()
        if end < start:
            raise ValueError("end_date must be >= start_date")
        day = start
        days: List[str] = []
        while day <= end:
            days.append(day.isoformat())
            day += timedelta(days=1)
        return days

    @task
    def backfill_day(day: str) -> Dict[str, Any]:
        logical_date = datetime.fromisoformat(day).date()
        context = get_current_context()
        conf = context.get("dag_run").conf if context.get("dag_run") else {}
        run_id = context["dag_run"].run_id if context.get("dag_run") else "manual"
        tenant_id = os.environ.get("AURUM_TENANT_ID", "aurum")
        schema_version = os.environ.get("AURUM_DROUGHT_SCHEMA_VERSION", "1")

        # Raster indices
        raster_topic = os.environ.get("AURUM_DROUGHT_INDEX_TOPIC", "aurum.drought.index.v1")
        raster_kafka = KafkaConfig(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            topic=raster_topic,
            schema_file=INDEX_SCHEMA,
        )
        datasets = conf.get("datasets")
        workload = discover_raster_workload(logical_date, CATALOG_PATH, datasets=datasets)
        trino_cfg = TrinoConfig.from_env()
        raster_count = 0
        base_workdir = Path(os.environ.get("AURUM_DROUGHT_WORKDIR", "/tmp/aurum_drought"))
        base_workdir.mkdir(parents=True, exist_ok=True)
        for item in workload:
            job_id = f"{run_id}:{logical_date}"
            with tempfile.TemporaryDirectory(prefix="backfill_raster_", dir=base_workdir) as tmpdir:
                result = process_raster_asset(
                    task=item,
                    catalog_path=CATALOG_PATH,
                    trino_config=trino_cfg,
                    kafka_config=raster_kafka,
                    workdir=Path(tmpdir),
                    job_id=job_id,
                    tenant_id=tenant_id,
                    schema_version=schema_version,
                )
            raster_count += result.get("records", 0)

        summary: Dict[str, Any] = {"raster_records": raster_count}

        if conf.get("include_usdm", True) and logical_date.weekday() == 3:  # Thursday runs
            usdm_topic = os.environ.get("AURUM_DROUGHT_USDM_TOPIC", "aurum.drought.usdm_area.v1")
            usdm_kafka = KafkaConfig(
                bootstrap_servers=raster_kafka.bootstrap_servers,
                schema_registry_url=raster_kafka.schema_registry_url,
                topic=usdm_topic,
                schema_file=USDM_SCHEMA,
            )
            result = process_usdm_snapshot(
                logical_date=logical_date,
                catalog_path=CATALOG_PATH,
                trino_config=trino_cfg,
                kafka_config=usdm_kafka,
                job_id=f"{run_id}:{logical_date}",
                tenant_id=tenant_id,
                schema_version=schema_version,
            )
            summary["usdm_records"] = result.get("records", 0)

        if conf.get("include_vectors", True):
            vector_topic = os.environ.get("AURUM_DROUGHT_VECTOR_TOPIC", "aurum.drought.vector_event.v1")
            vector_kafka = KafkaConfig(
                bootstrap_servers=raster_kafka.bootstrap_servers,
                schema_registry_url=raster_kafka.schema_registry_url,
                topic=vector_topic,
                schema_file=VECTOR_SCHEMA,
            )
            layers = conf.get("layers")
            assets = discover_vector_assets(logical_date, CATALOG_PATH, layers=layers)
            vector_count = 0
            for asset in assets:
                result = process_vector_asset(
                    asset=asset,
                    kafka_config=vector_kafka,
                    job_id=f"{run_id}:{logical_date}",
                    tenant_id=tenant_id,
                    schema_version=schema_version,
                )
                vector_count += result.get("records", 0)
            summary["vector_records"] = vector_count

        return summary

    dates = expand_dates()
    backfill_day.expand(day=dates)


drought_backfill()
