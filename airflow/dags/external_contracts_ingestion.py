"""Airflow DAG orchestrating canonical external contract ingestion."""
from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

try:  # Dataset support is optional depending on Airflow version
    from airflow.datasets import Dataset  # type: ignore
except Exception:  # pragma: no cover - airflow without dataset feature
    Dataset = None  # type: ignore

from aurum.airflow_utils.datasets import dataset_uri
from aurum.external_contracts import ExternalContractsPublisher, TrinoExternalContractsConsumer
from aurum.observability.metrics import (
    EXTERNAL_DATA_FRESHNESS,
    record_external_contract_merge,
    record_external_contract_publish,
)

try:  # pragma: no cover - optional dependency for DAG parsing
    from aurum.db.watermark import update_ingest_watermark  # type: ignore
except Exception:  # pragma: no cover - database layer not available during import
    update_ingest_watermark = None  # type: ignore

UTC = timezone.utc
PROVIDERS: tuple[str, ...] = ("eia", "fred", "noaa", "worldbank")
logger = logging.getLogger(__name__)

PROVIDER_DATASETS: Dict[str, Dict[str, str]] = {
    provider: {
        "trigger": dataset_uri("triggers", "external", provider, "incremental_ready"),
        "kafka": dataset_uri("ingest", "external", provider, "kafka"),
        "catalog": dataset_uri("warehouse", "external", provider, "series_catalog"),
        "observations": dataset_uri("warehouse", "external", provider, "timeseries_observation"),
    }
    for provider in PROVIDERS
}

SCHEDULE = (
    [Dataset(metadata["trigger"]) for metadata in PROVIDER_DATASETS.values()] if Dataset is not None else "0 * * * *"
)

DEFAULT_ARGS = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}


def _build_consumer() -> TrinoExternalContractsConsumer:
    return TrinoExternalContractsConsumer(
        host=os.getenv("AURUM_TRINO_HOST"),
        port=int(os.getenv("AURUM_TRINO_PORT", "8080")),
        user=os.getenv("AURUM_TRINO_USER", "aurum-airflow"),
        password=os.getenv("AURUM_TRINO_PASSWORD"),
        catalog=os.getenv("AURUM_TRINO_CATALOG", "iceberg"),
        schema=os.getenv("AURUM_TRINO_SCHEMA", "external"),
        staging_schema=os.getenv("AURUM_TRINO_STAGING_SCHEMA", "external_stage"),
    )


def _publish_provider(provider: str, *, catalog: bool = True, observations: bool = True, **context: Any) -> str:
    publisher = ExternalContractsPublisher()
    result = publisher.publish_provider_sync(provider, catalog=catalog, observations=observations)
    record_external_contract_publish(provider, result.status)
    return result.status


def _catalog_stage_override(provider: str) -> Optional[str]:
    return os.getenv(f"AURUM_EXT_{provider.upper()}_CATALOG_STAGE")


def _obs_stage_override(provider: str) -> Optional[str]:
    return os.getenv(f"AURUM_EXT_{provider.upper()}_OBS_STAGE")


def _merge_catalog(provider: str, **_: Any) -> int:
    consumer = _build_consumer()
    summary = consumer.merge_catalog(provider, staging_table=_catalog_stage_override(provider))
    record_external_contract_merge(provider, "series_catalog", summary.records_available)
    return summary.records_available


def _merge_observations(provider: str, **_: Any) -> int:
    consumer = _build_consumer()
    summary = consumer.merge_observations(provider, staging_table=_obs_stage_override(provider))
    record_external_contract_merge(provider, "timeseries_observation", summary.records_available)
    return summary.records_available


def _update_watermark(provider: str, **context: Any) -> str:
    logical_date = context.get("logical_date") or context.get("execution_date")
    if not isinstance(logical_date, datetime):
        raise RuntimeError("logical_date not available for watermark update")

    ts = logical_date.astimezone(UTC)

    if update_ingest_watermark is not None:
        try:
            update_ingest_watermark(f"external.contracts.{provider}", "logical_date", ts, policy="exact")
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning("Failed to persist watermark", extra={"provider": provider, "error": str(exc)})

    try:
        if EXTERNAL_DATA_FRESHNESS is not None:
            freshness = max(0.0, (datetime.now(tz=UTC) - ts).total_seconds() / 3600.0)
            EXTERNAL_DATA_FRESHNESS.labels(provider=provider, dataset="timeseries_observation").set(freshness)
    except Exception:  # pragma: no cover - metrics optional
        pass

    return ts.isoformat()


dag = DAG(
    dag_id="external_contracts_ingestion",
    description="Ingest external provider contracts into canonical Iceberg tables",
    default_args=DEFAULT_ARGS,
    schedule=SCHEDULE,
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["external", "contracts", "iceberg"],
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

for provider in PROVIDERS:
    publish = PythonOperator(
        task_id=f"publish_{provider}",
        python_callable=_publish_provider,
        op_kwargs={"provider": provider},
        dag=dag,
    )

    merge_catalog = PythonOperator(
        task_id=f"merge_catalog_{provider}",
        python_callable=_merge_catalog,
        op_kwargs={"provider": provider},
        dag=dag,
    )

    merge_obs = PythonOperator(
        task_id=f"merge_observations_{provider}",
        python_callable=_merge_observations,
        op_kwargs={"provider": provider},
        dag=dag,
    )

    watermark = PythonOperator(
        task_id=f"update_watermark_{provider}",
        python_callable=_update_watermark,
        op_kwargs={"provider": provider},
        dag=dag,
    )

    if Dataset is not None:
        datasets = PROVIDER_DATASETS[provider]
        publish.inlets = [Dataset(datasets["trigger"])]  # type: ignore[attr-defined]
        publish.outlets = [Dataset(datasets["kafka"])]  # type: ignore[attr-defined]
        merge_catalog.inlets = [Dataset(datasets["kafka"])]  # type: ignore[attr-defined]
        merge_catalog.outlets = [Dataset(datasets["catalog"])]  # type: ignore[attr-defined]
        merge_obs.inlets = [Dataset(datasets["kafka"])]  # type: ignore[attr-defined]
        merge_obs.outlets = [Dataset(datasets["observations"])]  # type: ignore[attr-defined]
        watermark.inlets = [Dataset(datasets["catalog"]), Dataset(datasets["observations"])]  # type: ignore[attr-defined]
        watermark.outlets = [Dataset(datasets["observations"])]  # type: ignore[attr-defined]

    start >> publish >> [merge_catalog, merge_obs]
    [merge_catalog, merge_obs] >> watermark >> end
