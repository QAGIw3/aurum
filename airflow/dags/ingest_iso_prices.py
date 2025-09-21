"""TaskFlow DAG for CAISO and ERCOT ISO price ingestion."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.utils.context import get_current_context

from aurum.airflow_utils import (
    DagConfig,
    DagSchedule,
    build_default_args,
    build_failure_callback,
    build_preflight_callable,
    dag_factory,
)
from aurum.airflow_utils.iso import IngestSource, register_sources, update_watermark
from scripts.ingest.caiso_prc_lmp_to_kafka import (
    CaisoIngestConfig,
    run_ingest as run_caiso_ingest,
)
from scripts.ingest.ercot_mis_to_kafka import (
    ErcotIngestConfig,
    run_ingest as run_ercot_ingest,
)


@dataclass(frozen=True)
class WatermarkConfig:
    """Configuration describing how a watermark should be updated."""

    source_name: str
    key: str = "logical_date"


DEFAULT_ARGS = build_default_args(
    retries=3,
    retry_delay=timedelta(minutes=10),
    execution_timeout=timedelta(minutes=45),
)

SOURCES = (
    IngestSource(
        "caiso_helper",
        description="CAISO PRC_LMP helper ingestion",
        schedule="30 * * * *",
        target="kafka",
    ),
    IngestSource(
        "ercot_helper",
        description="ERCOT MIS helper ingestion",
        schedule="30 * * * *",
        target="kafka",
    ),
)

PRE_FLIGHT = build_preflight_callable(
    required_variables=("aurum_kafka_bootstrap", "aurum_schema_registry"),
    optional_variables=(
        "aurum_caiso_market",
        "aurum_ercot_mis_url",
    ),
    warn_only_variables=("aurum_ercot_bearer_token",),
)

DAG_CONFIG = DagConfig(
    dag_id="ingest_iso_prices",
    description="Ingest CAISO and ERCOT ISO prices using Python helpers",
    schedule=DagSchedule(
        cron="30 * * * *",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
    ),
    tags=("aurum", "caiso", "ercot", "iso"),
    default_args=DEFAULT_ARGS,
)


def _resolve_boolean(value: str | None) -> bool:
    if value is None:
        return False
    return value.lower() not in {"", "0", "false", "no"}


@dag_factory(DAG_CONFIG)
def ingest_iso_prices():
    @task(task_id="start")
    def start() -> None:  # pragma: no cover - trivial
        return None

    @task(task_id="preflight_airflow_vars")
    def preflight() -> None:
        PRE_FLIGHT()

    @task(task_id="register_sources")
    def register_ingest_sources() -> None:
        register_sources(SOURCES)

    @task(task_id="caiso_prc_lmp_helper", pool="api_caiso", execution_timeout=timedelta(minutes=20))
    def run_caiso_helper() -> int:
        context = get_current_context()
        start_dt = context["data_interval_start"].astimezone(timezone.utc).isoformat()
        end_dt = context["data_interval_end"].astimezone(timezone.utc).isoformat()

        bootstrap = Variable.get("aurum_kafka_bootstrap")
        schema_registry = Variable.get("aurum_schema_registry")
        market = Variable.get("aurum_caiso_market", default_var="RTPD")
        node = Variable.get("aurum_caiso_node", default_var=None)
        dlq_topic = Variable.get("aurum_dlq_topic", default_var=None)
        dlq_subject = Variable.get("aurum_dlq_subject", default_var="aurum.ingest.error.v1")
        debug_flag = _resolve_boolean(Variable.get("aurum_ingest_debug", default_var="0"))
        topic = Variable.get("aurum_caiso_topic", default_var="aurum.iso.caiso.lmp.v1")

        config = CaisoIngestConfig(
            start=start_dt,
            end=end_dt,
            market=market,
            topic=topic,
            schema_registry=schema_registry,
            bootstrap_servers=bootstrap,
            node=node or None,
            dlq_topic=dlq_topic or None,
            dlq_subject=dlq_subject,
            debug=debug_flag,
        )
        return run_caiso_ingest(config)

    @task(task_id="ercot_mis_helper", pool="api_ercot", execution_timeout=timedelta(minutes=20))
    def run_ercot_helper() -> int:
        context = get_current_context()
        _ = context["data_interval_start"]  # capture for dependency, unused value
        url = Variable.get("aurum_ercot_mis_url", default_var="")
        if not url:
            raise AirflowSkipException("aurum_ercot_mis_url not configured")

        bearer = Variable.get("aurum_ercot_bearer_token", default_var=None)
        bootstrap = Variable.get("aurum_kafka_bootstrap")
        schema_registry = Variable.get("aurum_schema_registry")
        topic = Variable.get("aurum_ercot_topic", default_var="aurum.iso.ercot.lmp.v1")
        dlq_topic = Variable.get("aurum_dlq_topic", default_var=None)
        dlq_subject = Variable.get("aurum_dlq_subject", default_var="aurum.ingest.error.v1")

        config = ErcotIngestConfig(
            url=url,
            topic=topic,
            schema_registry=schema_registry,
            bootstrap_servers=bootstrap,
            subject=None,
            bearer_token=bearer or None,
            dlq_topic=dlq_topic or None,
            dlq_subject=dlq_subject,
        )
        return run_ercot_ingest(config)

    @task(task_id="caiso_watermark")
    def caiso_watermark(_: int, config: WatermarkConfig) -> None:
        context = get_current_context()
        value = context.get(config.key)
        if not isinstance(value, datetime):  # pragma: no cover - defensive
            raise RuntimeError(f"Context missing datetime '{config.key}' for {config.source_name}")
        update_watermark(config.source_name, value, key=config.key)

    @task(task_id="ercot_watermark")
    def ercot_watermark(_: int, config: WatermarkConfig) -> None:
        context = get_current_context()
        value = context.get(config.key)
        if not isinstance(value, datetime):  # pragma: no cover - defensive
            raise RuntimeError(f"Context missing datetime '{config.key}' for {config.source_name}")
        update_watermark(config.source_name, value, key=config.key)

    @task(task_id="end")
    def end() -> None:  # pragma: no cover - trivial
        return None

    start_task = start()
    preflight_task = preflight()
    register_task = register_ingest_sources()

    start_task >> preflight_task >> register_task

    caiso_result = run_caiso_helper()
    ercot_result = run_ercot_helper()

    register_task >> caiso_result
    register_task >> ercot_result

    caiso_done = caiso_watermark(caiso_result, WatermarkConfig("caiso_helper"))
    ercot_done = ercot_watermark(ercot_result, WatermarkConfig("ercot_helper"))

    end_task = end()
    caiso_done >> end_task
    ercot_done >> end_task


dag = ingest_iso_prices()
dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices")
