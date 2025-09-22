"""Generic ISO ingest DAG template (param-driven).

Params:
  - iso: ISO code (e.g., MISO, CAISO, ISONE)
  - dataset: Dataset identifier (lmp, load, genmix, asm)
  - chunker: Chunking policy hint (hour/day) – used by external job/env
  - watermark_table: Watermark table identifier (default ingest_watermark)

This DAG renders and runs the appropriate SeaTunnel template for the provider
and updates the watermark based on the Airflow logical date window.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from aurum.airflow_utils import build_preflight_callable

from aurum.airflow_utils.dag_factory import DagConfig, DagSchedule, dag_factory
from aurum.airflow_utils.iso import IngestSource, make_watermark_callable, register_sources, build_render_command


def _template_for(iso: str, dataset: str) -> str:
    iso_lower = iso.lower()
    ds_lower = dataset.lower()
    # Prefer dedicated templates when available
    candidates = [
        f"seatunnel/jobs/templates/{iso_lower}_{ds_lower}_to_kafka.conf.tmpl",
        f"seatunnel/jobs/templates/{ds_lower}_to_kafka.conf.tmpl",
        f"seatunnel/jobs/templates/iso_{ds_lower}_to_kafka.conf.tmpl",
    ]
    for rel in candidates:
        p = Path("/opt/airflow/") / rel
        if p.exists():
            return rel
    # Fallback to iso_lmp_to_kafka if nothing else
    return "seatunnel/jobs/templates/iso_lmp_to_kafka.conf.tmpl"


config = DagConfig(
    dag_id="iso_ingest_dag",
    description="Parametrized ISO ingest runner",
    schedule=DagSchedule(cron="*/30 * * * *", start_date=datetime(2024, 1, 1), catchup=False),
    tags=("ingest", "iso"),
)


@dag_factory(config)
def _factory() -> None:  # type: ignore[no-redef]
    from airflow.models import Variable

    iso = Variable.get("aurum_iso_code", default_var="MISO")
    dataset = Variable.get("aurum_iso_dataset", default_var="lmp")
    chunker = Variable.get("aurum_iso_chunker", default_var="hour")
    job_name = f"{iso.lower()}_{dataset}_to_kafka"

    # Register the source best-effort so Grafana watermark panels know it
    register_sources(
        [
            IngestSource(
                name=f"iso.{iso.lower()}.{dataset}",
                description=f"{iso} {dataset} ingest",
                schedule=config.schedule.cron,
                target=Variable.get("aurum_iso_topic", default_var=f"aurum.iso.{iso.lower()}.{dataset}.v1"),
            )
        ]
    )

    # Render command for SeaTunnel template
    tmpl_rel = _template_for(iso, dataset)
    default_topic = f"aurum.iso.{iso.lower()}.{dataset}.v1"
    default_subject = f"{default_topic}-value"
    env_assignments = " && ".join(
        [
            f"export ISO_CODE=\"{iso}\"",
            f"export ISO_DATASET=\"{dataset}\"",
            f"export ISO_INTERVAL_START=\"{{{{ data_interval_start.in_timezone('UTC').isoformat() }}}}\"",
            f"export ISO_INTERVAL_END=\"{{{{ data_interval_end.in_timezone('UTC').isoformat() }}}}\"",
            f"export ISO_TOPIC=\"{{{{ var.value.get('aurum_iso_topic', '{default_topic}') }}}}\"",
            f"export ISO_SUBJECT=\"{{{{ var.value.get('aurum_iso_subject', '{default_subject}') }}}}\"",
            # Common infra
            "export AURUM_KAFKA_BOOTSTRAP_SERVERS=\"{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}\"",
            "export AURUM_SCHEMA_REGISTRY_URL=\"{{ var.value.get('aurum_schema_registry', 'http://schema-registry:8081') }}\"",
        ]
    )

    # Dataset-specific env for templates (topic/subject)
    ds_env_lines = []
    if dataset.lower() == "lmp":
        ds_env_lines.extend(
            [
                "export ISO_LMP_TOPIC=\"${ISO_TOPIC}\"",
                "export ISO_LMP_SUBJECT=\"${ISO_SUBJECT}\"",
            ]
        )
    elif dataset.lower() == "load":
        ds_env_lines.extend(
            [
                "export ISO_LOAD_TOPIC=\"${ISO_TOPIC}\"",
                "export ISO_LOAD_SUBJECT=\"${ISO_SUBJECT}\"",
            ]
        )
    elif dataset.lower() == "genmix":
        ds_env_lines.extend(
            [
                "export ISO_GENMIX_TOPIC=\"${ISO_TOPIC}\"",
                "export ISO_GENMIX_SUBJECT=\"${ISO_SUBJECT}\"",
            ]
        )

    # Provider-specific defaults for out-of-the-box runs
    provider_env_lines: list[str] = []
    iso_upper = iso.upper()
    ds_lower = dataset.lower()
    if iso_upper == "MISO" and ds_lower == "lmp":
        provider_env_lines.extend(
            [
                "export MISO_URL=\"{{ var.value.get('aurum_miso_rt_url', 'https://apim.misoenergy.org/pricing/v1/real-time/${YYYY}-${MM}-${DD}/cpnode-lmp') }}\"",
                "export MISO_MARKET=\"{{ var.value.get('aurum_miso_market', 'REAL_TIME') }}\"",
            ]
        )
    elif iso_upper == "CAISO" and ds_lower == "genmix":
        provider_env_lines.extend(
            [
                "export CAISO_GENMIX_ENDPOINT=\"{{ var.value.get('aurum_caiso_genmix_endpoint', 'https://oasis.caiso.com/oasisapi/eir/GenFuelMix?resultformat=6') }}\"",
                "export CAISO_GENMIX_INTERVAL_START=\"{{ data_interval_start.in_timezone('UTC').isoformat() }}\"",
                "export CAISO_GENMIX_INTERVAL_END=\"{{ data_interval_end.in_timezone('UTC').isoformat() }}\"",
                "export CAISO_GENMIX_AUTH_HEADER=\"{{ var.value.get('aurum_caiso_genmix_auth_header', '') }}\"",
            ]
        )
    elif iso_upper == "CAISO" and ds_lower == "load":
        provider_env_lines.extend(
            [
                "export CAISO_LOAD_ENDPOINT=\"{{ var.value.get('aurum_caiso_load_endpoint', 'https://oasis.caiso.com/oasisapi/eir/SystemLoad?resultformat=6') }}\"",
                "export CAISO_LOAD_INTERVAL_START=\"{{ data_interval_start.in_timezone('UTC').isoformat() }}\"",
                "export CAISO_LOAD_INTERVAL_END=\"{{ data_interval_end.in_timezone('UTC').isoformat() }}\"",
                "export CAISO_LOAD_AUTH_HEADER=\"{{ var.value.get('aurum_caiso_load_auth_header', '') }}\"",
            ]
        )

    # Preflight: validate env variables for selected ISO/dataset
    required_vars = [
        "aurum_kafka_bootstrap",
        "aurum_schema_registry",
    ]
    optional_vars: list[str] = []
    warn_only_vars: list[str] = []

    if iso_upper == "MISO" and ds_lower == "lmp":
        warn_only_vars.extend(["aurum_miso_rt_url"])  # defaults provided, warn if missing
    elif iso_upper == "CAISO" and ds_lower == "genmix":
        warn_only_vars.extend(["aurum_caiso_genmix_endpoint"])  # defaults provided
        optional_vars.extend(["aurum_caiso_genmix_auth_header"])  # optional auth
    elif iso_upper == "CAISO" and ds_lower == "load":
        warn_only_vars.extend(["aurum_caiso_load_endpoint"])  # defaults provided
        optional_vars.extend(["aurum_caiso_load_auth_header"])  # optional auth

    preflight = PythonOperator(
        task_id="preflight",
        python_callable=build_preflight_callable(
            required_variables=required_vars,
            optional_variables=optional_vars,
            warn_only_variables=warn_only_vars,
        ),
    )

    render = BashOperator(
        task_id="render_job",
        bash_command=build_render_command(
            job_name=job_name,
            env_assignments=env_assignments,
            bin_path="${SEATUNNEL_HOME}/bin:${PATH}",
            pythonpath_entry="/opt/airflow/src",
            render_only=True,
            pre_lines=(
                f"export AURUM_SEATUNNEL_JOB_TEMPLATE=\"{tmpl_rel}\"",
                *ds_env_lines,
                *provider_env_lines,
            ),
        ),
    )

    run = BashOperator(
        task_id="run_job",
        bash_command=build_render_command(
            job_name=job_name,
            env_assignments=env_assignments,
            bin_path="${SEATUNNEL_HOME}/bin:${PATH}",
            pythonpath_entry="/opt/airflow/src",
            render_only=False,
            pre_lines=(
                f"export AURUM_SEATUNNEL_JOB_TEMPLATE=\"{tmpl_rel}\"",
                *ds_env_lines,
                *provider_env_lines,
            ),
        ),
    )

    watermark = PythonOperator(
        task_id="update_watermark",
        python_callable=make_watermark_callable(
            source_name=f"iso.{iso.lower()}.{dataset}",
            key="data_interval_end",
            policy="hour" if chunker == "hour" else "day",
        ),
        op_kwargs={},
    )

    preflight >> render >> run >> watermark


dag = _factory()
