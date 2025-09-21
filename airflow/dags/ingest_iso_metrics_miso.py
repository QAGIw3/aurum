"""Airflow DAG to ingest MISO load, generation mix, and RT LMP data."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Tuple

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from aurum.airflow_utils import build_failure_callback, build_preflight_callable


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
}

BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")


def _register_sources() -> None:
    try:
        import sys

        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        register_ingest_source(
            "miso_load",
            description="MISO load ingestion",
            schedule="45 * * * *",
            target="kafka",
        )
        register_ingest_source(
            "miso_genmix",
            description="MISO generation mix ingestion",
            schedule="45 * * * *",
            target="kafka",
        )
        register_ingest_source(
            "miso_lmp",
            description="MISO real-time LMP ingestion",
            schedule="*/5 * * * *",
            target="kafka",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register MISO ingest sources: {exc}")


def _update_watermark(source_name: str, logical_date: datetime) -> None:
    watermark = logical_date.astimezone(timezone.utc)
    try:
        import sys

        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import update_ingest_watermark  # type: ignore

        update_ingest_watermark(source_name, "logical_date", watermark)
    except Exception as exc:  # pragma: no cover
        print(f"Failed to update watermark for {source_name}: {exc}")


def _build_job(
    task_prefix: str,
    job_name: str,
    source_name: str,
    *,
    env_entries: Iterable[str],
    vault_mappings: Iterable[str] | None = None,
    pool: str | None = None,
) -> Tuple[BashOperator, BashOperator, PythonOperator]:
    mapping_flags = ""
    if vault_mappings:
        mapping_flags = " " + " ".join(f"--mapping {mapping}" for mapping in vault_mappings)
    if mapping_flags:
        pull_cmd = (
            f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
            f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
            f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py{mapping_flags} --format shell)\" || true\n"
        )
    else:
        pull_cmd = ""

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    env_line = " ".join(
        list(env_entries)
        + [
            f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}'",
            f"SCHEMA_REGISTRY_URL='{schema_registry}'",
        ]
    )

    render_kwargs: dict[str, object] = {
        "task_id": f"{task_prefix}_render",
        "bash_command": (
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"{pull_cmd}"
            f"if [ \"${{AURUM_DEBUG:-0}}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe {job_name}; fi\n"
            f"AURUM_EXECUTE_SEATUNNEL=0 {env_line} scripts/seatunnel/run_job.sh {job_name} --render-only"
        ),
        "execution_timeout": timedelta(minutes=10),
    }
    if pool:
        render_kwargs["pool"] = pool
    render = BashOperator(**render_kwargs)

    exec_kwargs: dict[str, object] = {
        "task_id": f"{task_prefix}_execute",
        "bash_command": (
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"python scripts/k8s/run_seatunnel_job.py --job-name {job_name} --wait --timeout 600"
        ),
        "execution_timeout": timedelta(minutes=20),
    }
    if pool:
        exec_kwargs["pool"] = pool
    exec_job = BashOperator(**exec_kwargs)

    watermark = PythonOperator(
        task_id=f"{task_prefix}_watermark",
        python_callable=lambda **ctx: _update_watermark(source_name, ctx["logical_date"]),
    )

    return render, exec_job, watermark


with DAG(
    dag_id="ingest_iso_metrics_miso",
    description="Ingest MISO real-time LMP observations",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "miso", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight_lmp = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_miso_rtdb_base",
            ),
            optional_variables=(
                "aurum_miso_rtdb_headers",
                "aurum_miso_lmp_topic",
            ),
        ),
        execution_timeout=timedelta(minutes=5),
    )

    register_sources_lmp = PythonOperator(
        task_id="register_sources",
        python_callable=_register_sources,
        execution_timeout=timedelta(minutes=5),
    )

    lmp_command_lines = [
        "set -euo pipefail",
        "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi",
        "cd /opt/airflow",
        f"export PATH=\"{BIN_PATH}\"",
        f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
        "export MISO_RTDB_HEADERS=\"{{ var.value.get('aurum_miso_rtdb_headers', '') }}\"",
        (
            f"{VENV_PYTHON} scripts/ingest/miso_rtdb_to_kafka.py "
            "--start \"{{ data_interval_start.in_timezone('UTC').isoformat() }}\" "
            "--end \"{{ data_interval_end.in_timezone('UTC').isoformat() }}\" "
            "--market \"{{ var.value.get('aurum_miso_rtdb_market', 'RTM') }}\" "
            "--region \"{{ var.value.get('aurum_miso_rtdb_region', 'ALL') }}\" "
            "--base-url \"{{ var.value.get('aurum_miso_rtdb_base', 'https://api.misoenergy.org') }}\" "
            "--endpoint \"{{ var.value.get('aurum_miso_rtdb_endpoint', '/MISORTDBService/rest/data/getLMPConsolidatedTable') }}\" "
            "--topic \"{{ var.value.get('aurum_miso_lmp_topic', 'aurum.iso.miso.lmp.v1') }}\" "
            "--schema-registry \"{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}\" "
            "--bootstrap-servers \"{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}\" "
            "--interval-seconds {{ var.value.get('aurum_miso_rtdb_interval_seconds', 300) }} "
            "--dlq-topic \"{{ var.value.get('aurum_dlq_topic', '') }}\" "
            "--dlq-subject \"{{ var.value.get('aurum_dlq_subject', 'aurum.ingest.error.v1') }}\""
        ),
    ]
    lmp_ingest = BashOperator(
        task_id="miso_lmp_ingest",
        bash_command="\n".join(lmp_command_lines),
        execution_timeout=timedelta(minutes=20),
        pool="api_miso",
    )

    lmp_watermark = PythonOperator(
        task_id="miso_lmp_watermark",
        python_callable=lambda **ctx: _update_watermark("miso_lmp", ctx["logical_date"]),
        execution_timeout=timedelta(minutes=5),
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    start >> preflight_lmp >> register_sources_lmp >> lmp_ingest >> lmp_watermark >> end
    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_metrics_miso")


with DAG(
    dag_id="ingest_iso_metrics_miso_load_genmix",
    description="Ingest MISO load and generation mix observations",
    default_args=DEFAULT_ARGS,
    schedule_interval="45 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "miso", "load", "genmix"],
) as dag_load:
    start_load = EmptyOperator(task_id="start")

    preflight_load = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_miso_load_endpoint",
                "aurum_miso_genmix_endpoint",
            ),
            optional_variables=(
                "aurum_miso_load_topic",
                "aurum_miso_genmix_topic",
            ),
        ),
        execution_timeout=timedelta(minutes=5),
    )

    register_sources_load = PythonOperator(
        task_id="register_sources",
        python_callable=_register_sources,
        execution_timeout=timedelta(minutes=5),
    )

    load_env = [
        "MISO_LOAD_ENDPOINT=\"{{ var.value.get(\"aurum_miso_load_endpoint\") }}\"",
        "MISO_LOAD_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "MISO_LOAD_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "MISO_LOAD_TOPIC=\"{{ var.value.get(\"aurum_miso_load_topic\", \"aurum.iso.miso.load.v1\") }}\"",
    ]

    load_render, load_exec, load_watermark = _build_job(
        "miso_load",
        "miso_load_to_kafka",
        "miso_load",
        env_entries=load_env,
        pool="api_miso",
    )

    genmix_env = [
        "MISO_GENMIX_ENDPOINT=\"{{ var.value.get(\"aurum_miso_genmix_endpoint\") }}\"",
        "MISO_GENMIX_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "MISO_GENMIX_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "MISO_GENMIX_TOPIC=\"{{ var.value.get(\"aurum_miso_genmix_topic\", \"aurum.iso.miso.genmix.v1\") }}\"",
    ]

    genmix_render, genmix_exec, genmix_watermark = _build_job(
        "miso_genmix",
        "miso_genmix_to_kafka",
        "miso_genmix",
        env_entries=genmix_env,
        pool="api_miso",
    )

    end_load = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    start_load >> preflight_load >> register_sources_load
    register_sources_load >> load_render >> load_exec >> load_watermark
    register_sources_load >> genmix_render >> genmix_exec >> genmix_watermark
    [load_watermark, genmix_watermark] >> end_load
    dag_load.on_failure_callback = build_failure_callback(
        source="aurum.airflow.ingest_iso_metrics_miso_load_genmix"
    )
__all__ = ["dag", "dag_load"]
