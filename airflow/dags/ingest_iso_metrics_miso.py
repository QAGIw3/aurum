"""Airflow DAG to ingest MISO load, generation mix, and RT LMP data."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Tuple

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
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


def _make_minute_gate(minute: int) -> Callable[..., bool]:
    def _gate(**context: Any) -> bool:
        logical_date: datetime = context["logical_date"]
        return logical_date.minute == minute

    return _gate


def _build_job(
    task_prefix: str,
    job_name: str,
    source_name: str,
    *,
    env_entries: Iterable[str],
    vault_mappings: Iterable[str] | None = None,
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

    render = BashOperator(
        task_id=f"{task_prefix}_render",
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"{pull_cmd}"
            f"AURUM_EXECUTE_SEATUNNEL=0 {env_line} scripts/seatunnel/run_job.sh {job_name} --render-only"
        ),
    )

    exec_job = BashOperator(
        task_id=f"{task_prefix}_execute",
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"python scripts/k8s/run_seatunnel_job.py --job-name {job_name} --wait --timeout 600"
        ),
    )

    watermark = PythonOperator(
        task_id=f"{task_prefix}_watermark",
        python_callable=lambda **ctx: _update_watermark(source_name, ctx["logical_date"]),
    )

    return render, exec_job, watermark


with DAG(
    dag_id="ingest_iso_metrics_miso",
    description="Ingest MISO load, generation mix, and real-time LMP observations",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "miso", "load", "genmix", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    register_sources = PythonOperator(task_id="register_sources", python_callable=_register_sources)

    load_gate = ShortCircuitOperator(
        task_id="miso_load_gate",
        python_callable=_make_minute_gate(45),
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
    )

    genmix_gate = ShortCircuitOperator(
        task_id="miso_genmix_gate",
        python_callable=_make_minute_gate(45),
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
    )

    lmp_command_lines = [
        "set -euo pipefail",
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
    )

    lmp_watermark = PythonOperator(
        task_id="miso_lmp_watermark",
        python_callable=lambda **ctx: _update_watermark("miso_lmp", ctx["logical_date"]),
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    start >> register_sources
    register_sources >> load_gate >> load_render >> load_exec >> load_watermark
    register_sources >> genmix_gate >> genmix_render >> genmix_exec >> genmix_watermark
    register_sources >> lmp_ingest >> lmp_watermark
    [load_watermark, genmix_watermark, lmp_watermark] >> end
