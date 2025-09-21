"""Airflow DAG to ingest ISO-NE and SPP feeds via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable, metrics


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=45),
}

BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")

ISO_SOURCES = {
    "isone_ws": "ISO-NE web services ingestion",
    "spp_file": "SPP market file ingestion",
}


def _register_sources() -> None:
    # Defer import so the real package is used at runtime inside Airflow workers
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        for name, description in ISO_SOURCES.items():
            try:
                register_ingest_source(
                    name,
                    description=description,
                    schedule="20 * * * *",
                    target="kafka",
                )
            except Exception as exc:  # pragma: no cover
                print(f"Failed to register ingest source {name}: {exc}")
    except Exception as exc:  # pragma: no cover
        print(f"Failed during register_sources setup: {exc}")


def _update_watermark(source_name: str, logical_date: datetime) -> None:
    watermark = logical_date.astimezone(timezone.utc)
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import update_ingest_watermark  # type: ignore

        update_ingest_watermark(source_name, "logical_date", watermark)
        metrics.record_watermark_success(source_name, watermark)
    except Exception as exc:  # pragma: no cover
        print(f"Failed to update watermark for {source_name}: {exc}")


def build_seatunnel_task(job_name: str, env_assignments: list[str], mappings: list[str]) -> tuple[BashOperator, BashOperator]:
    mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH={PYTHONPATH_ENTRY}:${{PYTHONPATH:-}} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true\n"
    )

    env_line = " ".join(env_assignments)

    render = BashOperator(
        task_id=f"seatunnel_{job_name}",
        bash_command=(
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            "cd /opt/airflow\n"
            f"{pull_cmd}"
            f"if [ \"${{AURUM_DEBUG:-0}}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe {job_name}; fi\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then env | grep -E 'DLQ_TOPIC|DLQ_SUBJECT' || true; fi\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"AURUM_EXECUTE_SEATUNNEL=0 {env_line} scripts/seatunnel/run_job.sh {job_name} --render-only"
        ),
        execution_timeout=timedelta(minutes=10),
    )
    exec_k8s = BashOperator(
        task_id=f"seatunnel_{job_name}_execute_k8s",
        bash_command=(
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"python scripts/k8s/run_seatunnel_job.py --job-name {job_name} --wait --timeout 600"
        ),
        execution_timeout=timedelta(minutes=20),
    )
    return render, exec_k8s


with DAG(
    dag_id="ingest_iso_prices_isone_spp",
    description="Ingest ISO-NE and SPP feeds via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="20 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["aurum", "isone", "spp", "iso"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=("aurum_kafka_bootstrap", "aurum_schema_registry"),
            optional_variables=(
                "aurum_isone_ws_username",
                "aurum_isone_ws_password",
                "aurum_spp_file_base_url",
                "aurum_spp_topic",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=_register_sources,
    )

    isone_render, isone_exec = build_seatunnel_task(
        "isone_lmp_to_kafka",
        [
            "ISONE_URL='{{ var.value.get('aurum_isone_endpoint') }}'",
            "ISONE_START='{{ data_interval_start.in_timezone('UTC').isoformat() }}'",
            "ISONE_END='{{ data_interval_end.in_timezone('UTC').isoformat() }}'",
            "ISONE_MARKET='{{ var.value.get('aurum_isone_market', 'DA') }}'",
            "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
        ],
        mappings=[
            "secret/data/aurum/isone:username=ISONE_USERNAME",
            "secret/data/aurum/isone:password=ISONE_PASSWORD",
        ],
    )

    # SPP: stage JSON via Python helper, then publish via SeaTunnel LocalFile source
    spp_staging = os.environ.get("AURUM_STAGING_DIR", "files/staging") + "/spp/{{ ds }}.json"

    stage_spp_cmd = "\n".join(
        [
            "set -euo pipefail",
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi",
            "cd /opt/airflow",
            "mkdir -p $(dirname '" + spp_staging + "')",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            (
                "python scripts/ingest/spp_file_api_to_kafka.py "
                "--base-url {{ var.value.get('aurum_spp_base_url') }} "
                "--report {{ var.value.get('aurum_spp_rt_report') }} "
                "--date {{ ds }} "
                f"--output-json {spp_staging} --no-kafka"
            ),
        ]
    )
    stage_spp = BashOperator(
        task_id="stage_spp_lmp",
        bash_command=stage_spp_cmd,
        execution_timeout=timedelta(minutes=20),
    )

    spp_render, spp_exec = build_seatunnel_task(
        "spp_lmp_to_kafka",
        [
            f"SPP_INPUT_JSON={spp_staging}",
            "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'",
            "SPP_TOPIC='{{ var.value.get('aurum_spp_topic', 'aurum.iso.spp.lmp.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
            "ISO_LMP_SCHEMA_PATH=/opt/airflow/scripts/kafka/schemas/iso.lmp.v1.avsc",
        ],
        mappings=[],
    )

    isone_watermark = PythonOperator(
        task_id="isone_watermark",
        python_callable=lambda **ctx: _update_watermark("isone_ws", ctx["logical_date"]),
    )

    spp_watermark = PythonOperator(
        task_id="spp_watermark",
        python_callable=lambda **ctx: _update_watermark("spp_file", ctx["logical_date"]),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> isone_render >> isone_exec >> isone_watermark
    register_sources >> stage_spp >> spp_render >> spp_exec >> spp_watermark
    [isone_watermark, spp_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_isone_spp")
