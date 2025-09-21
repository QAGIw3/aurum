"""Airflow DAG to ingest NYISO and MISO day-ahead feeds via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable
from aurum.airflow_utils import iso as iso_utils


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


SOURCES = (
    iso_utils.IngestSource(
        "nyiso_csv",
        description="NYISO LBMP CSV ingestion",
        schedule="0 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "miso_csv",
        description="MISO market report CSV ingestion",
        schedule="0 * * * *",
        target="kafka",
    ),
)


def build_seatunnel_task(job_name: str, *, env_assignments: list[str], task_id_override: str | None = None) -> tuple[BashOperator, BashOperator]:
    mappings = ["secret/data/aurum/kafka:bootstrap=KAFKA_BOOTSTRAP_SERVERS"]
    mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH={PYTHONPATH_ENTRY}:${{PYTHONPATH:-}} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true"
    )

    env_line = " ".join(env_assignments)

    render = BashOperator(
        task_id=task_id_override or f"seatunnel_{job_name}",
        bash_command=iso_utils.build_render_command(
            job_name=job_name,
            env_assignments=f"AURUM_EXECUTE_SEATUNNEL=0 {env_line}",
            bin_path=BIN_PATH,
            pythonpath_entry=PYTHONPATH_ENTRY,
            debug_dump_env=True,
            pre_lines=[pull_cmd],
        ),
        execution_timeout=timedelta(minutes=10),
    )
    exec_k8s = BashOperator(
        task_id=(task_id_override or f"seatunnel_{job_name}") + "_execute_k8s",
        bash_command=iso_utils.build_k8s_command(
            job_name,
            bin_path=BIN_PATH,
            pythonpath_entry=PYTHONPATH_ENTRY,
            timeout=600,
        ),
        execution_timeout=timedelta(minutes=20),
    )
    return render, exec_k8s


with DAG(
    dag_id="ingest_iso_prices_nyiso_miso",
    description="Ingest NYISO and MISO day-ahead/real-time LMP feeds into Kafka",
    default_args=DEFAULT_ARGS,
    schedule_interval="15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "iso", "nyiso", "miso"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_nyiso_csv_url",
                "aurum_miso_da_url",
            ),
            optional_variables=(
                "aurum_nyiso_topic",
                "aurum_miso_topic",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    nyiso_render, nyiso_exec = build_seatunnel_task(
        "nyiso_lmp_to_kafka",
        env_assignments=[
            "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'",
            "NYISO_URL='{{ var.value.get('aurum_nyiso_url') }}'",
            "NYISO_TOPIC='{{ var.value.get('aurum_nyiso_topic', 'aurum.iso.nyiso.lmp.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
    )

    miso_da_render, miso_da_exec = build_seatunnel_task(
        "miso_lmp_to_kafka",
        env_assignments=[
            "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'",
            "MISO_URL='{{ var.value.get('aurum_miso_da_url') }}'",
            "MISO_MARKET='DAY_AHEAD'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        task_id_override="seatunnel_miso_lmp_da",
    )

    miso_rt_render, miso_rt_exec = build_seatunnel_task(
        "miso_lmp_to_kafka",
        env_assignments=[
            "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'",
            "MISO_URL='{{ var.value.get('aurum_miso_rt_url') }}'",
            "MISO_MARKET='REAL_TIME'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        task_id_override="seatunnel_miso_lmp_rt",
    )

    nyiso_watermark = PythonOperator(
        task_id="nyiso_watermark",
        python_callable=iso_utils.make_watermark_callable("nyiso_csv"),
    )

    miso_da_watermark = PythonOperator(
        task_id="miso_da_watermark",
        python_callable=iso_utils.make_watermark_callable("miso_csv"),
    )

    miso_rt_watermark = PythonOperator(
        task_id="miso_rt_watermark",
        python_callable=iso_utils.make_watermark_callable("miso_csv"),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> nyiso_render >> nyiso_exec >> nyiso_watermark
    register_sources >> miso_da_render >> miso_da_exec >> miso_da_watermark
    register_sources >> miso_rt_render >> miso_rt_exec >> miso_rt_watermark
    [nyiso_watermark, miso_da_watermark, miso_rt_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_nyiso_miso")
