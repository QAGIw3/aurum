"""Airflow DAG to load ISO LMP Kafka topics into TimescaleDB via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable
from aurum.airflow_utils import iso as iso_utils


def _validate_recent() -> None:
    import pandas as pd
    import psycopg
    from airflow.models import Variable  # type: ignore

    from aurum.dq import enforce_expectation_suite

    dsn = os.getenv("AURUM_TIMESCALE_DSN") or Variable.get(
        "aurum_timescale_dsn",
        default_var="postgresql://timescale:timescale@timescale:5432/timeseries",
    )
    lookback_minutes = int(os.getenv("AURUM_TIMESCALE_DQ_LOOKBACK_MINUTES", "120"))
    limit = int(os.getenv("AURUM_TIMESCALE_DQ_MAX_ROWS", "5000"))

    query = f"""
        SELECT
            iso_code,
            market,
            delivery_date,
            interval_start,
            interval_end,
            interval_minutes,
            location_id,
            location_name,
            location_type,
            price_total,
            price_energy,
            price_congestion,
            price_loss,
            currency,
            uom,
            settlement_point,
            source_run_id,
            ingest_ts,
            record_hash,
            metadata
        FROM public.iso_lmp_timeseries
        WHERE interval_start >= NOW() - INTERVAL '{max(lookback_minutes, 5)} minutes'
        ORDER BY interval_start DESC
        LIMIT {limit}
    """

    with psycopg.connect(dsn, autocommit=True) as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        print("No ISO LMP rows available for DQ validation window; skipping expectations.")
        return

    suite_path = Path(__file__).resolve().parents[2] / "ge" / "expectations" / "iso_lmp.json"
    enforce_expectation_suite(df, suite_path)
    print(f"Validated {len(df)} ISO LMP rows against {suite_path.name} expectations.")



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


VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")
BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")

SOURCES = (
    iso_utils.IngestSource(
        "iso_lmp_timescale",
        description="ISO LMP streaming load into Timescale",
        schedule="0 * * * *",
        target="timescale.public.iso_lmp_timeseries",
    ),
)



def build_timescale_task(task_id: str) -> BashOperator:
    mappings = [
        "secret/data/aurum/timescale:user=TIMESCALE_USER",
        "secret/data/aurum/timescale:password=TIMESCALE_PASSWORD",
    ]
    mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true"
    )

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"
    jdbc_url = "{{ var.value.get('aurum_timescale_jdbc', 'jdbc:postgresql://timescale:5432/timeseries') }}"
    topic_pattern = "{{ var.value.get('aurum_iso_lmp_topic_pattern', 'aurum\\.iso\\..*\\.lmp\\.v1') }}"
    table_name = "{{ var.value.get('aurum_iso_lmp_table', 'iso_lmp_timeseries') }}"

    env_line = (
        f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}' "
        f"SCHEMA_REGISTRY_URL='{schema_registry}' "
        f"TIMESCALE_JDBC_URL='{jdbc_url}' "
        f"ISO_LMP_TOPIC_PATTERN='{topic_pattern}' "
        f"ISO_LMP_TABLE='{table_name}'"
    )

    return BashOperator(
        task_id=task_id,
        bash_command=(
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            f"{pull_cmd}\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe iso_lmp_kafka_to_timescale; fi\n"
            f"{env_line} scripts/seatunnel/run_job.sh iso_lmp_kafka_to_timescale"
        ),
    )


with DAG(
    dag_id="ingest_iso_prices_timescale",
    description="Stream ISO LMP Kafka topics into TimescaleDB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "timescale", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_timescale_jdbc",
            ),
            optional_variables=(
                "aurum_iso_lmp_topic_pattern",
                "aurum_iso_lmp_table",
                "aurum_timescale_dsn",
            ),
        ),
    )

    register_source = PythonOperator(
        task_id="register_iso_lmp_source",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    load_timescale = build_timescale_task(task_id="iso_lmp_kafka_to_timescale")

    dq_check = PythonOperator(task_id="iso_lmp_dq_check", python_callable=_validate_recent)

    watermark = PythonOperator(
        task_id="update_iso_lmp_watermark",
        python_callable=iso_utils.make_watermark_callable("iso_lmp_timescale"),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_source >> load_timescale >> dq_check >> watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_timescale")
