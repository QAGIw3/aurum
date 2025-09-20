"""Ingest AESO SMP (system price) into Kafka via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator



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
    # Defer import so the real package is used at runtime inside Airflow workers
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        register_ingest_source(
            "aeso_smp",
            description="AESO system marginal price (SMP)",
            schedule="*/5 * * * *",
            target="kafka",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register ingest source aeso_smp: {exc}")


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


def build_seatunnel_task() -> BashOperator:
    mapping_flags = " --mapping secret/data/aurum/aeso:token=AESO_API_KEY"
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH={PYTHONPATH_ENTRY}:${{PYTHONPATH:-}} "
        f"{VENV_PYTHON} scripts/scripts/secrets/pull_vault_env.py{mapping_flags} --format shell)\" || true\n"
    )

    env_parts = [
        "AESO_ENDPOINT='{{ var.value.get('aurum_aeso_endpoint', 'https://api.aeso.ca/report/v1/price/systemMarginalPrice') }}'",
        "AESO_START='{{ data_interval_start.in_timezone('UTC').isoformat() }}'",
        "AESO_END='{{ data_interval_end.in_timezone('UTC').isoformat() }}'",
        "AESO_TOPIC='{{ var.value.get('aurum_aeso_topic', 'aurum.iso.aeso.lmp.v1') }}'",
        "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'",
        "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
    ]
    env_line = " ".join(env_parts)

    seatunnel_render = BashOperator(
        task_id="seatunnel_aeso_lmp_to_kafka",
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            "mkdir -p /tmp\n"
            "cat > /tmp/iso.lmp.v1.avsc <<'JSON'\n"
            "{\n"
            "  \"type\": \"record\",\n"
            "  \"name\": \"IsoLmpRecord\",\n"
            "  \"namespace\": \"aurum.iso\",\n"
            "  \"doc\": \"Normalized locational marginal price observation from an ISO day-ahead or real-time feed.\",\n"
            "  \"fields\": [\n"
            "    {\n"
            "      \"name\": \"iso_code\",\n"
            "      \"type\": {\n"
            "        \"type\": \"enum\",\n"
            "        \"name\": \"IsoCode\",\n"
            "        \"doc\": \"Market operator the observation belongs to.\",\n"
            "        \"symbols\": [\"PJM\", \"CAISO\", \"ERCOT\", \"NYISO\", \"MISO\", \"ISONE\", \"SPP\"]\n"
            "      }\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"market\",\n"
            "      \"type\": {\n"
            "        \"type\": \"enum\",\n"
            "        \"name\": \"IsoMarket\",\n"
            "        \"doc\": \"Normalized market/run identifier reported by the ISO.\",\n"
            "        \"symbols\": [\n"
            "          \"DAY_AHEAD\",\n"
            "          \"REAL_TIME\",\n"
            "          \"FIFTEEN_MINUTE\",\n"
            "          \"FIVE_MINUTE\",\n"
            "          \"HOUR_AHEAD\",\n"
            "          \"SETTLEMENT\",\n"
            "          \"UNKNOWN\"\n"
            "        ]\n"
            "      }\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"delivery_date\",\n"
            "      \"doc\": \"Trading or operating date for the interval (ISO local calendar).\",\n"
            "      \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"interval_start\",\n"
            "      \"doc\": \"UTC timestamp when the interval starts.\",\n"
            "      \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"interval_end\",\n"
            "      \"doc\": \"Optional UTC timestamp when the interval ends (exclusive).\",\n"
            "      \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"interval_minutes\",\n"
            "      \"doc\": \"Duration of the interval in minutes (if supplied by the ISO).\",\n"
            "      \"type\": [\"null\", \"int\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"location_id\",\n"
            "      \"doc\": \"Primary identifier (node, zone, hub) from the ISO feed.\",\n"
            "      \"type\": \"string\"\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"location_name\",\n"
            "      \"doc\": \"Human-readable description of the location.\",\n"
            "      \"type\": [\"null\", \"string\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"location_type\",\n"
            "      \"doc\": \"Classification of the location identifier.\",\n"
            "      \"type\": {\n"
            "        \"type\": \"enum\",\n"
            "        \"name\": \"IsoLocationType\",\n"
            "        \"symbols\": [\n"
            "          \"NODE\",\n"
            "          \"ZONE\",\n"
            "          \"HUB\",\n"
            "          \"SYSTEM\",\n"
            "          \"AGGREGATE\",\n"
            "          \"INTERFACE\",\n"
            "          \"RESOURCE\",\n"
            "          \"OTHER\"\n"
            "        ]\n"
            "      },\n"
            "      \"default\": \"OTHER\"\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"zone\",\n"
            "      \"doc\": \"Optional ISO zone identifier derived from the location registry.\",\n"
            "      \"type\": [\"null\", \"string\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"hub\",\n"
            "      \"doc\": \"Optional hub grouping associated with the location.\",\n"
            "      \"type\": [\"null\", \"string\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"timezone\",\n"
            "      \"doc\": \"Preferred timezone for interpreting interval timestamps (IANA name).\",\n"
            "      \"type\": [\"null\", \"string\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"price_total\",\n"
            "      \"doc\": \"Locational marginal price reported by the ISO.\",\n"
            "      \"type\": \"double\"\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"price_energy\",\n"
            "      \"doc\": \"Energy component of the price.\",\n"
            "      \"type\": [\"null\", \"double\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"price_congestion\",\n"
            "      \"doc\": \"Congestion component of the price.\",\n"
            "      \"type\": [\"null\", \"double\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"price_loss\",\n"
            "      \"doc\": \"Loss component of the price.\",\n"
            "      \"type\": [\"null\", \"double\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"currency\",\n"
            "      \"doc\": \"ISO reported currency (ISO-4217 code).\",\n"
            "      \"type\": \"string\",\n"
            "      \"default\": \"USD\"\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"uom\",\n"
            "      \"doc\": \"Unit of measure for the price.\",\n"
            "      \"type\": \"string\",\n"
            "      \"default\": \"MWh\"\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"settlement_point\",\n"
            "      \"doc\": \"Optional ISO-specific settlement point grouping.\",\n"
            "      \"type\": [\"null\", \"string\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"source_run_id\",\n"
            "      \"doc\": \"Identifier for the source extraction run (file, report id, etc.).\",\n"
            "      \"type\": [\"null\", \"string\"],\n"
            "      \"default\": null\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"ingest_ts\",\n"
            "      \"doc\": \"Timestamp when the record entered the pipeline (UTC).\",\n"
            "      \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"record_hash\",\n"
            "      \"doc\": \"Deterministic hash of the source fields for idempotency.\",\n"
            "      \"type\": \"string\"\n"
            "    },\n"
            "    {\n"
            "      \"name\": \"metadata\",\n"
            "      \"doc\": \"Optional key/value metadata captured from the source feed.\",\n"
            "      \"type\": [\n"
            "        \"null\",\n"
            "        {\n"
            "          \"type\": \"map\",\n"
            "          \"values\": \"string\"\n"
            "        }\n"
            "      ],\n"
            "      \"default\": null\n"
            "    }\n"
            "  ]\n"
            "}\n"
            "JSON\n"
            f"{pull_cmd}"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            "export ISO_LMP_SCHEMA_PATH=/tmp/iso.lmp.v1.avsc\n"
            # Render-only when running inside Airflow pods without Docker
            f"AURUM_EXECUTE_SEATUNNEL=0 {env_line} scripts/scripts/seatunnel/run_job.sh aeso_lmp_to_kafka --render-only"
        ),
    )
    seatunnel_exec = BashOperator(
        task_id="aeso_execute_k8s",
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            "export ISO_LMP_SCHEMA_PATH=/opt/airflow/scripts/scripts/kafka/schemas/iso.lmp.v1.avsc\n"
            "python scripts/k8s/run_seatunnel_job.py --job-name aeso_lmp_to_kafka --wait --timeout 600"
        ),
    )
    return seatunnel_render, seatunnel_exec


with DAG(
    dag_id="ingest_iso_prices_aeso",
    description="Ingest AESO SMP (system price) via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "aeso", "iso"],
) as dag:
    start = EmptyOperator(task_id="start")

    register_sources = PythonOperator(task_id="register_sources", python_callable=_register_sources)

    aeso_render, aeso_exec = build_seatunnel_task()

    aeso_watermark = PythonOperator(
        task_id="aeso_watermark",
        python_callable=lambda **ctx: _update_watermark("aeso_smp", ctx["logical_date"]),
    )

    end = EmptyOperator(task_id="end")

    start >> register_sources >> aeso_render >> aeso_exec >> aeso_watermark >> end
