"""Airflow DAG to ingest EIA bulk CSV archives into Kafka."""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")
BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "eia_bulk_datasets.json"
MANIFEST_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "eia" / "bulk_manifest.py"
MANIFEST_OUTPUT = Path(__file__).resolve().parents[2] / "artifacts" / "eia_bulk_manifest.json"

_SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if _SRC_PATH and _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)

from aurum.airflow_utils import build_default_args
from aurum.airflow_utils import iso as iso_utils

DEFAULT_ARGS: dict[str, Any] = build_default_args(
    owner="aurum-data",
    depends_on_past=False,
    email_on_failure=True,
    email=("aurum-ops@example.com", "data-team@aurum.com", "energy-team@aurum.com"),
    retries=5,
    retry_delay=timedelta(minutes=15),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=60),
    execution_timeout=timedelta(hours=8),
    sla=timedelta(hours=48),
    pool="api_eia",
    pool_slots=1,
    on_failure_callback=None,
    on_success_callback=None,
    on_retry_callback=None,
)


def _shell_quote(value: str) -> str:
    return value.replace("'", "'\"'\"'")


def _format_env_var(key: str, value: str, *, quote: bool = True) -> str:
    if quote:
        return f"{key}='{_shell_quote(value)}'"
    return f"{key}={value}"


def _load_bulk_datasets() -> list[dict[str, Any]]:
    if not CONFIG_PATH.exists():
        return []
    raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    datasets = raw.get("datasets", [])
    if not isinstance(datasets, list):
        raise RuntimeError(f"Expected 'datasets' array in {CONFIG_PATH}")
    return datasets


BULK_DATASETS = _load_bulk_datasets()


def _build_schema_fields_json(cfg: dict[str, Any]) -> str | None:
    fields = cfg.get("schema_fields")
    if not fields:
        return None
    return json.dumps(fields)


def _build_bulk_env(cfg: dict[str, Any]) -> list[str]:
    env: list[str] = []
    env.append(_format_env_var("EIA_BULK_URL", cfg["url"]))
    env.append(
        _format_env_var(
            "EIA_BULK_TOPIC",
            f"{{{{ var.value.get('{cfg['topic_var']}', '{cfg['default_topic']}') }}}}",
        )
    )
    env.append(_format_env_var("EIA_BULK_FREQUENCY", cfg.get("frequency", "MONTHLY")))

    for key, env_name in [
        ("series_id_expr", "EIA_BULK_SERIES_ID_EXPR"),
        ("period_expr", "EIA_BULK_PERIOD_EXPR"),
        ("value_expr", "EIA_BULK_VALUE_EXPR"),
        ("raw_value_expr", "EIA_BULK_RAW_VALUE_EXPR"),
        ("units_expr", "EIA_BULK_UNITS_EXPR"),
        ("area_expr", "EIA_BULK_AREA_EXPR"),
        ("sector_expr", "EIA_BULK_SECTOR_EXPR"),
        ("description_expr", "EIA_BULK_DESCRIPTION_EXPR"),
        ("source_expr", "EIA_BULK_SOURCE_EXPR"),
        ("dataset_expr", "EIA_BULK_DATASET_EXPR"),
        ("metadata_expr", "EIA_BULK_METADATA_EXPR"),
        ("filter_expr", "EIA_BULK_FILTER_EXPR"),
    ]:
        value = cfg.get(key)
        if value is not None:
            env.append(_format_env_var(env_name, str(value), quote=True))

    if cfg.get("csv_delimiter") is not None:
        env.append(_format_env_var("EIA_BULK_CSV_DELIMITER", str(cfg["csv_delimiter"])))
    if cfg.get("skip_header") is not None:
        env.append(_format_env_var("EIA_BULK_SKIP_HEADER", str(cfg["skip_header"]), quote=False))

    schema_fields_json = _build_schema_fields_json(cfg)
    if schema_fields_json is not None:
        env.append(_format_env_var("EIA_BULK_SCHEMA_FIELDS_JSON", schema_fields_json))

    extra_env = cfg.get("extra_env", {})
    if isinstance(extra_env, dict):
        for key, value in extra_env.items():
            env.append(_format_env_var(key, str(value)))

    if cfg.get("last_modified"):
        env.append(_format_env_var("EIA_BULK_LAST_MODIFIED", str(cfg["last_modified"])))

    env.append(
        _format_env_var(
            "KAFKA_BOOTSTRAP_SERVERS",
            "{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}",
        )
    )
    env.append(
        _format_env_var(
            "SCHEMA_REGISTRY_URL",
            "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}",
        )
    )

    return env


def _build_bulk_task(cfg: dict[str, Any]) -> BashOperator:
    env_assignments = _build_bulk_env(cfg)
    mappings = cfg.get("secret_mappings", []) or []
    mapping_flags = ""
    if mappings:
        mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)
    pull_cmd = ""
    if mapping_flags:
        pull_cmd = (
            f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
            f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
            f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true\n"
        )

    env_line = " ".join(env_assignments)

    return BashOperator(
        task_id=f"seatunnel_{cfg['source_name']}",
        bash_command=(
            "set -euo pipefail\n"
            f"cd /opt/airflow\n"
            f"{pull_cmd}"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"{env_line} scripts/seatunnel/run_job.sh eia_bulk_to_kafka"
        ),
    )


def _refresh_manifest(**_: Any) -> None:
    cmd = [
        VENV_PYTHON,
        str(MANIFEST_SCRIPT),
        f"--manifest-url=https://www.eia.gov/opendata/bulk/manifest.txt",
        f"--output={MANIFEST_OUTPUT}",
        f"--config={CONFIG_PATH}",
        "--update-config",
    ]
    result = subprocess.run(cmd, cwd="/opt/airflow", capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Bulk manifest refresh failed: {result.stderr.strip()}")
    print(result.stdout)


def _register_bulk_sources(**context: Any) -> None:
    if not BULK_DATASETS:
        return
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        for cfg in BULK_DATASETS:
            try:
                register_ingest_source(
                    cfg["source_name"],
                    description=cfg.get("description"),
                    schedule=cfg.get("schedule"),
                    target=cfg.get("default_topic"),
                )
            except Exception as exc:  # pragma: no cover
                print(f"Failed to register ingest source {cfg['source_name']}: {exc}")
    except Exception as exc:  # pragma: no cover
        print(f"Bulk source registration failed: {exc}")


def _preflight_required_vars(keys: list[str]) -> None:
    try:
        from airflow.models import Variable  # type: ignore
    except Exception as exc:  # pragma: no cover
        print(f"Airflow Variable API unavailable: {exc}")
        return
    missing: list[str] = []
    for key in keys:
        try:
            Variable.get(key)
        except Exception:
            missing.append(key)
    if missing:
        critical = {"aurum_kafka_bootstrap", "aurum_schema_registry"}
        if any(k in critical for k in missing):
            raise RuntimeError(f"Missing required Airflow Variables: {missing}")
        print(f"Warning: missing optional Airflow Variables: {missing}")


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


def _make_watermark_callable(source_name: str):
    return iso_utils.make_watermark_callable(source_name)


with DAG(
    dag_id="ingest_eia_bulk",
    description="Ingest EIA bulk CSV archives into Kafka",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6,18 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "eia", "bulk"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=lambda **_: _preflight_required_vars(
            ["aurum_kafka_bootstrap", "aurum_schema_registry"]
        ),
    )

    refresh_manifest = PythonOperator(
        task_id="refresh_bulk_manifest",
        python_callable=_refresh_manifest,
    )

    register_sources = PythonOperator(
        task_id="register_bulk_sources",
        python_callable=_register_bulk_sources,
    )

    task_results: list[tuple[BashOperator, PythonOperator]] = []
    for dataset_cfg in BULK_DATASETS:
        bulk_task = _build_bulk_task(dataset_cfg)
        watermark_task = PythonOperator(
            task_id=f"{dataset_cfg['source_name']}_watermark",
            python_callable=_make_watermark_callable(dataset_cfg["source_name"]),
        )
        task_results.append((bulk_task, watermark_task))

    end = EmptyOperator(task_id="end")

    start >> preflight >> refresh_manifest >> register_sources
    for bulk_task, watermark_task in task_results:
        register_sources >> bulk_task >> watermark_task >> end

    if not task_results:
        register_sources >> end
