"""Airflow DAG to ingest public reference feeds (NOAA, EIA, FRED, CPI, PJM)."""
from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

try:
    from aurum.reference.eia_catalog import get_dataset
except Exception:  # Use in-cluster stub if package layout isn't available
    class _EIADatasetStub:
        def __init__(self, default_frequency: str = "OTHER"):
            self.default_frequency = default_frequency

    def get_dataset(path: str) -> _EIADatasetStub:  # type: ignore[misc]
        return _EIADatasetStub()

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


PUBLIC_SOURCES: dict[str, dict[str, str]] = {
    "noaa_ghcnd": {
        "description": "NOAA GHCND daily ingestion",
        "schedule": "0 6 * * *",
    },
    "eia_series": {
        "description": "EIA v2 series ingestion",
        "schedule": "0 6 * * *",
    },
    "fred_series": {
        "description": "FRED series ingestion",
        "schedule": "0 6 * * *",
    },
    "cpi_series": {
        "description": "FRED CPI series ingestion",
        "schedule": "0 6 * * *",
    },
    "fuel_natgas_curve": {
        "description": "EIA Henry Hub natural gas fuel curve ingestion",
        "schedule": "0 6 * * *",
    },
    "fuel_co2_curve": {
        "description": "EIA CO₂ allowance price ingestion",
        "schedule": "0 6 * * *",
    },
    "pjm_lmp": {
        "description": "PJM day-ahead LMP ingestion",
        "schedule": "0 6 * * *",
    },
}


_EIA_DYNAMIC_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "eia_ingest_datasets.json"
)


def _load_eia_dynamic_datasets() -> list[dict[str, Any]]:
    if not _EIA_DYNAMIC_CONFIG_PATH.exists():
        return []

    try:
        raw_config = json.loads(_EIA_DYNAMIC_CONFIG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - operator error
        raise RuntimeError(
            f"Failed to parse {_EIA_DYNAMIC_CONFIG_PATH}: {exc}"
        ) from exc

    datasets: list[dict[str, Any]] = raw_config.get("datasets", [])
    if not isinstance(datasets, list):
        raise RuntimeError(
            f"Expected 'datasets' array in {_EIA_DYNAMIC_CONFIG_PATH}, got {type(datasets)}"
        )
    return datasets


EIA_DYNAMIC_DATASETS = _load_eia_dynamic_datasets()

for dataset_cfg in EIA_DYNAMIC_DATASETS:
    PUBLIC_SOURCES[dataset_cfg["source_name"]] = {
        "description": dataset_cfg.get("description", dataset_cfg["source_name"]),
        "schedule": dataset_cfg.get("schedule", "0 6 * * *"),
    }


def _register_sources() -> None:
    # Defer import to task runtime so real package is used in Airflow pods
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        for name, config in PUBLIC_SOURCES.items():
            description = config.get("description")
            schedule = config.get("schedule")
            target = config.get("target", "kafka")
            try:
                register_ingest_source(
                    name,
                    description=description,
                    schedule=schedule,
                    target=target,
                )
            except Exception as exc:  # pragma: no cover
                print(f"Failed to register ingest source {name}: {exc}")
    except Exception as exc:  # pragma: no cover
        print(f"Failed during register_sources setup: {exc}")


def _preflight_required_vars(keys: list[str]) -> None:
    """Validate required Airflow Variables exist; log and continue if missing optional ones.

    Fails the task if critical variables are missing to surface configuration issues early.
    """
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
        # Treat Kafka bootstrap and Schema Registry as critical
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


def _shell_quote(value: str) -> str:
    return value.replace("'", "'\"'\"'")


def _format_env_var(key: str, value: str, *, quote: bool = True) -> str:
    if quote:
        return f"{key}='{_shell_quote(value)}'"
    return f"{key}={value}"


def _build_eia_env(cfg: dict[str, Any]) -> list[str]:
    dataset = get_dataset(cfg["path"])
    env: list[str] = []

    series_path = cfg.get("data_path", cfg["path"])
    env.append(_format_env_var("EIA_SERIES_PATH", series_path))
    env.append(_format_env_var("EIA_SERIES_ID", cfg.get("series_id", "MULTI_SERIES")))
    env.append(_format_env_var("EIA_SERIES_ID_EXPR", cfg["series_id_expr"], quote=False))
    env.append(
        _format_env_var(
            "EIA_FREQUENCY",
            cfg.get("frequency") or (dataset.default_frequency or "OTHER").upper(),
        )
    )
    env.append(
        _format_env_var(
            "EIA_TOPIC",
            f"{{{{ var.value.get('{cfg['topic_var']}', '{cfg['default_topic']}') }}}}",
        )
    )
    env.append(
        _format_env_var(
            "EIA_UNITS",
            f"{{{{ var.value.get('{cfg['units_var']}', '{cfg['default_units']}') }}}}",
        )
    )
    env.append(_format_env_var("EIA_SEASONAL_ADJUSTMENT", cfg.get("seasonal_adjustment", "UNKNOWN")))
    env.append(_format_env_var("EIA_AREA_EXPR", cfg.get("area_expr", "CAST(NULL AS STRING)")))
    env.append(_format_env_var("EIA_SECTOR_EXPR", cfg.get("sector_expr", "CAST(NULL AS STRING)")))
    env.append(_format_env_var("EIA_DESCRIPTION_EXPR", cfg.get("description_expr", "CAST(NULL AS STRING)")))
    env.append(_format_env_var("EIA_SOURCE_EXPR", cfg.get("source_expr", "COALESCE(source, 'EIA')")))
    env.append(_format_env_var("EIA_DATASET_EXPR", cfg.get("dataset_expr", "COALESCE(dataset, '')")))
    env.append(_format_env_var("EIA_METADATA_EXPR", cfg.get("metadata_expr", "NULL")))
    env.append(_format_env_var("EIA_FILTER_EXPR", cfg.get("filter_expr", "TRUE"), quote=False))

    overrides_json = json.dumps(cfg.get("param_overrides", []))
    env.append(_format_env_var("EIA_PARAM_OVERRIDES_JSON", overrides_json))

    for extra_key, extra_value in cfg.get("extra_env", {}).items():
        env.append(_format_env_var(extra_key, extra_value))

    env.append(
        _format_env_var(
            "SCHEMA_REGISTRY_URL",
            "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}",
        )
    )

    return env


def _make_watermark_callable(source_name: str):
    def _call(**ctx: Any) -> None:
        _update_watermark(source_name, ctx["logical_date"])

    return _call


def build_seatunnel_task(
    job_name: str,
    env_assignments: list[str],
    mappings: list[str] | None = None,
    *,
    pool: str | None = None,
    task_id_override: str | None = None,
) -> BashOperator:
    mapping_flags = ""
    if mappings:
        mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)
    pull_cmd = ""
    if mapping_flags:
        pull_cmd = (
            f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
            f"PYTHONPATH={PYTHONPATH_ENTRY}:${{PYTHONPATH:-}} "
            f"{VENV_PYTHON} scripts/scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true\n"
        )

    # Ensure KAFKA bootstrap is always present for render-only validation
    env_all = list(env_assignments) + [
        "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'"
    ]
    env_line = " ".join(env_all)

    operator_kwargs: dict[str, object] = {
        "task_id": task_id_override or f"seatunnel_{job_name}",
        "bash_command": (
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            f"{pull_cmd}"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            # Render-only in Airflow pods without Docker
            f"AURUM_EXECUTE_SEATUNNEL=0 {env_line} scripts/scripts/seatunnel/run_job.sh {job_name} --render-only"
        ),
    }
    if pool:
        operator_kwargs["pool"] = pool
    return BashOperator(**operator_kwargs)


with DAG(
    dag_id="ingest_public_feeds",
    description="Ingest NOAA, EIA, FRED, CPI, and PJM public data feeds",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "public", "ingestion"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=lambda **_: _preflight_required_vars(
            [
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_timescale_jdbc",
            ]
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=_register_sources,
    )

    noaa_task = build_seatunnel_task(
        "noaa_ghcnd_to_kafka",
        [
            "NOAA_GHCND_START_DATE='{{ ds }}'",
            "NOAA_GHCND_END_DATE='{{ ds }}'",
            "NOAA_GHCND_TOPIC='{{ var.value.get('aurum_noaa_topic', 'aurum.ref.noaa.weather.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/noaa:token=NOAA_GHCND_TOKEN"],
    )

    # Optional: load NOAA weather stream into Timescale after ingesting to Kafka
    noaa_to_timescale = build_seatunnel_task(
        "noaa_weather_kafka_to_timescale",
        [
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
            "TIMESCALE_JDBC_URL='{{ var.value.get('aurum_timescale_jdbc', 'jdbc:postgresql://timescale:5432/timeseries') }}'",
            "NOAA_TABLE='{{ var.value.get('aurum_noaa_timescale_table', 'noaa_weather_timeseries') }}'",
        ],
        mappings=[
            "secret/data/aurum/timescale:user=TIMESCALE_USER",
            "secret/data/aurum/timescale:password=TIMESCALE_PASSWORD",
        ],
        task_id_override="seatunnel_noaa_weather_timescale",
    )

    eia_task = build_seatunnel_task(
        "eia_series_to_kafka",
        [
            "EIA_SERIES_PATH='{{ var.value.get('aurum_eia_series_path', 'electricity/wholesale/prices/data') }}'",
            "EIA_SERIES_ID='{{ var.value.get('aurum_eia_series_id', 'EBA.ALL.D.H') }}'",
            "EIA_FREQUENCY='{{ var.value.get('aurum_eia_frequency', 'HOURLY') }}'",
            "EIA_TOPIC='{{ var.value.get('aurum_eia_topic', 'aurum.ref.eia.series.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/eia:api_key=EIA_API_KEY"],
        task_id_override="seatunnel_eia_series_main",
    )

    # Optional: EIA series sink to Timescale
    eia_to_timescale = build_seatunnel_task(
        "eia_series_kafka_to_timescale",
        [
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
            "TIMESCALE_JDBC_URL='{{ var.value.get('aurum_timescale_jdbc', 'jdbc:postgresql://timescale:5432/timeseries') }}'",
        ],
        mappings=[
            "secret/data/aurum/timescale:user=TIMESCALE_USER",
            "secret/data/aurum/timescale:password=TIMESCALE_PASSWORD",
        ],
        task_id_override="seatunnel_eia_series_timescale",
    )

    dynamic_eia_results: list[tuple[dict[str, Any], BashOperator, PythonOperator]] = []
    for dataset_cfg in EIA_DYNAMIC_DATASETS:
        env_assignments = _build_eia_env(dataset_cfg)
        task = build_seatunnel_task(
            "eia_series_to_kafka",
            env_assignments,
            mappings=["secret/data/aurum/eia:api_key=EIA_API_KEY"],
            task_id_override=f"seatunnel_{dataset_cfg['source_name']}",
        )
        watermark = PythonOperator(
            task_id=f"{dataset_cfg['source_name']}_watermark",
            python_callable=_make_watermark_callable(dataset_cfg["source_name"]),
        )
        dynamic_eia_results.append((dataset_cfg, task, watermark))

    fred_task = build_seatunnel_task(
        "fred_series_to_kafka",
        [
            "FRED_SERIES_ID='{{ var.value.get('aurum_fred_series_id', 'DGS10') }}'",
            "FRED_FREQUENCY='{{ var.value.get('aurum_fred_frequency', 'DAILY') }}'",
            "FRED_SEASONAL_ADJ='{{ var.value.get('aurum_fred_seasonal_adj', 'NSA') }}'",
            "FRED_TOPIC='{{ var.value.get('aurum_fred_topic', 'aurum.ref.fred.series.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/fred:api_key=FRED_API_KEY"],
    )

    fred_to_timescale = build_seatunnel_task(
        "fred_series_kafka_to_timescale",
        [
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
            "TIMESCALE_JDBC_URL='{{ var.value.get('aurum_timescale_jdbc', 'jdbc:postgresql://timescale:5432/timeseries') }}'",
        ],
        mappings=[
            "secret/data/aurum/timescale:user=TIMESCALE_USER",
            "secret/data/aurum/timescale:password=TIMESCALE_PASSWORD",
        ],
        task_id_override="seatunnel_fred_series_timescale",
    )

    cpi_task = build_seatunnel_task(
        "cpi_series_to_kafka",
        [
            "CPI_SERIES_ID='{{ var.value.get('aurum_cpi_series_id', 'CPIAUCSL') }}'",
            "CPI_FREQUENCY='{{ var.value.get('aurum_cpi_frequency', 'MONTHLY') }}'",
            "CPI_SEASONAL_ADJ='{{ var.value.get('aurum_cpi_seasonal_adj', 'SA') }}'",
            "CPI_TOPIC='{{ var.value.get('aurum_cpi_topic', 'aurum.ref.cpi.series.v1') }}'",
            "CPI_START_DATE='{{ data_interval_start | ds }}'",
            "CPI_END_DATE='{{ data_interval_start | ds }}'",
            "CPI_AREA='{{ var.value.get('aurum_cpi_area', 'US') }}'",
            "CPI_UNITS='{{ var.value.get('aurum_cpi_units', 'Index') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/fred:api_key=FRED_API_KEY"],
    )

    cpi_to_timescale = build_seatunnel_task(
        "cpi_series_kafka_to_timescale",
        [
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
            "TIMESCALE_JDBC_URL='{{ var.value.get('aurum_timescale_jdbc', 'jdbc:postgresql://timescale:5432/timeseries') }}'",
        ],
        mappings=[
            "secret/data/aurum/timescale:user=TIMESCALE_USER",
            "secret/data/aurum/timescale:password=TIMESCALE_PASSWORD",
        ],
        task_id_override="seatunnel_cpi_series_timescale",
    )

    fuel_natgas_task = build_seatunnel_task(
        "eia_fuel_curve_to_kafka",
        [
            "FUEL_EIA_PATH='{{ var.value.get('aurum_fuel_natgas_eia_path', 'natural-gas/pri/fut/wfut/data') }}'",
            "FUEL_SERIES_ID='{{ var.value.get('aurum_fuel_natgas_series_id', 'EIA_NATGAS_HENRY_HUB') }}'",
            "FUEL_FUEL_TYPE='NATURAL_GAS'",
            "FUEL_FREQUENCY='{{ var.value.get('aurum_fuel_natgas_frequency', 'DAILY') }}'",
            "FUEL_TOPIC='{{ var.value.get('aurum_fuel_natgas_topic', 'aurum.ref.fuel.natural_gas.v1') }}'",
            "FUEL_UNITS='{{ var.value.get('aurum_fuel_natgas_units', 'USD/MMBtu') }}'",
            "FUEL_CURRENCY='{{ var.value.get('aurum_fuel_natgas_currency', 'USD') }}'",
            "FUEL_START='{{ data_interval_start | ds }}'",
            "FUEL_END='{{ data_interval_end | ds }}'",
            "FUEL_BENCHMARK_EXPR=\"'{{ var.value.get('aurum_fuel_natgas_benchmark', 'Henry Hub') }}'\"",
            "FUEL_REGION_EXPR=\"'{{ var.value.get('aurum_fuel_natgas_region', 'US_Gulf') }}'\"",
            "FUEL_FILTER_EXPR=\"{{ var.value.get('aurum_fuel_natgas_filter_expr', 'TRUE') }}\"",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/eia:api_key=EIA_API_KEY"],
        task_id_override="seatunnel_eia_fuel_ng",
    )

    fuel_co2_task = build_seatunnel_task(
        "eia_fuel_curve_to_kafka",
        [
            "FUEL_EIA_PATH='{{ var.value.get('aurum_fuel_co2_eia_path', 'environment/co2e/allowance-prices/data') }}'",
            "FUEL_SERIES_ID='{{ var.value.get('aurum_fuel_co2_series_id', 'EIA_CO2_RGGI') }}'",
            "FUEL_FUEL_TYPE='CO2'",
            "FUEL_FREQUENCY='{{ var.value.get('aurum_fuel_co2_frequency', 'MONTHLY') }}'",
            "FUEL_TOPIC='{{ var.value.get('aurum_fuel_co2_topic', 'aurum.ref.fuel.co2.v1') }}'",
            "FUEL_UNITS='{{ var.value.get('aurum_fuel_co2_units', 'USD/short_ton') }}'",
            "FUEL_CURRENCY='{{ var.value.get('aurum_fuel_co2_currency', 'USD') }}'",
            "FUEL_START='{{ data_interval_start | ds }}'",
            "FUEL_END='{{ data_interval_end | ds }}'",
            "FUEL_BENCHMARK_EXPR=\"'{{ var.value.get('aurum_fuel_co2_benchmark', 'RGGI') }}'\"",
            "FUEL_REGION_EXPR=\"'{{ var.value.get('aurum_fuel_co2_region', 'US_Northeast') }}'\"",
            "FUEL_FILTER_EXPR=\"{{ var.value.get('aurum_fuel_co2_filter_expr', 'TRUE') }}\"",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/eia:api_key=EIA_API_KEY"],
        task_id_override="seatunnel_eia_fuel_co2",
    )

    pjm_task = build_seatunnel_task(
        "pjm_lmp_to_kafka",
        [
            "PJM_TOPIC='{{ var.value.get('aurum_pjm_topic', 'aurum.iso.pjm.lmp.v1') }}'",
            "PJM_INTERVAL_START='{{ data_interval_start.in_timezone('America/New_York').isoformat() }}'",
            "PJM_INTERVAL_END='{{ data_interval_end.in_timezone('America/New_York').isoformat() }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/pjm:token=PJM_API_KEY"],
        pool="pjm_api",
    )

    pjm_load_task = build_seatunnel_task(
        "pjm_load_to_kafka",
        [
            "PJM_LOAD_ENDPOINT='{{ var.value.get('aurum_pjm_load_endpoint', 'https://api.pjm.com/api/v1/inst_load') }}'",
            "PJM_ROW_LIMIT='{{ var.value.get('aurum_pjm_row_limit', '10000') }}'",
            "PJM_INTERVAL_START='{{ data_interval_start.in_timezone('America/New_York').isoformat() }}'",
            "PJM_INTERVAL_END='{{ data_interval_end.in_timezone('America/New_York').isoformat() }}'",
            "PJM_LOAD_TOPIC='{{ var.value.get('aurum_pjm_load_topic', 'aurum.iso.pjm.load.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
        ],
        mappings=["secret/data/aurum/pjm:token=PJM_API_KEY"],
        pool="pjm_api",
    )

    pjm_genmix_task = build_seatunnel_task(
        "pjm_genmix_to_kafka",
        [
            "PJM_GENMIX_ENDPOINT='{{ var.value.get('aurum_pjm_genmix_endpoint', 'https://api.pjm.com/api/v1/gen_by_fuel') }}'",
            "PJM_ROW_LIMIT='{{ var.value.get('aurum_pjm_row_limit', '10000') }}'",
            "PJM_INTERVAL_START='{{ data_interval_start.in_timezone('America/New_York').isoformat() }}'",
            "PJM_INTERVAL_END='{{ data_interval_end.in_timezone('America/New_York').isoformat() }}'",
            "PJM_GENMIX_TOPIC='{{ var.value.get('aurum_pjm_genmix_topic', 'aurum.iso.pjm.genmix.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
        ],
        mappings=["secret/data/aurum/pjm:token=PJM_API_KEY"],
        pool="pjm_api",
    )

    noaa_watermark = PythonOperator(
        task_id="noaa_watermark",
        python_callable=lambda **ctx: _update_watermark("noaa_ghcnd", ctx["logical_date"]),
    )

    eia_watermark = PythonOperator(
        task_id="eia_watermark",
        python_callable=lambda **ctx: _update_watermark("eia_series", ctx["logical_date"]),
    )

    fred_watermark = PythonOperator(
        task_id="fred_watermark",
        python_callable=lambda **ctx: _update_watermark("fred_series", ctx["logical_date"]),
    )

    cpi_watermark = PythonOperator(
        task_id="cpi_watermark",
        python_callable=lambda **ctx: _update_watermark("cpi_series", ctx["logical_date"]),
    )

    fuel_natgas_watermark = PythonOperator(
        task_id="fuel_natgas_watermark",
        python_callable=lambda **ctx: _update_watermark("fuel_natgas_curve", ctx["logical_date"]),
    )

    fuel_co2_watermark = PythonOperator(
        task_id="fuel_co2_watermark",
        python_callable=lambda **ctx: _update_watermark("fuel_co2_curve", ctx["logical_date"]),
    )

    pjm_watermark = PythonOperator(
        task_id="pjm_watermark",
        python_callable=lambda **ctx: _update_watermark("pjm_lmp", ctx["logical_date"]),
    )

    pjm_load_watermark = PythonOperator(
        task_id="pjm_load_watermark",
        python_callable=lambda **ctx: _update_watermark("pjm_load", ctx["logical_date"]),
    )

    pjm_genmix_watermark = PythonOperator(
        task_id="pjm_genmix_watermark",
        python_callable=lambda **ctx: _update_watermark("pjm_genmix", ctx["logical_date"]),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> noaa_task >> noaa_to_timescale >> noaa_watermark
    register_sources >> eia_task >> eia_to_timescale >> eia_watermark
    dynamic_eia_watermarks: list[PythonOperator] = []
    for dataset_cfg, dynamic_task, dynamic_watermark in dynamic_eia_results:
        register_sources >> dynamic_task >> dynamic_watermark
        dynamic_eia_watermarks.append(dynamic_watermark)
    register_sources >> fred_task >> fred_to_timescale >> fred_watermark
    register_sources >> cpi_task >> cpi_to_timescale >> cpi_watermark
    register_sources >> fuel_natgas_task >> fuel_natgas_watermark
    register_sources >> fuel_co2_task >> fuel_co2_watermark
    register_sources >> pjm_task >> pjm_watermark
    register_sources >> pjm_load_task >> pjm_load_watermark
    register_sources >> pjm_genmix_task >> pjm_genmix_watermark
    [
        noaa_watermark,
        eia_watermark,
        *dynamic_eia_watermarks,
        fred_watermark,
        cpi_watermark,
        fuel_natgas_watermark,
        fuel_co2_watermark,
        pjm_load_watermark,
        pjm_genmix_watermark,
        pjm_watermark,
    ] >> end
