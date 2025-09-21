"""Builder classes for Airflow DAG components."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from aurum.airflow_utils import build_failure_callback, metrics


@dataclass
class DatasetConfig:
    """Dataset configuration for dynamic DAG generation."""

    source_name: str
    path: Optional[str] = None
    data_path: Optional[str] = None
    description: Optional[str] = None
    schedule: str = "0 6 * * *"
    topic_var: Optional[str] = None
    default_topic: Optional[str] = None
    series_id: Optional[str] = None
    series_id_expr: Optional[str] = None
    frequency: Optional[str] = None
    units_var: Optional[str] = None
    default_units: Optional[str] = None
    canonical_currency: Optional[str] = None
    canonical_unit: Optional[str] = None
    unit_conversion: Optional[str] = None
    area_expr: Optional[str] = None
    sector_expr: Optional[str] = None
    description_expr: Optional[str] = None
    source_expr: Optional[str] = None
    dataset_expr: Optional[str] = None
    metadata_expr: Optional[str] = None
    filter_expr: Optional[str] = None
    param_overrides: List[Dict[str, Any]] = None
    period_column: Optional[str] = None
    date_format: Optional[str] = None
    page_limit: int = 5000
    window_hours: Optional[int] = None
    window_days: Optional[int] = None
    window_months: Optional[int] = None
    window_years: Optional[int] = None
    dlq_topic: str = "aurum.ref.generic.dlq.v1"
    series_id_strategy: Optional[Dict[str, Any]] = None
    watermark_policy: str = "exact"

    def __post_init__(self):
        if self.param_overrides is None:
            self.param_overrides = []

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetConfig":
        """Create DatasetConfig from dictionary."""
        return cls(
            source_name=data["source_name"],
            path=data.get("path"),
            data_path=data.get("data_path"),
            description=data.get("description"),
            schedule=data.get("schedule", "0 6 * * *"),
            topic_var=data.get("topic_var"),
            default_topic=data.get("default_topic"),
            series_id=data.get("series_id"),
            series_id_expr=data.get("series_id_expr"),
            frequency=data.get("frequency"),
            units_var=data.get("units_var"),
            default_units=data.get("default_units"),
            canonical_currency=data.get("canonical_currency"),
            canonical_unit=data.get("canonical_unit"),
            unit_conversion=data.get("unit_conversion"),
            area_expr=data.get("area_expr"),
            sector_expr=data.get("sector_expr"),
            description_expr=data.get("description_expr"),
            source_expr=data.get("source_expr"),
            dataset_expr=data.get("dataset_expr"),
            metadata_expr=data.get("metadata_expr"),
            filter_expr=data.get("filter_expr"),
            param_overrides=data.get("param_overrides", []),
            period_column=data.get("period_column"),
            date_format=data.get("date_format"),
            page_limit=data.get("page_limit", 5000),
            window_hours=data.get("window_hours"),
            window_days=data.get("window_days"),
            window_months=data.get("window_months"),
            window_years=data.get("window_years"),
            dlq_topic=data.get("dlq_topic", "aurum.ref.generic.dlq.v1"),
            series_id_strategy=data.get("series_id_strategy"),
            watermark_policy=data.get("watermark_policy", "exact")
        )


class SeaTunnelTaskBuilder:
    """Builder for SeaTunnel tasks."""

    def __init__(self):
        self.bin_path = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
        self.pythonpath_entry = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        self.vault_addr = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
        self.vault_token = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")

    def build_eia_env(self, config: DatasetConfig) -> List[str]:
        """Build environment variables for EIA dataset."""
        assignments = {}

        def set_env(key: str, value: str, *, quote: bool = True) -> None:
            assignments[key] = (value, quote)

        def ensure_env(key: str, value: str, *, quote: bool = True) -> None:
            if key not in assignments:
                set_env(key, value, quote=quote)

        # Core EIA variables
        series_path = config.data_path or config.path
        set_env("EIA_SERIES_PATH", series_path)
        set_env("EIA_SERIES_ID", config.series_id or "MULTI_SERIES")
        set_env("EIA_SERIES_ID_EXPR", config.series_id_expr or "series_id", quote=False)
        set_env("EIA_FREQUENCY", config.frequency or "OTHER")
        set_env("EIA_SEASONAL_ADJUSTMENT", config.get("seasonal_adjustment", "UNKNOWN"))
        set_env("EIA_AREA_EXPR", config.area_expr or "CAST(NULL AS STRING)")
        set_env("EIA_SECTOR_EXPR", config.sector_expr or "CAST(NULL AS STRING)")
        set_env("EIA_DESCRIPTION_EXPR", config.description_expr or "CAST(NULL AS STRING)")
        set_env("EIA_SOURCE_EXPR", config.source_expr or "COALESCE(source, 'EIA')")
        set_env("EIA_DATASET_EXPR", config.dataset_expr or "COALESCE(dataset, '')")
        set_env("EIA_METADATA_EXPR", config.metadata_expr or "NULL")
        set_env("EIA_FILTER_EXPR", config.filter_expr or "TRUE", quote=False)
        set_env("EIA_LIMIT", str(config.page_limit), quote=False)
        set_env("EIA_DLQ_TOPIC", config.dlq_topic)
        set_env("EIA_DLQ_SUBJECT", f"{config.dlq_topic}-value")

        # Windowing
        set_env("EIA_WINDOW_END", "{{ data_interval_end.in_timezone('UTC').isoformat() }}")
        if config.window_hours is not None:
            set_env("EIA_WINDOW_HOURS", str(config.window_hours), quote=False)
        if config.window_days is not None:
            set_env("EIA_WINDOW_DAYS", str(config.window_days), quote=False)
        if config.window_months is not None:
            set_env("EIA_WINDOW_MONTHS", str(config.window_months), quote=False)
        if config.window_years is not None:
            set_env("EIA_WINDOW_YEARS", str(config.window_years), quote=False)

        # Parameter overrides
        overrides_json = json.dumps(config.param_overrides)
        set_env("EIA_PARAM_OVERRIDES_JSON", overrides_json)

        # Airflow variable integration
        if config.topic_var:
            set_env("EIA_TOPIC", f"{{{{ var.value.get('{config.topic_var}', '{config.default_topic}') }}}}")
        if config.units_var:
            set_env("EIA_UNITS", f"{{{{ var.value.get('{config.units_var}', '{config.default_units}') }}}}")

        return self._format_assignments(assignments)

    def build_fred_env(self, config: DatasetConfig) -> List[str]:
        """Build environment variables for FRED dataset."""
        assignments = {}

        def set_env(key: str, value: str, *, quote: bool = True) -> None:
            assignments[key] = (value, quote)

        # Core FRED variables
        set_env("FRED_SERIES_ID", config.series_id or "DGS10")
        set_env("FRED_FREQUENCY", config.frequency or "DAILY")
        set_env("FRED_SEASONAL_ADJ", config.get("seasonal_adjustment", "NSA"))
        set_env("FRED_START_DATE", "{{ data_interval_start | ds }}")
        set_env("FRED_END_DATE", "{{ data_interval_start | ds }}")
        set_env("FRED_UNITS", config.default_units or "Percent")
        set_env("FRED_SOURCE", "FRED")

        # Windowing
        if config.window_hours is not None:
            set_env("FRED_WINDOW_HOURS", str(config.window_hours), quote=False)
        if config.window_days is not None:
            set_env("FRED_WINDOW_DAYS", str(config.window_days), quote=False)
        if config.window_months is not None:
            set_env("FRED_WINDOW_MONTHS", str(config.window_months), quote=False)
        if config.window_years is not None:
            set_env("FRED_WINDOW_YEARS", str(config.window_years), quote=False)

        # Airflow variable integration
        if config.topic_var:
            set_env("FRED_TOPIC", f"{{{{ var.value.get('{config.topic_var}', '{config.default_topic}') }}}}")
        if config.units_var:
            set_env("FRED_UNITS", f"{{{{ var.value.get('{config.units_var}', '{config.default_units}') }}}}")

        return self._format_assignments(assignments)

    def build_cpi_env(self, config: DatasetConfig) -> List[str]:
        """Build environment variables for CPI dataset."""
        assignments = {}

        def set_env(key: str, value: str, *, quote: bool = True) -> None:
            assignments[key] = (value, quote)

        # Core CPI variables
        set_env("CPI_SERIES_ID", config.series_id or "CPIAUCSL")
        set_env("CPI_FREQUENCY", config.frequency or "MONTHLY")
        set_env("CPI_SEASONAL_ADJ", config.get("seasonal_adjustment", "SA"))
        set_env("CPI_START_DATE", "{{ data_interval_start | ds }}")
        set_env("CPI_END_DATE", "{{ data_interval_start | ds }}")
        set_env("CPI_AREA", config.get("area", "US_CITY_AVERAGE"))
        set_env("CPI_UNITS", config.default_units or "Index 1982-1984=100")
        set_env("CPI_SOURCE", "FRED")

        # Windowing
        if config.window_hours is not None:
            set_env("CPI_WINDOW_HOURS", str(config.window_hours), quote=False)
        if config.window_days is not None:
            set_env("CPI_WINDOW_DAYS", str(config.window_days), quote=False)
        if config.window_months is not None:
            set_env("CPI_WINDOW_MONTHS", str(config.window_months), quote=False)
        if config.window_years is not None:
            set_env("CPI_WINDOW_YEARS", str(config.window_years), quote=False)

        # Airflow variable integration
        if config.topic_var:
            set_env("CPI_TOPIC", f"{{{{ var.value.get('{config.topic_var}', '{config.default_topic}') }}}}")
        if config.units_var:
            set_env("CPI_UNITS", f"{{{{ var.value.get('{config.units_var}', '{config.default_units}') }}}}")

        return self._format_assignments(assignments)

    def build_noaa_env(self, config: DatasetConfig) -> List[str]:
        """Build environment variables for NOAA dataset."""
        assignments = {}

        def set_env(key: str, value: str, *, quote: bool = True) -> None:
            assignments[key] = (value, quote)

        # Core NOAA variables
        set_env("NOAA_GHCND_START_DATE", "{{ data_interval_start | ds }}")
        set_env("NOAA_GHCND_END_DATE", "{{ data_interval_start | ds }}")
        set_env("NOAA_GHCND_DATASET", "GHCND")
        set_env("NOAA_GHCND_LIMIT", str(config.page_limit), quote=False)
        set_env("NOAA_GHCND_STATION_LIMIT", "1000")
        set_env("NOAA_GHCND_UNIT_CODE", config.get("unit_code", "unknown"))
        set_env("NOAA_GHCND_UNITS", config.get("units", "metric"))

        # Windowing
        if config.window_hours is not None:
            set_env("NOAA_GHCND_SLIDING_HOURS", str(config.window_hours), quote=False)
        if config.window_days is not None:
            set_env("NOAA_GHCND_SLIDING_DAYS", str(config.window_days), quote=False)

        # Airflow variable integration
        if config.topic_var:
            set_env("NOAA_GHCND_TOPIC", f"{{{{ var.value.get('{config.topic_var}', '{config.default_topic}') }}}}")
        if config.units_var:
            set_env("NOAA_GHCND_UNITS", f"{{{{ var.value.get('{config.units_var}', '{config.default_units}') }}}}")

        return self._format_assignments(assignments)

    def build_generic_env(self, config: DatasetConfig) -> List[str]:
        """Build environment variables for generic dataset."""
        assignments = {}

        def set_env(key: str, value: str, *, quote: bool = True) -> None:
            assignments[key] = (value, quote)

        # Basic configuration
        set_env("SOURCE_NAME", config.source_name)
        set_env("SOURCE_DESCRIPTION", config.description or config.source_name)

        # Windowing
        if config.window_hours is not None:
            set_env("WINDOW_HOURS", str(config.window_hours), quote=False)
        if config.window_days is not None:
            set_env("WINDOW_DAYS", str(config.window_days), quote=False)
        if config.window_months is not None:
            set_env("WINDOW_MONTHS", str(config.window_months), quote=False)
        if config.window_years is not None:
            set_env("WINDOW_YEARS", str(config.window_years), quote=False)

        return self._format_assignments(assignments)

    def _format_assignments(self, assignments: Dict[str, tuple[str, bool]]) -> List[str]:
        """Format environment variable assignments."""
        formatted = []
        for key, (value, quote) in assignments.items():
            if quote:
                formatted.append(f"{key}='{value}'")
            else:
                formatted.append(f"{key}={value}")
        return formatted

    def create_seatunnel_task(
        self,
        job_name: str,
        env_assignments: List[str],
        mappings: List[str],
        *,
        task_id_override: Optional[str] = None,
        pool: str = "default"
    ) -> BashOperator:
        """Create a SeaTunnel task."""
        mapping_flags = ""
        if mappings:
            mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)

        pull_cmd = ""
        if mapping_flags:
            pull_cmd = (
                f"eval \"$(VAULT_ADDR={self.vault_addr} VAULT_TOKEN={self.vault_token} "
                f"PYTHONPATH={self.pythonpath_entry}:${{PYTHONPATH:-}} "
                f"{self.venv_python} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true\n"
            )

        # Ensure KAFKA bootstrap is always present for render-only validation
        env_all = list(env_assignments) + [
            "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'"
        ]
        env_line = " ".join(env_all)

        operator_kwargs = {
            "task_id": task_id_override or f"seatunnel_{job_name}",
            "bash_command": (
                "set -euo pipefail\n"
                "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
                "cd /opt/airflow\n"
                f"{pull_cmd}"
                "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe {job_name}; fi\n"
                "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then env | grep -E 'DLQ_TOPIC|DLQ_SUBJECT' || true; fi\n"
                f"export PATH=\"{self.bin_path}\"\n"
                f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{self.pythonpath_entry}\"\n"
                # Render-only in Airflow pods without Docker
                f"AURUM_EXECUTE_SEATUNNEL=0 {env_line} scripts/seatunnel/run_job.sh {job_name} --render-only"
            ),
            "execution_timeout": timedelta(minutes=25),
        }

        if pool != "default":
            operator_kwargs["pool"] = pool

        return BashOperator(**operator_kwargs)

    @property
    def venv_python(self) -> str:
        """Get the Python executable path."""
        return os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")


class VaultSecretBuilder:
    """Builder for Vault secret mappings."""

    def build_eia_mappings(self) -> List[str]:
        """Build Vault mappings for EIA."""
        return ["secret/data/aurum/eia:api_key=EIA_API_KEY"]

    def build_fred_mappings(self) -> List[str]:
        """Build Vault mappings for FRED."""
        return ["secret/data/aurum/fred:api_key=FRED_API_KEY"]

    def build_cpi_mappings(self) -> List[str]:
        """Build Vault mappings for CPI."""
        return ["secret/data/aurum/fred:api_key=FRED_API_KEY"]  # CPI uses FRED API key

    def build_noaa_mappings(self) -> List[str]:
        """Build Vault mappings for NOAA."""
        return ["secret/data/aurum/noaa:token=NOAA_GHC_TOKEN"]

    def build_generic_mappings(self, source_name: str) -> List[str]:
        """Build generic Vault mappings."""
        # Try to infer mappings from source name
        if "eia" in source_name.lower():
            return self.build_eia_mappings()
        elif "fred" in source_name.lower():
            return self.build_fred_mappings()
        elif "cpi" in source_name.lower():
            return self.build_cpi_mappings()
        elif "noaa" in source_name.lower():
            return self.build_noaa_mappings()
        else:
            return []  # No mappings for unknown sources


class WatermarkBuilder:
    """Builder for watermark tasks."""

    def create_watermark_task(
        self,
        source_name: str,
        policy: str = "exact",
        task_id: str = "watermark"
    ) -> PythonOperator:
        """Create a watermark update task."""
        def _update_watermark(**context):
            logical_date = context["logical_date"]
            try:
                from aurum.db import update_ingest_watermark
                update_ingest_watermark(source_name, "logical_date", logical_date, policy=policy)
                metrics.record_watermark_success(source_name, logical_date)
            except Exception as e:
                print(f"Failed to update watermark for {source_name}: {e}")
                metrics.record_watermark_failure(source_name, logical_date, str(e))
                raise

        return PythonOperator(
            task_id=task_id,
            python_callable=_update_watermark
        )


class TaskGroupBuilder:
    """Builder for task groups."""

    def create_source_group(
        self,
        source_type: str,
        tasks: List[Any],
        prefix_group_id: bool = False
    ) -> TaskGroup:
        """Create a task group for a source type."""
        group_id = f"ingest_{source_type}"

        with TaskGroup(group_id=group_id, prefix_group_id=prefix_group_id) as group:
            for task in tasks:
                # Tasks are already created and should be referenced here
                pass

        return group
