"""Tests for Airflow DAG factory."""

from __future__ import annotations

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from aurum.airflow_factory import (
    AirflowDagFactory,
    DatasetConfig,
    SeaTunnelTaskBuilder,
    VaultSecretBuilder,
    WatermarkBuilder,
    TaskGroupBuilder
)


class TestDatasetConfig:
    def test_from_dict_minimal(self) -> None:
        """Test DatasetConfig creation from minimal dictionary."""
        data = {
            "source_name": "test_dataset",
            "schedule": "0 6 * * *",
            "topic_var": "test_topic_var",
            "default_topic": "test.topic.v1",
            "frequency": "DAILY"
        }

        config = DatasetConfig.from_dict(data)

        assert config.source_name == "test_dataset"
        assert config.schedule == "0 6 * * *"
        assert config.topic_var == "test_topic_var"
        assert config.default_topic == "test.topic.v1"
        assert config.frequency == "DAILY"
        assert config.param_overrides == []

    def test_from_dict_complete(self) -> None:
        """Test DatasetConfig creation from complete dictionary."""
        data = {
            "source_name": "eia_test",
            "path": "test/path",
            "data_path": "test/data/path",
            "description": "Test dataset",
            "schedule": "0 8 * * *",
            "topic_var": "test_topic_var",
            "default_topic": "test.topic.v1",
            "series_id": "TEST123",
            "series_id_expr": "series_id",
            "frequency": "MONTHLY",
            "units_var": "test_units_var",
            "default_units": "TEST_UNITS",
            "canonical_currency": "USD",
            "canonical_unit": "MWh",
            "unit_conversion": "1.0",
            "area_expr": "area",
            "sector_expr": "sector",
            "description_expr": "description",
            "source_expr": "source",
            "dataset_expr": "dataset",
            "metadata_expr": "metadata",
            "filter_expr": "filter_condition",
            "param_overrides": [{"test": "value"}],
            "period_column": "period",
            "date_format": "YYYY-MM-DD",
            "page_limit": 1000,
            "window_hours": 24,
            "window_days": 1,
            "window_months": 1,
            "window_years": None,
            "dlq_topic": "test.dlq.v1",
            "series_id_strategy": {"source": "test"},
            "watermark_policy": "day"
        }

        config = DatasetConfig.from_dict(data)

        assert config.source_name == "eia_test"
        assert config.path == "test/path"
        assert config.data_path == "test/data/path"
        assert config.description == "Test dataset"
        assert config.schedule == "0 8 * * *"
        assert config.topic_var == "test_topic_var"
        assert config.default_topic == "test.topic.v1"
        assert config.series_id == "TEST123"
        assert config.series_id_expr == "series_id"
        assert config.frequency == "MONTHLY"
        assert config.units_var == "test_units_var"
        assert config.default_units == "TEST_UNITS"
        assert config.canonical_currency == "USD"
        assert config.canonical_unit == "MWh"
        assert config.unit_conversion == "1.0"
        assert config.area_expr == "area"
        assert config.sector_expr == "sector"
        assert config.description_expr == "description"
        assert config.source_expr == "source"
        assert config.dataset_expr == "dataset"
        assert config.metadata_expr == "metadata"
        assert config.filter_expr == "filter_condition"
        assert config.param_overrides == [{"test": "value"}]
        assert config.period_column == "period"
        assert config.date_format == "YYYY-MM-DD"
        assert config.page_limit == 1000
        assert config.window_hours == 24
        assert config.window_days == 1
        assert config.window_months == 1
        assert config.window_years is None
        assert config.dlq_topic == "test.dlq.v1"
        assert config.series_id_strategy == {"source": "test"}
        assert config.watermark_policy == "day"


class TestSeaTunnelTaskBuilder:
    def test_build_eia_env(self) -> None:
        """Test EIA environment variable building."""
        builder = SeaTunnelTaskBuilder()

        config = DatasetConfig(
            source_name="eia_test",
            path="test/path",
            data_path="test/data/path",
            series_id="TEST123",
            series_id_expr="series_id",
            frequency="DAILY",
            default_units="MWh",
            area_expr="area",
            sector_expr="sector",
            description_expr="description",
            source_expr="source",
            dataset_expr="dataset",
            metadata_expr="metadata",
            filter_expr="filter_condition",
            param_overrides=[{"test": "value"}],
            page_limit=1000,
            window_hours=24,
            window_days=1,
            dlq_topic="test.dlq.v1",
            watermark_policy="day"
        )

        env_vars = builder.build_eia_env(config)

        assert "EIA_SERIES_PATH='test/data/path'" in env_vars
        assert "EIA_SERIES_ID='TEST123'" in env_vars
        assert "EIA_FREQUENCY='DAILY'" in env_vars
        assert "EIA_WINDOW_HOURS=24" in env_vars
        assert "EIA_WINDOW_DAYS=1" in env_vars

    def test_build_fred_env(self) -> None:
        """Test FRED environment variable building."""
        builder = SeaTunnelTaskBuilder()

        config = DatasetConfig(
            source_name="fred_test",
            series_id="DGS10",
            frequency="DAILY",
            default_units="Percent",
            watermark_policy="day"
        )

        env_vars = builder.build_fred_env(config)

        assert "FRED_SERIES_ID='DGS10'" in env_vars
        assert "FRED_FREQUENCY='DAILY'" in env_vars
        assert "FRED_UNITS='Percent'" in env_vars

    def test_build_cpi_env(self) -> None:
        """Test CPI environment variable building."""
        builder = SeaTunnelTaskBuilder()

        config = DatasetConfig(
            source_name="cpi_test",
            series_id="CPIAUCSL",
            frequency="MONTHLY",
            default_units="Index 1982-1984=100",
            area="US_CITY_AVERAGE",
            watermark_policy="month"
        )

        env_vars = builder.build_cpi_env(config)

        assert "CPI_SERIES_ID='CPIAUCSL'" in env_vars
        assert "CPI_FREQUENCY='MONTHLY'" in env_vars
        assert "CPI_AREA='US_CITY_AVERAGE'" in env_vars
        assert "CPI_UNITS='Index 1982-1984=100'" in env_vars

    def test_build_noaa_env(self) -> None:
        """Test NOAA environment variable building."""
        builder = SeaTunnelTaskBuilder()

        config = DatasetConfig(
            source_name="noaa_test",
            page_limit=2000,
            default_units="metric",
            watermark_policy="day"
        )

        env_vars = builder.build_noaa_env(config)

        assert "NOAA_GHCND_LIMIT=2000" in env_vars
        assert "NOAA_GHCND_UNITS='metric'" in env_vars


class TestVaultSecretBuilder:
    def test_build_eia_mappings(self) -> None:
        """Test EIA Vault mappings."""
        builder = VaultSecretBuilder()
        mappings = builder.build_eia_mappings()

        assert mappings == ["secret/data/aurum/eia:api_key=EIA_API_KEY"]

    def test_build_fred_mappings(self) -> None:
        """Test FRED Vault mappings."""
        builder = VaultSecretBuilder()
        mappings = builder.build_fred_mappings()

        assert mappings == ["secret/data/aurum/fred:api_key=FRED_API_KEY"]

    def test_build_cpi_mappings(self) -> None:
        """Test CPI Vault mappings."""
        builder = VaultSecretBuilder()
        mappings = builder.build_cpi_mappings()

        assert mappings == ["secret/data/aurum/fred:api_key=FRED_API_KEY"]

    def test_build_noaa_mappings(self) -> None:
        """Test NOAA Vault mappings."""
        builder = VaultSecretBuilder()
        mappings = builder.build_noaa_mappings()

        assert mappings == ["secret/data/aurum/noaa:token=NOAA_GHC_TOKEN"]

    def test_build_generic_mappings(self) -> None:
        """Test generic Vault mappings."""
        builder = VaultSecretBuilder()

        # Test EIA source name
        mappings = builder.build_generic_mappings("eia_test_source")
        assert mappings == ["secret/data/aurum/eia:api_key=EIA_API_KEY"]

        # Test FRED source name
        mappings = builder.build_generic_mappings("fred_test_source")
        assert mappings == ["secret/data/aurum/fred:api_key=FRED_API_KEY"]

        # Test unknown source
        mappings = builder.build_generic_mappings("unknown_source")
        assert mappings == []


class TestWatermarkBuilder:
    def test_create_watermark_task(self) -> None:
        """Test watermark task creation."""
        builder = WatermarkBuilder()

        with patch('aurum.airflow_factory.builders.metrics') as mock_metrics:
            with patch('aurum.airflow_factory.builders.update_ingest_watermark') as mock_update:
                task = builder.create_watermark_task(
                    source_name="test_source",
                    policy="day",
                    task_id="test_watermark"
                )

                assert task.task_id == "test_watermark"

                # Test task execution
                context = {"logical_date": "2024-01-01T00:00:00Z"}
                task.python_callable(**context)

                mock_update.assert_called_once_with("test_source", "logical_date", "2024-01-01T00:00:00Z", policy="day")
                mock_metrics.record_watermark_success.assert_called_once()


class TestAirflowDagFactory:
    def test_init(self) -> None:
        """Test AirflowDagFactory initialization."""
        factory = AirflowDagFactory(dag_id="test_dag")

        assert factory.dag_id == "test_dag"
        assert factory.schedule_interval == "0 6 * * *"
        assert factory.catchup is False
        assert factory.max_active_runs == 1

    @patch('aurum.airflow_factory.factory.Path.exists')
    def test_load_configs_file_not_found(self, mock_exists) -> None:
        """Test configuration loading with missing file."""
        mock_exists.return_value = False

        factory = AirflowDagFactory()

        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            factory._load_configs("nonexistent.json")

    def test_group_configs_by_source(self) -> None:
        """Test configuration grouping by source type."""
        factory = AirflowDagFactory()

        configs = [
            DatasetConfig(source_name="eia_test1", frequency="DAILY"),
            DatasetConfig(source_name="eia_test2", frequency="MONTHLY"),
            DatasetConfig(source_name="fred_test", frequency="DAILY"),
            DatasetConfig(source_name="cpi_test", frequency="MONTHLY"),
        ]

        grouped = factory._group_configs_by_source(configs)

        assert len(grouped) == 3
        assert "eia" in grouped
        assert "fred" in grouped
        assert "cpi" in grouped
        assert len(grouped["eia"]) == 2
        assert len(grouped["fred"]) == 1
        assert len(grouped["cpi"]) == 1

    def test_get_source_type(self) -> None:
        """Test source type detection."""
        factory = AirflowDagFactory()

        eia_config = DatasetConfig(source_name="eia_test")
        fred_config = DatasetConfig(source_name="fred_test")
        cpi_config = DatasetConfig(source_name="cpi_test")
        noaa_config = DatasetConfig(source_name="noaa_test")
        other_config = DatasetConfig(source_name="other_test")

        assert factory._get_source_type(eia_config) == "eia"
        assert factory._get_source_type(fred_config) == "fred"
        assert factory._get_source_type(cpi_config) == "cpi"
        assert factory._get_source_type(noaa_config) == "noaa"
        assert factory._get_source_type(other_config) == "other"


class TestAirflowFactoryIntegration:
    @patch('aurum.airflow_factory.factory.Path.exists')
    def test_create_dag_from_configs(self, mock_exists) -> None:
        """Test complete DAG creation from configuration files."""
        # Mock configuration file
        mock_exists.return_value = True

        config_data = {
            "datasets": [
                {
                    "source_name": "eia_test",
                    "path": "test/path",
                    "schedule": "0 6 * * *",
                    "topic_var": "test_topic_var",
                    "default_topic": "test.topic.v1",
                    "frequency": "DAILY",
                    "default_units": "MWh",
                    "watermark_policy": "day"
                },
                {
                    "source_name": "fred_test",
                    "series_id": "DGS10",
                    "schedule": "0 6 * * *",
                    "topic_var": "fred_topic_var",
                    "default_topic": "fred.topic.v1",
                    "frequency": "DAILY",
                    "default_units": "Percent",
                    "watermark_policy": "day"
                }
            ]
        }

        with patch('aurum.airflow_factory.factory.open') as mock_open:
            mock_file = MagicMock()
            mock_file.read.return_value = json.dumps(config_data)
            mock_open.return_value.__enter__.return_value = mock_file

            factory = AirflowDagFactory(dag_id="test_dag")
            dag = factory.create_dag(["test_config.json"])

            assert dag.dag_id == "test_dag"
            assert dag.description == "Dynamic data ingestion DAG generated from configuration"
            assert dag.schedule_interval == "0 6 * * *"
            assert dag.catchup is False
            assert dag.max_active_runs == 1

    @patch('aurum.airflow_factory.factory.Path.exists')
    def test_create_dag_invalid_config(self, mock_exists) -> None:
        """Test DAG creation with invalid configuration."""
        mock_exists.return_value = True

        # Invalid JSON
        with patch('aurum.airflow_factory.factory.open') as mock_open:
            mock_file = MagicMock()
            mock_file.read.return_value = "invalid json"
            mock_open.return_value.__enter__.return_value = mock_file

            factory = AirflowDagFactory()

            with pytest.raises(RuntimeError, match="Failed to parse"):
                factory._load_configs("invalid.json")

        # Missing datasets field
        with patch('aurum.airflow_factory.factory.open') as mock_open:
            mock_file = MagicMock()
            mock_file.read.return_value = '{"other_field": []}'
            mock_open.return_value.__enter__.return_value = mock_file

            factory = AirflowDagFactory()

            with pytest.raises(RuntimeError, match="Expected 'datasets' array"):
                factory._load_configs("invalid.json")

    def test_create_eia_tasks(self) -> None:
        """Test EIA task creation."""
        factory = AirflowDagFactory()

        config = DatasetConfig(
            source_name="eia_test",
            path="test/path",
            frequency="DAILY",
            default_units="MWh",
            watermark_policy="day"
        )

        with patch.object(factory.task_builder, 'build_eia_env') as mock_build_env:
            with patch.object(factory.secret_builder, 'build_eia_mappings') as mock_build_mappings:
                with patch.object(factory.task_builder, 'create_seatunnel_task') as mock_create_task:
                    with patch.object(factory.watermark_builder, 'create_watermark_task') as mock_create_watermark:

                        mock_build_env.return_value = ["EIA_TEST=test"]
                        mock_build_mappings.return_value = ["secret/data/aurum/eia:api_key=EIA_API_KEY"]
                        mock_create_task.return_value = MagicMock()
                        mock_create_watermark.return_value = MagicMock()

                        tasks = factory._create_eia_tasks(config)

                        assert len(tasks) == 2
                        mock_build_env.assert_called_once_with(config)
                        mock_build_mappings.assert_called_once()
                        mock_create_task.assert_called_once()
                        mock_create_watermark.assert_called_once_with(
                            source_name="eia_test",
                            policy="day",
                            task_id="eia_test_watermark"
                        )

    def test_create_fred_tasks(self) -> None:
        """Test FRED task creation."""
        factory = AirflowDagFactory()

        config = DatasetConfig(
            source_name="fred_test",
            series_id="DGS10",
            frequency="DAILY",
            default_units="Percent",
            watermark_policy="day"
        )

        with patch.object(factory.task_builder, 'build_fred_env') as mock_build_env:
            with patch.object(factory.secret_builder, 'build_fred_mappings') as mock_build_mappings:
                with patch.object(factory.task_builder, 'create_seatunnel_task') as mock_create_task:
                    with patch.object(factory.watermark_builder, 'create_watermark_task') as mock_create_watermark:

                        mock_build_env.return_value = ["FRED_TEST=test"]
                        mock_build_mappings.return_value = ["secret/data/aurum/fred:api_key=FRED_API_KEY"]
                        mock_create_task.return_value = MagicMock()
                        mock_create_watermark.return_value = MagicMock()

                        tasks = factory._create_fred_tasks(config)

                        assert len(tasks) == 2
                        mock_build_env.assert_called_once_with(config)
                        mock_build_mappings.assert_called_once()
                        mock_create_task.assert_called_once()
                        mock_create_watermark.assert_called_once_with(
                            source_name="fred_test",
                            policy="day",
                            task_id="fred_test_watermark"
                        )

    def test_create_cpi_tasks(self) -> None:
        """Test CPI task creation."""
        factory = AirflowDagFactory()

        config = DatasetConfig(
            source_name="cpi_test",
            series_id="CPIAUCSL",
            frequency="MONTHLY",
            default_units="Index 1982-1984=100",
            watermark_policy="month"
        )

        with patch.object(factory.task_builder, 'build_cpi_env') as mock_build_env:
            with patch.object(factory.secret_builder, 'build_fred_mappings') as mock_build_mappings:
                with patch.object(factory.task_builder, 'create_seatunnel_task') as mock_create_task:
                    with patch.object(factory.watermark_builder, 'create_watermark_task') as mock_create_watermark:

                        mock_build_env.return_value = ["CPI_TEST=test"]
                        mock_build_mappings.return_value = ["secret/data/aurum/fred:api_key=FRED_API_KEY"]
                        mock_create_task.return_value = MagicMock()
                        mock_create_watermark.return_value = MagicMock()

                        tasks = factory._create_cpi_tasks(config)

                        assert len(tasks) == 2
                        mock_build_env.assert_called_once_with(config)
                        mock_build_mappings.assert_called_once()
                        mock_create_task.assert_called_once()
                        mock_create_watermark.assert_called_once_with(
                            source_name="cpi_test",
                            policy="month",
                            task_id="cpi_test_watermark"
                        )

    def test_create_noaa_tasks(self) -> None:
        """Test NOAA task creation."""
        factory = AirflowDagFactory()

        config = DatasetConfig(
            source_name="noaa_test",
            page_limit=1000,
            watermark_policy="day"
        )

        with patch.object(factory.task_builder, 'build_noaa_env') as mock_build_env:
            with patch.object(factory.secret_builder, 'build_noaa_mappings') as mock_build_mappings:
                with patch.object(factory.task_builder, 'create_seatunnel_task') as mock_create_task:
                    with patch.object(factory.watermark_builder, 'create_watermark_task') as mock_create_watermark:

                        mock_build_env.return_value = ["NOAA_TEST=test"]
                        mock_build_mappings.return_value = ["secret/data/aurum/noaa:token=NOAA_GHC_TOKEN"]
                        mock_create_task.return_value = MagicMock()
                        mock_create_watermark.return_value = MagicMock()

                        tasks = factory._create_noaa_tasks(config)

                        assert len(tasks) == 2
                        mock_build_env.assert_called_once_with(config)
                        mock_build_mappings.assert_called_once()
                        mock_create_task.assert_called_once()
                        mock_create_watermark.assert_called_once_with(
                            source_name="noaa_test",
                            policy="day",
                            task_id="noaa_test_watermark"
                        )
