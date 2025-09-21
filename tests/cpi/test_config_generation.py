"""Tests for CPI configuration generation utilities."""

from __future__ import annotations

import json
import pytest
from pathlib import Path

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from scripts.cpi.generate_ingest_config import (
    CpiSeries,
    GeneratedDataset,
    _build_generated_entry,
    _frequency_to_label,
    _frequency_to_schedule,
    _derive_watermark_policy,
    _extract_area_from_title,
    _load_cpi_catalog,
    _validate_config,
    _write_schema_file,
)


class TestCPIConfigGeneration:
    def test_frequency_to_schedule(self) -> None:
        """Test frequency to schedule conversion."""
        assert _frequency_to_schedule("Daily") == "0 6 * * *"
        assert _frequency_to_schedule("Weekly") == "0 6 * * 1"
        assert _frequency_to_schedule("Monthly") == "0 6 1 * *"
        assert _frequency_to_schedule("Quarterly") == "0 6 1 */3 *"
        assert _frequency_to_schedule("Annual") == "0 6 1 1 *"
        assert _frequency_to_schedule("Unknown") == "0 6 * * *"

    def test_frequency_to_label(self) -> None:
        """Test frequency to label conversion."""
        assert _frequency_to_label("Daily") == "DAILY"
        assert _frequency_to_label("Weekly") == "WEEKLY"
        assert _frequency_to_label("Monthly") == "MONTHLY"
        assert _frequency_to_label("Quarterly") == "QUARTERLY"
        assert _frequency_to_label("Annual") == "ANNUAL"
        assert _frequency_to_label("Unknown") == "OTHER"

    def test_derive_watermark_policy(self) -> None:
        """Test watermark policy derivation."""
        assert _derive_watermark_policy("Daily") == "day"
        assert _derive_watermark_policy("Weekly") == "week"
        assert _derive_watermark_policy("Monthly") == "month"
        assert _derive_watermark_policy("Quarterly") == "month"
        assert _derive_watermark_policy("Annual") == "month"
        assert _derive_watermark_policy("Unknown") == "exact"

    def test_extract_area_from_title(self) -> None:
        """Test area extraction from CPI series title."""
        assert _extract_area_from_title("Consumer Price Index for All Urban Consumers: All Items in U.S. City Average") == "US_CITY_AVERAGE"
        assert _extract_area_from_title("Consumer Price Index: Total All Items for the United States") == "US"
        assert _extract_area_from_title("Consumer Price Index: Total All Items for the European Union") == "UNKNOWN"

    def test_build_generated_entry_monthly(self) -> None:
        """Test building generated entry for monthly CPI series."""
        series = CpiSeries(
            id="CPIAUCSL",
            title="Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
            units="Index 1982-1984=100",
            frequency="Monthly",
            seasonal_adjustment="Seasonally Adjusted",
            last_updated="2024-01-11",
            popularity=89,
            notes="CPI data",
            category="Prices",
            start_date="1947-01-01",
            end_date="2023-12-01",
            api_path="series/observations"
        )

        entry = _build_generated_entry(series, default_schedule="0 8 * * *")

        assert entry.source_name == "cpi_cpiaucsl"
        assert entry.series_id == "CPIAUCSL"
        assert entry.description == "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average"
        assert entry.schedule == "0 6 1 * *"
        assert entry.topic_var == "aurum_cpi_cpiaucsl_topic"
        assert entry.default_topic == "aurum.ref.cpi.cpiaucsl.v1"
        assert entry.frequency == "MONTHLY"
        assert entry.units_var == "aurum_cpi_cpiaucsl_units"
        assert entry.default_units == "Index 1982-1984=100"
        assert entry.area == "US_CITY_AVERAGE"
        assert entry.seasonal_adjustment == "Seasonally Adjusted"
        assert entry.window_hours is None
        assert entry.window_days is None
        assert entry.window_months == 1
        assert entry.window_years is None
        assert entry.dlq_topic == "aurum.ref.cpi.series.dlq.v1"
        assert entry.watermark_policy == "month"

    def test_build_generated_entry_daily(self) -> None:
        """Test building generated entry for daily CPI series."""
        series = CpiSeries(
            id="CPITRNSL",
            title="Consumer Price Index for All Urban Consumers: Transportation in U.S. City Average",
            units="Index 1982-1984=100",
            frequency="Daily",
            seasonal_adjustment="Seasonally Adjusted",
            last_updated="2024-01-11",
            popularity=65,
            notes="Transportation CPI data",
            category="Prices",
            start_date="1947-01-01",
            end_date="2023-12-01",
            api_path="series/observations"
        )

        entry = _build_generated_entry(series, default_schedule="0 8 * * *")

        assert entry.source_name == "cpi_cpitrnsl"
        assert entry.series_id == "CPITRNSL"
        assert entry.schedule == "0 6 * * *"
        assert entry.frequency == "DAILY"
        assert entry.area == "US_CITY_AVERAGE"
        assert entry.window_hours == 24
        assert entry.window_days is None
        assert entry.window_months is None
        assert entry.window_years is None
        assert entry.watermark_policy == "day"

    def test_load_cpi_catalog(self, tmp_path: Path) -> None:
        """Test loading CPI catalog from JSON."""
        catalog_data = {
            "generated_at": "2025-01-21T04:00:00.000000+00:00",
            "dataset_count": 2,
            "base_url": "https://api.stlouisfed.org/fred",
            "series": [
                {
                    "id": "CPIAUCSL",
                    "title": "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
                    "units": "Index 1982-1984=100",
                    "frequency": "Monthly",
                    "seasonal_adjustment": "Seasonally Adjusted",
                    "last_updated": "2024-01-11",
                    "popularity": 89,
                    "notes": "CPI data",
                    "category": "Prices",
                    "start_date": "1947-01-01",
                    "end_date": "2023-12-01",
                    "api_path": "series/observations"
                },
                {
                    "id": "CPITRNSL",
                    "title": "Consumer Price Index for All Urban Consumers: Transportation in U.S. City Average",
                    "units": "Index 1982-1984=100",
                    "frequency": "Monthly",
                    "seasonal_adjustment": "Seasonally Adjusted",
                    "last_updated": "2024-01-11",
                    "popularity": 65,
                    "notes": "Transportation CPI data",
                    "category": "Prices",
                    "start_date": "1947-01-01",
                    "end_date": "2023-12-01",
                    "api_path": "series/observations"
                }
            ]
        }

        catalog_file = tmp_path / "cpi_catalog.json"
        with open(catalog_file, 'w', encoding='utf-8') as f:
            json.dump(catalog_data, f)

        series = _load_cpi_catalog(catalog_file)

        assert len(series) == 2
        assert series[0].id == "CPIAUCSL"
        assert series[0].frequency == "Monthly"
        assert series[0].area == "US_CITY_AVERAGE"
        assert series[1].id == "CPITRNSL"
        assert series[1].frequency == "Monthly"

    def test_load_cpi_catalog_invalid(self, tmp_path: Path) -> None:
        """Test loading invalid CPI catalog."""
        catalog_file = tmp_path / "invalid_catalog.json"
        with open(catalog_file, 'w', encoding='utf-8') as f:
            f.write("invalid json")

        with pytest.raises(RuntimeError, match="Failed to load CPI catalog"):
            _load_cpi_catalog(catalog_file)

    def test_validate_config_valid(self) -> None:
        """Test validating valid configuration."""
        valid_config = {
            "datasets": [
                {
                    "source_name": "cpi_cpiaucsl",
                    "series_id": "CPIAUCSL",
                    "description": "Consumer Price Index for All Urban Consumers",
                    "schedule": "0 6 1 * *",
                    "topic_var": "aurum_cpi_cpiaucsl_topic",
                    "default_topic": "aurum.ref.cpi.cpiaucsl.v1",
                    "frequency": "MONTHLY",
                    "units_var": "aurum_cpi_cpiaucsl_units",
                    "default_units": "Index 1982-1984=100",
                    "area": "US_CITY_AVERAGE",
                    "seasonal_adjustment": "Seasonally Adjusted",
                    "window_hours": None,
                    "window_days": None,
                    "window_months": 1,
                    "window_years": None,
                    "dlq_topic": "aurum.ref.cpi.series.dlq.v1",
                    "watermark_policy": "month"
                }
            ]
        }

        # Should not raise exception
        _validate_config(valid_config)

    def test_validate_config_invalid(self) -> None:
        """Test validating invalid configuration."""
        invalid_config = {
            "datasets": [
                {
                    "source_name": "cpi_cpiaucsl",
                    # Missing required 'series_id' field
                    "schedule": "0 6 1 * *",
                    "topic_var": "aurum_cpi_cpiaucsl_topic",
                    "default_topic": "aurum.ref.cpi.cpiaucsl.v1",
                    "frequency": "MONTHLY"
                }
            ]
        }

        with pytest.raises(ValueError, match="Configuration validation failed"):
            _validate_config(invalid_config)

    def test_write_schema_file(self, tmp_path: Path) -> None:
        """Test writing schema file."""
        schema_path = tmp_path / "config" / "cpi_ingest_datasets.schema.json"

        # Mock the REPO_ROOT to use tmp_path
        import scripts.cpi.generate_ingest_config as config_module
        original_repo_root = config_module.REPO_ROOT
        config_module.REPO_ROOT = tmp_path

        try:
            _write_schema_file()
            assert schema_path.exists()

            schema_content = json.loads(schema_path.read_text(encoding='utf-8'))
            assert "$schema" in schema_content
            assert "type" in schema_content
            assert "properties" in schema_content
            assert "datasets" in schema_content["properties"]

        finally:
            config_module.REPO_ROOT = original_repo_root


class TestCPIConfigGenerationIntegration:
    def test_full_config_generation_workflow(self, tmp_path: Path) -> None:
        """Test the complete configuration generation workflow."""
        # Create a test catalog
        catalog_data = {
            "generated_at": "2025-01-21T04:00:00.000000+00:00",
            "dataset_count": 3,
            "base_url": "https://api.stlouisfed.org/fred",
            "series": [
                {
                    "id": "CPIAUCSL",
                    "title": "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
                    "units": "Index 1982-1984=100",
                    "frequency": "Monthly",
                    "seasonal_adjustment": "Seasonally Adjusted",
                    "last_updated": "2024-01-11",
                    "popularity": 89,
                    "notes": "CPI data",
                    "category": "Prices",
                    "start_date": "1947-01-01",
                    "end_date": "2023-12-01",
                    "api_path": "series/observations"
                },
                {
                    "id": "CPITRNSL",
                    "title": "Consumer Price Index for All Urban Consumers: Transportation in U.S. City Average",
                    "units": "Index 1982-1984=100",
                    "frequency": "Monthly",
                    "seasonal_adjustment": "Seasonally Adjusted",
                    "last_updated": "2024-01-11",
                    "popularity": 65,
                    "notes": "Transportation CPI data",
                    "category": "Prices",
                    "start_date": "1947-01-01",
                    "end_date": "2023-12-01",
                    "api_path": "series/observations"
                },
                {
                    "id": "CPIENGSL",
                    "title": "Consumer Price Index for All Urban Consumers: Energy in U.S. City Average",
                    "units": "Index 1982-1984=100",
                    "frequency": "Monthly",
                    "seasonal_adjustment": "Seasonally Adjusted",
                    "last_updated": "2024-01-11",
                    "popularity": 62,
                    "notes": "Energy CPI data",
                    "category": "Prices",
                    "start_date": "1947-01-01",
                    "end_date": "2023-12-01",
                    "api_path": "series/observations"
                }
            ]
        }

        catalog_file = tmp_path / "cpi_catalog.json"
        with open(catalog_file, 'w', encoding='utf-8') as f:
            json.dump(catalog_data, f)

        config_file = tmp_path / "cpi_ingest_datasets.json"

        # Import the module to test
        import scripts.cpi.generate_ingest_config as config_module

        # Mock REPO_ROOT to use tmp_path
        original_repo_root = config_module.REPO_ROOT
        config_module.REPO_ROOT = tmp_path

        try:
            # Test parsing args
            args = config_module.parse_args([
                "--catalog", str(catalog_file),
                "--output", str(config_file),
                "--default-schedule", "0 8 * * *"
            ])

            assert args.catalog == catalog_file
            assert args.output == config_file
            assert args.default_schedule == "0 8 * * *"

            # Test main function
            result = config_module.main([
                "--catalog", str(catalog_file),
                "--output", str(config_file),
                "--default-schedule", "0 8 * * *"
            ])

            assert result == 0
            assert config_file.exists()

            # Validate generated configuration
            with open(config_file, 'r', encoding='utf-8') as f:
                generated_config = json.load(f)

            assert "datasets" in generated_config
            assert len(generated_config["datasets"]) == 3

            # Check first dataset
            first_dataset = generated_config["datasets"][0]
            assert first_dataset["source_name"] == "cpi_cpiaucsl"
            assert first_dataset["series_id"] == "CPIAUCSL"
            assert first_dataset["frequency"] == "MONTHLY"
            assert first_dataset["schedule"] == "0 6 1 * *"
            assert first_dataset["watermark_policy"] == "month"
            assert first_dataset["window_months"] == 1
            assert first_dataset["area"] == "US_CITY_AVERAGE"

            # Check second dataset
            second_dataset = generated_config["datasets"][1]
            assert second_dataset["source_name"] == "cpi_cpitrnsl"
            assert second_dataset["series_id"] == "CPITRNSL"
            assert second_dataset["frequency"] == "MONTHLY"
            assert second_dataset["schedule"] == "0 6 1 * *"
            assert second_dataset["watermark_policy"] == "month"
            assert second_dataset["window_months"] == 1
            assert second_dataset["area"] == "US_CITY_AVERAGE"

        finally:
            config_module.REPO_ROOT = original_repo_root
