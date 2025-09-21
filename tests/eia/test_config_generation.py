from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
SCRIPTS_ROOT = REPO_ROOT / "scripts" / "eia"

sys.path.insert(0, str(SRC_ROOT))

from aurum.reference import eia_catalog  # noqa: E402
from aurum.reference.eia_catalog import EiaDataset  # noqa: E402


@pytest.fixture(scope="module")
def generator_module():
    spec = importlib.util.spec_from_file_location(
        "aurum_generate_ingest_config",
        SCRIPTS_ROOT / "generate_ingest_config.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def test_generated_config_matches_checked_in(generator_module) -> None:
    datasets = list(eia_catalog.iter_datasets())
    overrides = json.loads((REPO_ROOT / "config" / "eia_ingest_overrides.json").read_text())
    generated = generator_module.generate_config(
        datasets,
        default_schedule="15 6 * * *",
        overrides=overrides,
    )
    current = json.loads((REPO_ROOT / "config" / "eia_ingest_datasets.json").read_text())
    assert current["datasets"] == generated


def test_config_schema_validation(generator_module) -> None:
    """Test that generated config validates against the JSON schema."""
    # Create a minimal dataset for testing
    test_dataset = EiaDataset(
        path="test/dataset",
        name="Test Dataset",
        description="Test dataset for validation",
        frequencies=[
            {
                "id": "daily",
                "description": "Daily",
                "query": "D",
                "format": "YYYY-MM-DD"
            }
        ],
        facets=[],
        data_columns=["value"],
        start_period="2024-01-01",
        end_period="2024-12-31",
        default_frequency="daily",
        default_date_format="YYYY-MM-DD",
        browser_total=None,
        browser_frequency=None,
        browser_date_format=None,
        warnings=[]
    )

    # Generate config for test dataset
    generated = generator_module.generate_config(
        [test_dataset],
        default_schedule="15 6 * * *",
        overrides={}
    )

    # Validate against schema - this should not raise an exception
    generator_module._validate_config({"datasets": generated})


def test_schedule_resolution(generator_module) -> None:
    """Test that frequency-based schedule resolution works correctly."""
    # Test dataset with different frequencies
    datasets = [
        EiaDataset(
            path="test/hourly",
            name="Hourly Dataset",
            description="Hourly test data",
            frequencies=[
                {
                    "id": "hourly",
                    "description": "Hourly",
                    "query": "H",
                    "format": "YYYY-MM-DD HH"
                }
            ],
            facets=[],
            data_columns=["value"],
            start_period="2024-01-01",
            end_period="2024-12-31",
            default_frequency="hourly",
            default_date_format="YYYY-MM-DD HH",
            browser_total=None,
            browser_frequency=None,
            browser_date_format=None,
            warnings=[]
        ),
        EiaDataset(
            path="test/daily",
            name="Daily Dataset",
            description="Daily test data",
            frequencies=[
                {
                    "id": "daily",
                    "description": "Daily",
                    "query": "D",
                    "format": "YYYY-MM-DD"
                }
            ],
            facets=[],
            data_columns=["value"],
            start_period="2024-01-01",
            end_period="2024-12-31",
            default_frequency="daily",
            default_date_format="YYYY-MM-DD",
            browser_total=None,
            browser_frequency=None,
            browser_date_format=None,
            warnings=[]
        )
    ]

    generated = generator_module.generate_config(
        datasets,
        default_schedule="0 0 * * *",
        overrides={}
    )

    # Check that schedules were resolved correctly
    assert len(generated) == 2
    hourly_dataset = next(d for d in generated if d["source_name"] == "eia_test_hourly")
    daily_dataset = next(d for d in generated if d["source_name"] == "eia_test_daily")

    assert hourly_dataset["schedule"] == "5 * * * *"
    assert daily_dataset["schedule"] == "20 6 * * *"


def test_window_defaults(generator_module) -> None:
    """Test that window defaults are set correctly based on frequency."""
    datasets = [
        EiaDataset(
            path="test/hourly",
            name="Hourly Dataset",
            description="Hourly test data",
            frequencies=[
                {
                    "id": "hourly",
                    "description": "Hourly",
                    "query": "H",
                    "format": "YYYY-MM-DD HH"
                }
            ],
            facets=[],
            data_columns=["value"],
            start_period="2024-01-01",
            end_period="2024-12-31",
            default_frequency="hourly",
            default_date_format="YYYY-MM-DD HH",
            browser_total=None,
            browser_frequency=None,
            browser_date_format=None,
            warnings=[]
        ),
        EiaDataset(
            path="test/annual",
            name="Annual Dataset",
            description="Annual test data",
            frequencies=[
                {
                    "id": "annual",
                    "description": "Annual",
                    "query": "A",
                    "format": "YYYY"
                }
            ],
            facets=[],
            data_columns=["value"],
            start_period="2024-01-01",
            end_period="2024-12-31",
            default_frequency="annual",
            default_date_format="YYYY",
            browser_total=None,
            browser_frequency=None,
            browser_date_format=None,
            warnings=[]
        )
    ]

    generated = generator_module.generate_config(
        datasets,
        default_schedule="15 6 * * *",
        overrides={}
    )

    hourly_dataset = next(d for d in generated if d["source_name"] == "eia_test_hourly")
    annual_dataset = next(d for d in generated if d["source_name"] == "eia_test_annual")

    # Hourly should have window_hours = 1
    assert hourly_dataset["window_hours"] == 1
    assert hourly_dataset["window_days"] is None
    assert hourly_dataset["window_months"] is None
    assert hourly_dataset["window_years"] is None

    # Annual should have window_years = 1
    assert annual_dataset["window_hours"] is None
    assert annual_dataset["window_days"] is None
    assert annual_dataset["window_months"] is None
    assert annual_dataset["window_years"] == 1


def test_override_merging(generator_module) -> None:
    """Test that overrides are merged correctly with generated config."""
    test_dataset = EiaDataset(
        path="test/dataset",
        name="Test Dataset",
        description="Test dataset for override testing",
        frequencies=[
            {
                "id": "daily",
                "description": "Daily",
                "query": "D",
                "format": "YYYY-MM-DD"
            }
        ],
        facets=[],
        data_columns=["value"],
        start_period="2024-01-01",
        end_period="2024-12-31",
        default_frequency="daily",
        default_date_format="YYYY-MM-DD",
        browser_total=None,
        browser_frequency=None,
        browser_date_format=None,
        warnings=[]
    )

    overrides = {
        "test/dataset": {
            "schedule": "30 12 * * *",
            "page_limit": 1000,
            "filter_expr": "value > 0",
            "description": "Overridden description"
        }
    }

    generated = generator_module.generate_config(
        [test_dataset],
        default_schedule="15 6 * * *",
        overrides=overrides
    )

    assert len(generated) == 1
    config = generated[0]

    # Check that path-based override was applied
    assert config["schedule"] == "30 12 * * *"
    assert config["page_limit"] == 1000
    assert config["filter_expr"] == "value > 0"

    # Check that description override was applied
    assert config["description"] == "Overridden description"


def test_invalid_schedule_raises_error(generator_module) -> None:
    """Test that invalid cron schedules are rejected."""
    test_dataset = EiaDataset(
        path="test/dataset",
        name="Test Dataset",
        description="Test dataset",
        frequencies=[
            {
                "id": "daily",
                "description": "Daily",
                "query": "D",
                "format": "YYYY-MM-DD"
            }
        ],
        facets=[],
        data_columns=["value"],
        start_period="2024-01-01",
        end_period="2024-12-31",
        default_frequency="daily",
        default_date_format="YYYY-MM-DD",
        browser_total=None,
        browser_frequency=None,
        browser_date_format=None,
        warnings=[]
    )

    overrides = {
        "test/dataset": {
            "schedule": "invalid cron expression"
        }
    }

    generated = generator_module.generate_config(
        [test_dataset],
        default_schedule="15 6 * * *",
        overrides=overrides
    )

    payload = {"datasets": generated}

    # Should raise validation error due to invalid cron pattern
    with pytest.raises(ValueError, match="Generated config is invalid"):
        generator_module._validate_config(payload)


def test_required_fields_validation(generator_module) -> None:
    """Test that required fields are present and correctly typed."""
    test_dataset = EiaDataset(
        path="test/dataset",
        name="Test Dataset",
        description="Test dataset",
        frequencies=[
            {
                "id": "daily",
                "description": "Daily",
                "query": "D",
                "format": "YYYY-MM-DD"
            }
        ],
        facets=[],
        data_columns=["value"],
        start_period="2024-01-01",
        end_period="2024-12-31",
        default_frequency="daily",
        default_date_format="YYYY-MM-DD",
        browser_total=None,
        browser_frequency=None,
        browser_date_format=None,
        warnings=[]
    )

    generated = generator_module.generate_config(
        [test_dataset],
        default_schedule="15 6 * * *",
        overrides={}
    )

    assert len(generated) == 1
    config = generated[0]

    # Check required fields
    required_fields = [
        "source_name", "path", "data_path", "schedule", "topic_var",
        "default_topic", "series_id_expr", "frequency"
    ]

    for field in required_fields:
        assert field in config, f"Required field {field} is missing"
        assert config[field] is not None, f"Required field {field} is None"

    # Check field types
    assert isinstance(config["source_name"], str)
    assert isinstance(config["path"], str)
    assert isinstance(config["schedule"], str)
    assert isinstance(config["frequency"], str)
    assert isinstance(config["page_limit"], int)
    assert config["frequency"] in ["HOURLY", "SUBHOURLY", "QUARTER_HOURLY", "EIGHTH_HOURLY", "DAILY", "WEEKLY", "MONTHLY", "QUARTERLY", "ANNUAL"]


def test_series_id_strategy_validation(generator_module) -> None:
    """Test that series_id_strategy is correctly structured."""
    test_dataset = EiaDataset(
        path="test/dataset",
        name="Test Dataset",
        description="Test dataset",
        frequencies=[
            {
                "id": "daily",
                "description": "Daily",
                "query": "D",
                "format": "YYYY-MM-DD"
            }
        ],
        facets=[{"id": "seriesId", "description": "Series"}],
        data_columns=["value"],
        start_period="2024-01-01",
        end_period="2024-12-31",
        default_frequency="daily",
        default_date_format="YYYY-MM-DD",
        browser_total=None,
        browser_frequency=None,
        browser_date_format=None,
        warnings=[]
    )

    generated = generator_module.generate_config(
        [test_dataset],
        default_schedule="15 6 * * *",
        overrides={}
    )

    assert len(generated) == 1
    config = generated[0]

    # Check series_id_strategy structure
    assert "series_id_strategy" in config
    strategy = config["series_id_strategy"]
    assert "source" in strategy
    assert "components" in strategy
    assert isinstance(strategy["components"], list)
    assert len(strategy["components"]) > 0
