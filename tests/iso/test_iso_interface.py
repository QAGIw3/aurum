"""Tests for ISO data extraction interface."""

from __future__ import annotations

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.iso import (
    IsoBaseExtractor,
    IsoConfig,
    IsoDataType,
    NyisoExtractor,
    PjmExtractor,
    CaisoExtractor,
    MisoExtractor,
    SppExtractor,
    AesoExtractor
)


class TestIsoConfig:
    def test_config_defaults(self) -> None:
        """Test ISO configuration defaults."""
        config = IsoConfig(base_url="https://api.example.com")

        assert config.base_url == "https://api.example.com"
        assert config.markets == ["DAM", "RTM"]
        assert config.data_types == [IsoDataType.LMP, IsoDataType.LOAD]
        assert config.timeout == 30
        assert config.max_retries == 3
        assert config.backoff_factor == 0.3

    def test_config_custom_values(self) -> None:
        """Test ISO configuration with custom values."""
        config = IsoConfig(
            base_url="https://api.example.com",
            api_key="test_key",
            timeout=60,
            max_retries=5,
            markets=["DAM"],
            data_types=[IsoDataType.LMP],
            requests_per_minute=100,
            requests_per_hour=1000
        )

        assert config.api_key == "test_key"
        assert config.timeout == 60
        assert config.max_retries == 5
        assert config.markets == ["DAM"]
        assert config.data_types == [IsoDataType.LMP]
        assert config.requests_per_minute == 100
        assert config.requests_per_hour == 1000


class TestIsoBaseExtractor:
    def test_init(self) -> None:
        """Test base extractor initialization."""
        config = IsoConfig(base_url="https://api.example.com")
        extractor = IsoBaseExtractor(config)

        assert extractor.config == config
        assert extractor.session is not None

    def test_extract_all_data_types(self) -> None:
        """Test extraction of all configured data types."""
        config = IsoConfig(base_url="https://api.example.com")
        extractor = IsoBaseExtractor(config)

        # Mock the individual extraction methods
        with patch.object(extractor, 'get_lmp_data') as mock_lmp, \
             patch.object(extractor, 'get_load_data') as mock_load, \
             patch.object(extractor, 'get_generation_mix') as mock_genmix:

            mock_lmp.return_value = [{"test": "lmp"}]
            mock_load.return_value = [{"test": "load"}]
            mock_genmix.return_value = [{"test": "genmix"}]

            result = extractor.extract_all_data_types("2024-01-01", "2024-01-02")

            assert IsoDataType.LMP in result
            assert IsoDataType.LOAD in result
            assert IsoDataType.GENERATION_MIX in result
            assert result[IsoDataType.LMP] == [{"test": "lmp"}]
            assert result[IsoDataType.LOAD] == [{"test": "load"}]
            assert result[IsoDataType.GENERATION_MIX] == [{"test": "genmix"}]

    def test_validate_date_range_valid(self) -> None:
        """Test date range validation with valid inputs."""
        config = IsoConfig(base_url="https://api.example.com")
        extractor = IsoBaseExtractor(config)

        start, end = extractor._validate_date_range("2024-01-01", "2024-01-02")
        assert start == "2024-01-01"
        assert end == "2024-01-02"

    def test_validate_date_range_invalid_order(self) -> None:
        """Test date range validation with invalid order."""
        config = IsoConfig(base_url="https://api.example.com")
        extractor = IsoBaseExtractor(config)

        with pytest.raises(ValueError, match="Start date must be before"):
            extractor._validate_date_range("2024-01-02", "2024-01-01")

    def test_validate_date_range_too_long(self) -> None:
        """Test date range validation with range too long."""
        config = IsoConfig(base_url="https://api.example.com")
        extractor = IsoBaseExtractor(config)

        with pytest.raises(ValueError, match="Date range cannot exceed 1 year"):
            extractor._validate_date_range("2023-01-01", "2024-01-02")

    def test_normalize_timestamp_string(self) -> None:
        """Test timestamp normalization with string input."""
        config = IsoConfig(base_url="https://api.example.com")
        extractor = IsoBaseExtractor(config)

        result = extractor._normalize_timestamp("2024-01-01T12:00:00")
        assert result == "2024-01-01T12:00:00"

    def test_normalize_timestamp_int(self) -> None:
        """Test timestamp normalization with integer input."""
        config = IsoConfig(base_url="https://api.example.com")
        extractor = IsoBaseExtractor(config)

        result = extractor._normalize_timestamp(1704110400)  # 2024-01-01 12:00:00 UTC
        assert result.startswith("2024-01-01")


class TestNyisoExtractor:
    def test_init(self) -> None:
        """Test NYISO extractor initialization."""
        config = IsoConfig(base_url="http://mis.nyiso.com/public")
        extractor = NyisoExtractor(config)

        assert extractor.config.base_url == "http://mis.nyiso.com/public"
        assert extractor.markets == {
            "DAM": "damlbmp_zone",
            "RTM": "realtime_zone"
        }

    def test_setup_rate_limiting(self) -> None:
        """Test NYISO rate limiting setup."""
        config = IsoConfig(base_url="http://mis.nyiso.com/public")
        extractor = NyisoExtractor(config)

        # NYISO specific rate limits should be applied
        assert extractor.config.requests_per_minute == 10
        assert extractor.config.requests_per_hour == 600

    def test_normalize_lmp_record(self) -> None:
        """Test NYISO LMP record normalization."""
        config = IsoConfig(base_url="http://mis.nyiso.com/public")
        extractor = NyisoExtractor(config)

        row = {
            "Time Stamp": "01/01/2024 12:00:00",
            "PTID": "12345",
            "Name": "TEST_NODE",
            "Zone": "A",
            "LBMP ($/MWHr)": "50.00",
            "Marginal Cost Congestion ($/MWHr)": "5.00",
            "Marginal Cost Losses ($/MWHr)": "2.00"
        }

        result = extractor._normalize_lmp_record(row, "DAM")

        assert result["timestamp"] == "2024-01-01T12:00:00"
        assert result["node_id"] == "12345"
        assert result["node_name"] == "TEST_NODE"
        assert result["zone"] == "A"
        assert result["market"] == "DAM"
        assert result["lmp"] == 50.00
        assert result["congestion"] == 5.00
        assert result["losses"] == 2.00
        assert result["source"] == "NYISO"
        assert result["data_type"] == "lmp"

    def test_normalize_load_record(self) -> None:
        """Test NYISO load record normalization."""
        config = IsoConfig(base_url="http://mis.nyiso.com/public")
        extractor = NyisoExtractor(config)

        row = {
            "Time Stamp": "01/01/2024 12:00:00",
            "Zone": "A",
            "Load (MW)": "1000.00"
        }

        result = extractor._normalize_load_record(row)

        assert result["timestamp"] == "2024-01-01T12:00:00"
        assert result["zone"] == "A"
        assert result["load_mw"] == 1000.00
        assert result["source"] == "NYISO"
        assert result["data_type"] == "load"

    def test_parse_nyiso_timestamp(self) -> None:
        """Test NYISO timestamp parsing."""
        config = IsoConfig(base_url="http://mis.nyiso.com/public")
        extractor = NyisoExtractor(config)

        result = extractor._parse_nyiso_timestamp("01/01/2024 12:00:00")
        assert result == "2024-01-01T12:00:00"


class TestPjmExtractor:
    def test_init(self) -> None:
        """Test PJM extractor initialization."""
        config = IsoConfig(base_url="https://api.pjm.com/api/v1", api_key="test_key")
        extractor = PjmExtractor(config)

        assert extractor.config.base_url == "https://api.pjm.com/api/v1"
        assert extractor.markets == {
            "DAM": "DAY_AHEAD_HOURLY",
            "RTM": "REAL_TIME_5_MIN"
        }

    def test_get_auth_headers(self) -> None:
        """Test PJM authentication headers."""
        config = IsoConfig(base_url="https://api.pjm.com/api/v1", api_key="test_key")
        extractor = PjmExtractor(config)

        headers = extractor._get_auth_headers()
        assert headers == {"Ocp-Apim-Subscription-Key": "test_key"}

    def test_normalize_lmp_record(self) -> None:
        """Test PJM LMP record normalization."""
        config = IsoConfig(base_url="https://api.pjm.com/api/v1")
        extractor = PjmExtractor(config)

        item = {
            "datetime_beginning_utc": "2024-01-01T12:00:00",
            "bus_id": "12345",
            "bus_name": "TEST_NODE",
            "area": "PJM",
            "total_lmp": 50.00,
            "congestion_price": 5.00,
            "marginal_loss_price": 2.00
        }

        result = extractor._normalize_lmp_record(item, "DAM")

        assert result["timestamp"] == "2024-01-01T12:00:00"
        assert result["node_id"] == "12345"
        assert result["node_name"] == "TEST_NODE"
        assert result["zone"] == "PJM"
        assert result["market"] == "DAM"
        assert result["lmp"] == 50.00
        assert result["congestion"] == 5.00
        assert result["losses"] == 2.00
        assert result["source"] == "PJM"
        assert result["data_type"] == "lmp"


class TestCaisoExtractor:
    def test_init(self) -> None:
        """Test CAISO extractor initialization."""
        config = IsoConfig(base_url="http://oasis.caiso.com/oasisapi")
        extractor = CaisoExtractor(config)

        assert extractor.config.base_url == "http://oasis.caiso.com/oasisapi"
        assert extractor.markets == {
            "DAM": "DAM",
            "RTM": "RTM"
        }

    def test_normalize_lmp_record(self) -> None:
        """Test CAISO LMP record normalization."""
        config = IsoConfig(base_url="http://oasis.caiso.com/oasisapi")
        extractor = CaisoExtractor(config)

        item = {
            "INTERVAL_START_GMT": "2024-01-01T12:00:00",
            "PNODE_ID": "12345",
            "PNODE_NAME": "TEST_NODE",
            "TAC_ZONE": "PGAE",
            "LMP_CONGESTION": 5.00,
            "LMP_LOSS": 2.00,
            "LMP_ENERGY": 43.00
        }

        result = extractor._normalize_lmp_record(item, "DAM")

        assert result["timestamp"] == "2024-01-01T12:00:00"
        assert result["node_id"] == "12345"
        assert result["node_name"] == "TEST_NODE"
        assert result["zone"] == "PGAE"
        assert result["market"] == "DAM"
        assert result["lmp"] == 50.00  # Sum of components
        assert result["congestion"] == 5.00
        assert result["losses"] == 2.00
        assert result["energy"] == 43.00
        assert result["source"] == "CAISO"
        assert result["data_type"] == "lmp"


class TestMisoExtractor:
    def test_init(self) -> None:
        """Test MISO extractor initialization."""
        config = IsoConfig(base_url="https://api.misoenergy.org")
        extractor = MisoExtractor(config)

        assert extractor.config.base_url == "https://api.misoenergy.org"
        assert extractor.markets == {
            "DAM": "DA",
            "RTM": "RT"
        }


class TestSppExtractor:
    def test_init(self) -> None:
        """Test SPP extractor initialization."""
        config = IsoConfig(base_url="https://api.spp.org/api/v1")
        extractor = SppExtractor(config)

        assert extractor.config.base_url == "https://api.spp.org/api/v1"
        assert extractor.markets == {
            "DAM": "DAM",
            "RTM": "RTBM"
        }


class TestAesoExtractor:
    def test_init(self) -> None:
        """Test AESO extractor initialization."""
        config = IsoConfig(base_url="https://api.aeso.ca/api/v1", api_key="test_token")
        extractor = AesoExtractor(config)

        assert extractor.config.base_url == "https://api.aeso.ca/api/v1"
        assert extractor.markets == {
            "DAM": "energy",
            "RTM": "dispatch"
        }

    def test_get_auth_headers(self) -> None:
        """Test AESO authentication headers."""
        config = IsoConfig(base_url="https://api.aeso.ca/api/v1", api_key="test_token")
        extractor = AesoExtractor(config)

        headers = extractor._get_auth_headers()
        assert headers == {"Authorization": "Bearer test_token"}


class TestIsoConfigGeneration:
    def test_generate_config_from_catalog(self) -> None:
        """Test configuration generation from ISO catalog."""
        # This would test the generate_ingest_config.py script
        # For now, just test that we can import the module
        from scripts.iso.generate_ingest_config import _load_iso_catalog, _build_generated_entry, _derive_schedule

        # Test that we can load the catalog
        catalog_path = REPO_ROOT / "config" / "iso_catalog.json"
        assert catalog_path.exists()

        # Test schedule derivation
        assert _derive_schedule("lmp", "DAM") == "0 6 * * *"
        assert _derive_schedule("lmp", "RTM") == "0 */6 * * *"
        assert _derive_schedule("generation_mix", "DAM") == "0 8 * * *"

        # Test watermark policy derivation
        from scripts.iso.generate_ingest_config import _derive_watermark_policy
        assert _derive_watermark_policy("lmp", "RTM") == "hour"
        assert _derive_watermark_policy("load", "DAM") == "hour"
        assert _derive_watermark_policy("generation_mix", "DAM") == "day"

    def test_iso_source_from_dict(self) -> None:
        """Test IsoSource creation from dictionary."""
        from scripts.iso.generate_ingest_config import IsoSource

        data = {
            "name": "test_iso",
            "full_name": "Test ISO",
            "region": "Test Region",
            "base_url": "https://api.test.com",
            "api_key_required": True,
            "data_types": ["lmp", "load"],
            "markets": ["DAM", "RTM"],
            "rate_limits": {"requests_per_minute": 100}
        }

        iso = IsoSource.from_dict(data)

        assert iso.name == "test_iso"
        assert iso.full_name == "Test ISO"
        assert iso.api_key_required is True
        assert iso.data_types == ["lmp", "load"]
        assert iso.markets == ["DAM", "RTM"]
        assert iso.rate_limits == {"requests_per_minute": 100}


class TestIntegrationScenarios:
    def test_nyiso_csv_parsing(self) -> None:
        """Test NYISO CSV parsing integration."""
        config = IsoConfig(base_url="http://mis.nyiso.com/public")
        extractor = NyisoExtractor(config)

        # Mock CSV data
        csv_data = """Time Stamp,PTID,Name,Zone,LBMP ($/MWHr),Marginal Cost Congestion ($/MWHr),Marginal Cost Losses ($/MWHr)
01/01/2024 12:00:00,12345,TEST_NODE,A,50.00,5.00,2.00
01/01/2024 13:00:00,67890,TEST_NODE2,B,55.00,6.00,2.50"""

        with patch.object(extractor, '_make_csv_request') as mock_request:
            mock_request.return_value = csv_data

            result = extractor.get_lmp_data("2024-01-01", "2024-01-01", "DAM")

            assert len(result) == 2
            assert result[0]["lmp"] == 50.00
            assert result[0]["congestion"] == 5.00
            assert result[0]["losses"] == 2.00
            assert result[1]["lmp"] == 55.00

    def test_pjm_json_parsing(self) -> None:
        """Test PJM JSON parsing integration."""
        config = IsoConfig(base_url="https://api.pjm.com/api/v1", api_key="test_key")
        extractor = PjmExtractor(config)

        # Mock JSON response
        json_response = {
            "items": [
                {
                    "datetime_beginning_utc": "2024-01-01T12:00:00",
                    "bus_id": "12345",
                    "bus_name": "TEST_NODE",
                    "area": "PJM",
                    "total_lmp": 50.00,
                    "congestion_price": 5.00,
                    "marginal_loss_price": 2.00
                }
            ]
        }

        with patch.object(extractor, '_make_request') as mock_request:
            mock_request.return_value = json_response

            result = extractor.get_lmp_data("2024-01-01", "2024-01-01", "DAM")

            assert len(result) == 1
            assert result[0]["lmp"] == 50.00
            assert result[0]["congestion"] == 5.00
            assert result[0]["losses"] == 2.00
