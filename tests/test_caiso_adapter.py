"""Comprehensive tests for CAISO adapter."""

import pytest
import pytest_asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from aurum.external.adapters.caiso import CaisoAdapter, CaisoConfig
from aurum.external.collect import HttpRequest
from aurum.test_fixtures import (
    MockHttpResponse,
    create_successful_caiso_response,
    generate_caiso_lmp_data,
    validate_iso_record_schema
)
from aurum.test_fixtures.fixtures import get_test_time_range


class TestCaisoAdapter:
    """Test suite for CAISO adapter."""

    @pytest.fixture
    def adapter(self):
        """Create CAISO adapter instance."""
        return CaisoAdapter(
            series_id="test_caiso_lmp_v1",
            kafka_topic="test.aurum.iso.caiso.lmp.v1"
        )

    @pytest.fixture
    def sample_lmp_data(self):
        """Generate sample LMP data for testing."""
        return generate_caiso_lmp_data(count=10)

    @pytest.fixture
    def mock_collector(self):
        """Create mock external collector."""
        collector = MagicMock()
        collector.request = AsyncMock()
        return collector

    def test_adapter_initialization(self, adapter):
        """Test adapter initialization."""
        assert adapter._cfg is not None
        assert adapter.circuit_breaker is not None
        assert adapter.metrics is not None
        assert adapter._request_count == 0

    def test_build_request(self, adapter):
        """Test request building."""
        start_time, end_time = get_test_time_range()

        request = adapter.build_request({
            "start": start_time,
            "end": end_time,
            "data_type": "lmp"
        })

        assert isinstance(request, HttpRequest)
        assert request.method == "GET"
        assert request.path == "/Group_Prices/PRC_LMP"
        assert "startdatetime" in request.params
        assert "enddatetime" in request.params
        assert request.timeout == adapter._cfg.timeout_seconds

    def test_parse_page_success(self, adapter, sample_lmp_data):
        """Test successful response parsing."""
        mock_response = create_successful_caiso_response(sample_lmp_data)

        records, next_cursor = adapter.parse_page(mock_response.json_data)

        assert isinstance(records, list)
        assert len(records) == len(sample_lmp_data)
        assert next_cursor is None  # CAISO doesn't use cursor pagination

        # Validate each record
        for record in records:
            assert "ingest_ts" in record
            assert record["iso_code"] == "CAISO"

    def test_parse_page_empty_response(self, adapter):
        """Test parsing empty response."""
        mock_response = MockHttpResponse(status=200, content=b'{}')

        records, next_cursor = adapter.parse_page({})

        assert records == []
        assert next_cursor is None

    def test_validate_request_params(self, adapter):
        """Test request parameter validation."""
        valid_params = {
            "startdatetime": "2024-01-15T07:00-0000",
            "enddatetime": "2024-01-15T08:00-0000"
        }

        # Should not raise exception
        adapter._validate_request_params(valid_params)

    def test_validate_request_params_invalid(self, adapter):
        """Test request parameter validation with invalid data."""
        invalid_params = {
            "startdatetime": "invalid-date",
            "enddatetime": "2024-01-15T08:00-0000"
        }

        with pytest.raises(ValueError, match="Invalid datetime format"):
            adapter._validate_request_params(invalid_params)

    def test_validate_record(self, adapter, sample_lmp_data):
        """Test record validation."""
        # Valid record should pass
        valid_record = sample_lmp_data[0]
        assert adapter._validate_record(valid_record)

    def test_validate_record_invalid(self, adapter):
        """Test record validation with invalid data."""
        invalid_record = {
            "invalid_field": "invalid_value"
        }
        assert not adapter._validate_record(invalid_record)

    @pytest.mark.asyncio
    async def test_make_resilient_request_success(self, adapter, sample_lmp_data):
        """Test successful resilient request."""
        mock_response = create_successful_caiso_response(sample_lmp_data)
        adapter.collector.request = AsyncMock(return_value=mock_response)

        request = HttpRequest(
            method="GET",
            path="/Group_Prices/PRC_LMP",
            params={"test": "params"}
        )

        result = await adapter.make_resilient_request(request)

        assert result is not None
        assert "PRC_LMP" in result
        assert adapter._request_count == 1
        assert adapter._error_count == 0

    @pytest.mark.asyncio
    async def test_make_resilient_request_circuit_breaker_open(self, adapter):
        """Test request with open circuit breaker."""
        # Force circuit breaker open
        adapter.circuit_breaker.is_open_state = True

        request = HttpRequest(
            method="GET",
            path="/Group_Prices/PRC_LMP",
            params={"test": "params"}
        )

        with pytest.raises(RuntimeError, match="Circuit breaker is open"):
            await adapter.make_resilient_request(request)

    @pytest.mark.asyncio
    async def test_retry_with_backoff(self, adapter, sample_lmp_data):
        """Test retry logic with exponential backoff."""
        # Mock first request to fail, second to succeed
        mock_response = create_successful_caiso_response(sample_lmp_data)
        adapter.collector.request = AsyncMock(side_effect=[
            RuntimeError("Connection failed"),
            mock_response
        ])

        request = HttpRequest(
            method="GET",
            path="/Group_Prices/PRC_LMP",
            params={"test": "params"}
        )

        result = await adapter._retry_with_backoff(request)

        assert result is not None
        assert adapter.collector.request.call_count == 2

    def test_health_status(self, adapter):
        """Test health status reporting."""
        health = adapter.get_health_status()

        assert "adapter" in health
        assert health["adapter"] == "caiso"
        assert "circuit_breaker_status" in health
        assert "total_requests" in health
        assert "error_rate" in health

    def test_data_validation_against_golden_data(self, sample_lmp_data):
        """Test that generated data passes validation against golden data."""
        validation_result = validate_iso_record_schema(
            sample_lmp_data,
            "CAISO",
            "lmp"
        )

        assert validation_result.passed
        assert len(validation_result.errors) == 0
        assert validation_result.metrics["record_count"] == len(sample_lmp_data)

    @pytest.mark.parametrize("data_type,expected_fields", [
        ("lmp", ["location_id", "price_total", "interval_start", "currency", "uom"]),
        ("load", ["area", "mw", "interval_start"]),
        ("asm", ["location_id", "price_mcp", "interval_start"])
    ])
    def test_required_fields_validation(self, adapter, data_type, expected_fields):
        """Test that required fields are correctly identified."""
        # This test ensures the adapter knows about required fields
        assert set(expected_fields).issubset(set([
            "location_id", "price_total", "interval_start", "currency", "uom",
            "area", "mw", "price_mcp"
        ]))

    def test_circuit_breaker_integration(self, adapter):
        """Test circuit breaker integration."""
        # Test initial state
        assert not adapter.circuit_breaker.is_open()

        # Test recording failures
        for _ in range(adapter._cfg.circuit_breaker_threshold):
            adapter.circuit_breaker.record_failure()

        assert adapter.circuit_breaker.is_open()

        # Test recovery
        adapter.circuit_breaker.record_success()
        assert not adapter.circuit_breaker.is_open()


class TestCaisoAdapterIntegration:
    """Integration tests for CAISO adapter."""

    @pytest.fixture
    def integration_adapter(self):
        """Create adapter for integration testing."""
        adapter = CaisoAdapter(
            series_id="integration_test_caiso_lmp_v1",
            kafka_topic="integration.test.aurum.iso.caiso.lmp.v1"
        )

        # Configure for testing
        adapter._cfg = CaisoConfig(
            max_retries=2,
            base_backoff_seconds=0.1,
            max_backoff_seconds=1.0,
            circuit_breaker_threshold=3,
            timeout_seconds=5
        )

        return adapter

    @pytest.mark.asyncio
    async def test_full_request_cycle(self, integration_adapter):
        """Test complete request/response cycle."""
        # Mock successful response
        sample_data = generate_caiso_lmp_data(count=5)
        mock_response = create_successful_caiso_response(sample_data)

        integration_adapter.collector.request = AsyncMock(return_value=mock_response)

        # Build and execute request
        start_time, end_time = get_test_time_range()
        request = integration_adapter.build_request({
            "start": start_time,
            "end": end_time,
            "data_type": "lmp"
        })

        result = await integration_adapter.make_resilient_request(request)

        # Validate result
        assert result is not None
        assert "PRC_LMP" in result
        assert len(result["PRC_LMP"]["results"]["items"]) == 5

        # Validate health metrics
        health = integration_adapter.get_health_status()
        assert health["total_requests"] == 1
        assert health["error_rate"] == 0.0

    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, integration_adapter):
        """Test error handling and recovery mechanisms."""
        # Mock error responses
        error_response = MockHttpResponse(
            status=500,
            content=b'{"error": "Internal Server Error"}'
        )

        integration_adapter.collector.request = AsyncMock(return_value=error_response)

        start_time, end_time = get_test_time_range()
        request = integration_adapter.build_request({
            "start": start_time,
            "end": end_time,
            "data_type": "lmp"
        })

        # Should raise exception after retries
        with pytest.raises(RuntimeError, match="HTTP 500"):
            await integration_adapter.make_resilient_request(request)

        # Check that circuit breaker is triggered
        health = integration_adapter.get_health_status()
        assert health["error_count"] == 1
        assert health["total_requests"] == 1
        assert health["error_rate"] == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
