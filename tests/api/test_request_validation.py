"""Tests for comprehensive request validation with Pydantic models and error envelopes."""
from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from aurum.api.models import (
    AurumBaseModel,
    Meta,
    ErrorEnvelope,
    ValidationErrorDetail,
    ValidationErrorResponse,
    CurveQueryParams,
    CurveDiffQueryParams,
    ScenarioCreateRequest,
    CurvePoint,
    CurveDiffPoint,
)
from aurum.api.exceptions import (
    handle_api_exception,
    ValidationException,
    NotFoundException,
    ForbiddenException,
)


class TestAurumBaseModel:
    """Test the base model configuration."""

    def test_base_model_forbids_extra_fields(self):
        """Test that AurumBaseModel forbids extra fields by default."""

        class TestModel(AurumBaseModel):
            name: str

        # Should work with valid data
        model = TestModel(name="test")
        assert model.name == "test"

        # Should raise validation error with extra fields
        with pytest.raises(Exception):
            TestModel(name="test", extra_field="extra")


class TestErrorEnvelope:
    """Test error envelope model."""

    def test_error_envelope_creation(self):
        """Test creating error envelope with all fields."""
        envelope = ErrorEnvelope(
            error="ValidationError",
            message="Field is required",
            code="required",
            field="tenant_id",
            value=None,
            context={"parameter": "tenant_id"},
            request_id="req-123",
        )

        assert envelope.error == "ValidationError"
        assert envelope.message == "Field is required"
        assert envelope.code == "required"
        assert envelope.field == "tenant_id"
        assert envelope.value is None
        assert envelope.context == {"parameter": "tenant_id"}
        assert envelope.request_id == "req-123"
        assert isinstance(envelope.timestamp, datetime)

    def test_error_envelope_minimal(self):
        """Test creating error envelope with minimal fields."""
        envelope = ErrorEnvelope(
            error="TestError",
            message="Something went wrong",
        )

        assert envelope.error == "TestError"
        assert envelope.message == "Something went wrong"
        assert envelope.code is None
        assert envelope.field is None
        assert envelope.value is None
        assert envelope.context is None
        assert envelope.request_id is None

    def test_error_envelope_invalid_context(self):
        """Test error envelope with invalid context."""
        with pytest.raises(ValueError, match="Context must be a dictionary"):
            ErrorEnvelope(error="TestError", context="not a dict")

    def test_error_envelope_to_dict(self):
        """Test error envelope serialization."""
        envelope = ErrorEnvelope(
            error="ValidationError",
            message="Field is required",
            request_id="req-123",
        )

        data = envelope.dict()
        assert isinstance(data, dict)
        assert data["error"] == "ValidationError"
        assert data["message"] == "Field is required"
        assert data["request_id"] == "req-123"
        assert "timestamp" in data


class TestValidationErrorDetail:
    """Test validation error detail model."""

    def test_validation_error_detail_creation(self):
        """Test creating validation error detail."""
        error = ValidationErrorDetail(
            field="tenant_id",
            message="Field is required",
            value=None,
            code="required",
            constraint="required"
        )

        assert error.field == "tenant_id"
        assert error.message == "Field is required"
        assert error.value is None
        assert error.code == "required"
        assert error.constraint == "required"

    def test_validation_error_detail_invalid_constraint(self):
        """Test validation error detail with invalid constraint."""
        with pytest.raises(ValueError, match="Invalid constraint type"):
            ValidationErrorDetail(
                field="tenant_id",
                message="Invalid constraint",
                constraint="invalid_constraint_type"
            )


class TestValidationErrorResponse:
    """Test validation error response model."""

    def test_validation_error_response_creation(self):
        """Test creating validation error response."""
        field_errors = [
            ValidationErrorDetail(
                field="tenant_id",
                message="Field is required",
                code="required"
            ),
            ValidationErrorDetail(
                field="name",
                message="Field cannot be empty",
                code="string_not_empty"
            )
        ]

        response = ValidationErrorResponse(
            message="Request validation failed",
            field_errors=field_errors,
            request_id="req-123"
        )

        assert response.error == "Validation Error"
        assert response.message == "Request validation failed"
        assert len(response.field_errors) == 2
        assert response.request_id == "req-123"
        assert isinstance(response.timestamp, datetime)

    def test_validation_error_response_to_dict(self):
        """Test validation error response serialization."""
        field_errors = [
            ValidationErrorDetail(
                field="tenant_id",
                message="Field is required"
            )
        ]

        response = ValidationErrorResponse(
            message="Validation failed",
            field_errors=field_errors
        )

        data = response.dict()
        assert data["error"] == "Validation Error"
        assert data["message"] == "Validation failed"
        assert len(data["field_errors"]) == 1
        assert data["field_errors"][0]["field"] == "tenant_id"


class TestCurveQueryParams:
    """Test curve query parameters validation."""

    def test_curve_query_params_valid(self):
        """Test valid curve query parameters."""
        params = CurveQueryParams(
            asof="2024-01-01",
            iso="CA",
            market="DA",
            location="ALBERTA",
            limit=100,
            offset=0
        )

        assert params.asof == "2024-01-01"
        assert params.iso == "CA"
        assert params.market == "DA"
        assert params.location == "ALBERTA"
        assert params.limit == 100
        assert params.offset == 0

    def test_curve_query_params_invalid_asof_format(self):
        """Test invalid asof date format."""
        with pytest.raises(ValueError, match="asof must be in YYYY-MM-DD format"):
            CurveQueryParams(asof="2024/01/01")

    def test_curve_query_params_invalid_iso_format(self):
        """Test invalid ISO code format."""
        with pytest.raises(ValueError, match="ISO code must contain only letters"):
            CurveQueryParams(iso="CA123")

    def test_curve_query_params_cursor_and_offset_conflict(self):
        """Test cursor and offset cannot be used together."""
        with pytest.raises(ValueError, match="Cannot use cursor and offset together"):
            CurveQueryParams(cursor="test-cursor", offset=10)

    def test_curve_query_params_multiple_cursors_conflict(self):
        """Test multiple cursor parameters cannot be used together."""
        with pytest.raises(ValueError, match="Cannot use multiple cursor parameters together"):
            CurveQueryParams(cursor="cursor1", since_cursor="cursor2")

    def test_curve_query_params_invalid_limit(self):
        """Test invalid limit values."""
        with pytest.raises(ValueError):
            CurveQueryParams(limit=0)  # Must be >= 1

        with pytest.raises(ValueError):
            CurveQueryParams(limit=600)  # Must be <= 500

    def test_curve_query_params_invalid_offset(self):
        """Test invalid offset values."""
        with pytest.raises(ValueError):
            CurveQueryParams(offset=-1)  # Must be >= 0

    def test_curve_query_params_cursor_too_long(self):
        """Test cursor length validation."""
        long_cursor = "x" * 1001
        with pytest.raises(ValueError, match="Cursor too long"):
            CurveQueryParams(cursor=long_cursor)


class TestCurveDiffQueryParams:
    """Test curve difference query parameters validation."""

    def test_curve_diff_query_params_valid(self):
        """Test valid curve diff query parameters."""
        params = CurveDiffQueryParams(
            asof_a="2024-01-01",
            asof_b="2024-01-02",
            iso="CA",
            market="DA",
            limit=100
        )

        assert params.asof_a == "2024-01-01"
        assert params.asof_b == "2024-01-02"
        assert params.iso == "CA"
        assert params.market == "DA"

    def test_curve_diff_query_params_same_dates_invalid(self):
        """Test same dates are invalid."""
        with pytest.raises(ValueError, match="asof_a and asof_b must be different dates"):
            CurveDiffQueryParams(asof_a="2024-01-01", asof_b="2024-01-01")

    def test_curve_diff_query_params_invalid_date_range(self):
        """Test invalid date range (too large)."""
        with pytest.raises(ValueError, match="Date range too large"):
            CurveDiffQueryParams(
                asof_a="2024-01-01",
                asof_b="2025-01-01"  # More than 365 days
            )

    def test_curve_diff_query_params_no_dimension_filters(self):
        """Test no dimension filters provided."""
        with pytest.raises(ValueError, match="At least one dimension filter must be specified"):
            CurveDiffQueryParams(asof_a="2024-01-01", asof_b="2024-01-02")

    def test_curve_diff_query_params_invalid_date_format(self):
        """Test invalid date format."""
        with pytest.raises(ValueError, match="Date must be in YYYY-MM-DD format"):
            CurveDiffQueryParams(asof_a="2024/01/01", asof_b="2024-01-02", iso="CA")


class TestScenarioCreateRequest:
    """Test scenario creation request validation."""

    def test_scenario_create_request_valid(self):
        """Test valid scenario creation request."""
        request = ScenarioCreateRequest(
            tenant_id="tenant-123",
            name="Test Scenario",
            assumptions=[
                {"type": "price", "value": 100},
                {"type": "growth", "value": 0.05}
            ],
            tags=["test", "example"]
        )

        assert request.tenant_id == "tenant-123"
        assert request.name == "Test Scenario"
        assert len(request.assumptions) == 2
        assert request.tags == ["test", "example"]

    def test_scenario_create_request_invalid_tenant_id(self):
        """Test invalid tenant ID format."""
        with pytest.raises(ValueError, match="tenant_id cannot be empty"):
            ScenarioCreateRequest(tenant_id="", name="Test Scenario")

    def test_scenario_create_request_invalid_tenant_id_characters(self):
        """Test invalid characters in tenant ID."""
        with pytest.raises(ValueError, match="tenant_id can only contain alphanumeric characters"):
            ScenarioCreateRequest(tenant_id="tenant@123", name="Test Scenario")

    def test_scenario_create_request_invalid_name(self):
        """Test invalid scenario name."""
        with pytest.raises(ValueError, match="scenario name cannot be empty"):
            ScenarioCreateRequest(tenant_id="tenant-123", name="")

    def test_scenario_create_request_invalid_tags(self):
        """Test invalid tags."""
        with pytest.raises(ValueError, match="Tags cannot be empty"):
            ScenarioCreateRequest(
                tenant_id="tenant-123",
                name="Test Scenario",
                tags=["valid", ""]
            )

    def test_scenario_create_request_invalid_tag_characters(self):
        """Test invalid characters in tags."""
        with pytest.raises(ValueError, match="Tags can only contain alphanumeric characters"):
            ScenarioCreateRequest(
                tenant_id="tenant-123",
                name="Test Scenario",
                tags=["valid", "invalid@tag"]
            )

    def test_scenario_create_request_invalid_assumptions(self):
        """Test invalid assumptions structure."""
        with pytest.raises(ValueError, match="Each assumption must have a 'type' field"):
            ScenarioCreateRequest(
                tenant_id="tenant-123",
                name="Test Scenario",
                assumptions=[{"value": 100}]  # Missing 'type'
            )

    def test_scenario_create_request_too_many_assumptions(self):
        """Test too many assumptions."""
        # This should fail due to the max_items constraint
        assumptions = [{"type": f"assumption_{i}", "value": i} for i in range(101)]

        with pytest.raises(Exception):  # Pydantic validation error
            ScenarioCreateRequest(
                tenant_id="tenant-123",
                name="Test Scenario",
                assumptions=assumptions
            )


class TestCurvePointValidation:
    """Test curve point validation."""

    def test_curve_point_valid(self):
        """Test valid curve point."""
        point = CurvePoint(
            curve_key="CA_DA_ALBERTA",
            tenor_label="CAL_2024",
            asof_date="2024-01-01",
            mid=100.50
        )

        assert point.curve_key == "CA_DA_ALBERTA"
        assert point.tenor_label == "CAL_2024"
        assert point.asof_date == "2024-01-01"
        assert point.mid == 100.50

    def test_curve_point_no_prices_invalid(self):
        """Test curve point with no prices is invalid."""
        with pytest.raises(ValueError, match="At least one price field"):
            CurvePoint(
                curve_key="CA_DA_ALBERTA",
                tenor_label="CAL_2024",
                asof_date="2024-01-01"
            )

    def test_curve_point_invalid_curve_key(self):
        """Test invalid curve key format."""
        with pytest.raises(ValueError, match="Curve key cannot be empty"):
            CurvePoint(
                curve_key="",
                tenor_label="CAL_2024",
                asof_date="2024-01-01",
                mid=100.50
            )

    def test_curve_point_invalid_price_range(self):
        """Test price value out of range."""
        with pytest.raises(ValueError, match="Price must be between -1e9 and 1e9"):
            CurvePoint(
                curve_key="CA_DA_ALBERTA",
                tenor_label="CAL_2024",
                asof_date="2024-01-01",
                mid=1e10  # Too large
            )

    def test_curve_point_invalid_curve_key_characters(self):
        """Test invalid characters in curve key."""
        with pytest.raises(ValueError, match="Curve key can only contain alphanumeric characters"):
            CurvePoint(
                curve_key="CA@DA#ALBERTA",
                tenor_label="CAL_2024",
                asof_date="2024-01-01",
                mid=100.50
            )


class TestCurveDiffPointValidation:
    """Test curve diff point validation."""

    def test_curve_diff_point_valid(self):
        """Test valid curve diff point."""
        point = CurveDiffPoint(
            curve_key="CA_DA_ALBERTA",
            tenor_label="CAL_2024",
            asof_a="2024-01-01",
            asof_b="2024-01-02",
            mid_a=100.50,
            mid_b=105.25
        )

        assert point.curve_key == "CA_DA_ALBERTA"
        assert point.asof_a == "2024-01-01"
        assert point.asof_b == "2024-01-02"
        assert point.mid_a == 100.50
        assert point.mid_b == 105.25

    def test_curve_diff_point_same_dates_invalid(self):
        """Test same dates are invalid."""
        with pytest.raises(ValueError, match="asof_a and asof_b must be different dates"):
            CurveDiffPoint(
                curve_key="CA_DA_ALBERTA",
                tenor_label="CAL_2024",
                asof_a="2024-01-01",
                asof_b="2024-01-01",
                mid_a=100.50,
                mid_b=105.25
            )

    def test_curve_diff_point_no_prices_invalid(self):
        """Test no price comparisons are invalid."""
        with pytest.raises(ValueError, match="At least one price comparison"):
            CurveDiffPoint(
                curve_key="CA_DA_ALBERTA",
                tenor_label="CAL_2024",
                asof_a="2024-01-01",
                asof_b="2024-01-02"
            )

    def test_curve_diff_point_invalid_diff_pct(self):
        """Test invalid percentage difference."""
        with pytest.raises(ValueError, match="Percentage difference must be between -1000 and 1000"):
            CurveDiffPoint(
                curve_key="CA_DA_ALBERTA",
                tenor_label="CAL_2024",
                asof_a="2024-01-01",
                asof_b="2024-01-02",
                mid_a=100,
                mid_b=105,
                diff_pct=2000  # Too large
            )


class TestExceptionHandling:
    """Test exception handling with error envelopes."""

    def test_validation_exception_handling(self):
        """Test ValidationException handling."""
        request = Mock()
        request.client = Mock()
        request.client.host = "127.0.0.1"

        exc = ValidationException(
            detail="Field is required",
            request_id="req-123",
            field_errors={"tenant_id": "Field is required"}
        )

        response = handle_api_exception(request, exc)
        assert response.status_code == 400

        detail = response.detail
        assert isinstance(detail, dict)
        assert detail["error"] == "ValidationException"
        assert detail["message"] == "Field is required"
        assert detail["context"]["field_errors"] == {"tenant_id": "Field is required"}

    def test_pydantic_validation_error_handling(self):
        """Test Pydantic ValidationError handling."""
        request = Mock()
        request.client = Mock()
        request.client.host = "127.0.0.1"

        # Create a mock Pydantic ValidationError
        class MockValidationError(Exception):
            def __init__(self):
                self.model = Mock()
                self.errors = lambda: [
                    {
                        "loc": ("tenant_id",),
                        "msg": "Field is required",
                        "input": None,
                        "type": "missing"
                    }
                ]

        exc = MockValidationError()

        response = handle_api_exception(request, exc)
        assert response.status_code == 400

        detail = response.detail
        assert isinstance(detail, dict)
        assert detail["error"] == "Validation Error"
        assert len(detail["field_errors"]) == 1
        assert detail["field_errors"][0]["field"] == "tenant_id"
        assert detail["field_errors"][0]["message"] == "Field is required"

    def test_http_exception_handling(self):
        """Test FastAPI HTTPException handling."""
        request = Mock()
        request.client = Mock()
        request.client.host = "127.0.0.1"

        exc = HTTPException(status_code=404, detail="Not found")

        response = handle_api_exception(request, exc)
        assert response.status_code == 404

        detail = response.detail
        assert isinstance(detail, dict)
        assert detail["error"] == "HTTPException"
        assert detail["message"] == "Not found"

    def test_value_error_handling(self):
        """Test ValueError handling."""
        request = Mock()
        request.client = Mock()
        request.client.host = "127.0.0.1"

        exc = ValueError("Invalid parameter")

        response = handle_api_exception(request, exc)
        assert response.status_code == 400

        detail = response.detail
        assert isinstance(detail, dict)
        assert detail["error"] == "ValueError"
        assert detail["message"] == "Invalid parameter"

    def test_internal_server_error_handling(self):
        """Test internal server error handling."""
        request = Mock()
        request.client = Mock()
        request.client.host = "127.0.0.1"

        exc = RuntimeError("Database connection failed")

        response = handle_api_exception(request, exc)
        assert response.status_code == 500

        detail = response.detail
        assert isinstance(detail, dict)
        assert detail["error"] == "InternalServerError"
        assert detail["message"] == "An unexpected error occurred"
        assert "context" in detail
        assert detail["context"]["type"] == "RuntimeError"
        assert detail["context"]["module"] == "builtins"


class TestMetaValidation:
    """Test metadata validation."""

    def test_meta_valid(self):
        """Test valid metadata."""
        meta = Meta(
            request_id="req-123",
            query_time_ms=150
        )

        assert meta.request_id == "req-123"
        assert meta.query_time_ms == 150

    def test_meta_invalid_query_time(self):
        """Test invalid query time."""
        with pytest.raises(ValueError):
            Meta(request_id="req-123", query_time_ms=-1)

    def test_meta_invalid_cursor_length(self):
        """Test invalid cursor length."""
        long_cursor = "x" * 1001
        with pytest.raises(ValueError, match="Cursor too long"):
            Meta(request_id="req-123", next_cursor=long_cursor)


class TestIntegrationScenarios:
    """Test integration scenarios for request validation."""

    def test_comprehensive_curve_query_validation(self):
        """Test comprehensive validation of curve query parameters."""
        # Valid case
        params = CurveQueryParams(
            asof="2024-01-01",
            iso="CA",
            limit=100,
            offset=0
        )
        assert params.asof == "2024-01-01"
        assert params.iso == "CA"

        # Invalid date format
        with pytest.raises(ValueError):
            CurveQueryParams(asof="2024/01/01")

        # Invalid pagination combination
        with pytest.raises(ValueError):
            CurveQueryParams(cursor="cursor", offset=10)

        # Invalid limit
        with pytest.raises(ValueError):
            CurveQueryParams(limit=0)

    def test_comprehensive_scenario_creation_validation(self):
        """Test comprehensive validation of scenario creation."""
        # Valid case
        request = ScenarioCreateRequest(
            tenant_id="tenant-123",
            name="Test Scenario",
            assumptions=[
                {"type": "price", "value": 100},
                {"type": "growth", "rate": 0.05}
            ],
            tags=["test", "example"]
        )
        assert request.tenant_id == "tenant-123"
        assert len(request.assumptions) == 2

        # Invalid tenant ID
        with pytest.raises(ValueError):
            ScenarioCreateRequest(tenant_id="", name="Test")

        # Invalid tags
        with pytest.raises(ValueError):
            ScenarioCreateRequest(
                tenant_id="tenant-123",
                name="Test",
                tags=["valid", ""]  # Empty tag
            )

        # Invalid assumptions
        with pytest.raises(ValueError):
            ScenarioCreateRequest(
                tenant_id="tenant-123",
                name="Test",
                assumptions=[{"value": 100}]  # Missing type
            )

    def test_error_envelope_serialization(self):
        """Test error envelope JSON serialization."""
        envelope = ErrorEnvelope(
            error="ValidationError",
            message="Field is required",
            field="tenant_id",
            context={"parameter": "tenant_id"},
            request_id="req-123"
        )

        # Should serialize to JSON without errors
        json_str = envelope.json()
        parsed = json.loads(json_str)

        assert parsed["error"] == "ValidationError"
        assert parsed["message"] == "Field is required"
        assert parsed["field"] == "tenant_id"
        assert parsed["context"]["parameter"] == "tenant_id"
        assert parsed["request_id"] == "req-123"

    def test_validation_error_response_serialization(self):
        """Test validation error response JSON serialization."""
        field_errors = [
            ValidationErrorDetail(
                field="tenant_id",
                message="Field is required",
                code="required"
            )
        ]

        response = ValidationErrorResponse(
            message="Request validation failed",
            field_errors=field_errors,
            request_id="req-123"
        )

        # Should serialize to JSON without errors
        json_str = response.json()
        parsed = json.loads(json_str)

        assert parsed["error"] == "Validation Error"
        assert parsed["message"] == "Request validation failed"
        assert len(parsed["field_errors"]) == 1
        assert parsed["field_errors"][0]["field"] == "tenant_id"
        assert parsed["request_id"] == "req-123"
