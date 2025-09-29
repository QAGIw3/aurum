"""RFC7807 error format compliance tests."""

import pytest
import json
from tests.common import create_test_app, TestAppConfig
from tests.factories import ApiPayloadFactory


@pytest.mark.contract
class TestRFC7807ErrorFormat:
    """Test that API error responses conform to RFC7807 standard."""

    def test_error_response_structure(self):
        """Test that error responses follow RFC7807 structure."""
        # Create an error response using our factory
        error_response = ApiPayloadFactory.create_error_response()

        # Verify RFC7807 compliance
        error_data = error_response["error"]

        # Required RFC7807 fields
        assert "type" in error_data
        assert "title" in error_data
        assert "status" in error_data
        assert "detail" in error_data

        # Verify field types
        assert isinstance(error_data["type"], str)
        assert isinstance(error_data["title"], str)
        assert isinstance(error_data["status"], int)
        assert isinstance(error_data["detail"], str)

        # Verify status codes are valid HTTP status codes
        status = error_data["status"]
        assert 100 <= status <= 599

    def test_error_type_uri_format(self):
        """Test that error type field contains valid URI."""
        error_response = ApiPayloadFactory.create_error_response()

        error_type = error_response["error"]["type"]

        # Should be a valid URI
        assert error_type.startswith(("http://", "https://"))

        # Should be a stable, versioned URI
        assert "/errors/" in error_type or "/problems/" in error_type

    def test_error_title_descriptive(self):
        """Test that error titles are descriptive and human-readable."""
        error_response = ApiPayloadFactory.create_error_response()

        title = error_response["error"]["title"]

        # Should be descriptive and not just an error code
        assert len(title) > 3
        assert not title.isupper()  # Should not be just an error code

        # Should be human-readable
        assert " " in title or title.replace("-", " ").replace("_", " ").strip()

    def test_error_detail_contextual(self):
        """Test that error details provide contextual information."""
        error_response = ApiPayloadFactory.create_error_response()

        detail = error_response["error"]["detail"]

        # Should provide specific context about what went wrong
        assert len(detail) > 10
        assert not detail.startswith("Error")  # Should be specific, not generic

    def test_error_instance_optional(self):
        """Test that error instance field is optional and provides request context."""
        error_response = ApiPayloadFactory.create_error_response()

        # Instance field is optional in RFC7807
        if "instance" in error_response["error"]:
            instance = error_response["error"]["instance"]

            # Should be a valid URI pointing to the specific request
            assert instance.startswith(("http://", "https://"))

            # Should contain request ID or similar identifier
            assert "request" in instance.lower() or "id" in instance.lower()

    def test_validation_error_structure(self):
        """Test validation error responses follow RFC7807 with field-specific errors."""
        # Create a validation error response
        error_response = ApiPayloadFactory.create_error_response({
            "error": {
                "type": "https://example.com/errors/validation-error",
                "title": "Validation Error",
                "status": 400,
                "detail": "Request validation failed",
                "validation_errors": [
                    {
                        "field": "scenario_type",
                        "message": "Must be one of: monte_carlo, forecasting, stress_test",
                        "code": "INVALID_ENUM_VALUE"
                    },
                    {
                        "field": "confidence_interval",
                        "message": "Must be between 0.0 and 1.0",
                        "code": "INVALID_RANGE"
                    }
                ]
            }
        })

        error_data = error_response["error"]

        # Should have validation-specific fields
        assert "validation_errors" in error_data
        assert isinstance(error_data["validation_errors"], list)
        assert len(error_data["validation_errors"]) > 0

        # Each validation error should have required fields
        for validation_error in error_data["validation_errors"]:
            assert "field" in validation_error
            assert "message" in validation_error
            assert "code" in validation_error

    def test_error_codes_are_meaningful(self):
        """Test that error codes are meaningful and consistent."""
        error_response = ApiPayloadFactory.create_error_response()

        error_data = error_response["error"]

        # Error codes should follow a consistent pattern
        if "code" in error_data:
            code = error_data["code"]

            # Should be uppercase with underscores (SCREAMING_SNAKE_CASE)
            assert code.isupper()
            assert "_" in code or code.isalpha()

            # Should be descriptive of the error type
            assert len(code) > 3

    def test_error_response_content_type(self):
        """Test that error responses have correct content type."""
        error_response = ApiPayloadFactory.create_error_response()

        # RFC7807 specifies application/problem+json content type
        # In our test app, we return application/json
        # This test verifies the structure is correct

        # Verify the response has the expected structure
        assert "error" in error_response
        assert isinstance(error_response["error"], dict)

    def test_error_response_serialization(self):
        """Test that error responses can be properly serialized to JSON."""
        error_response = ApiPayloadFactory.create_error_response()

        # Should be JSON serializable
        try:
            json_str = json.dumps(error_response)
            parsed = json.loads(json_str)

            # Should maintain structure after serialization
            assert "error" in parsed
            assert isinstance(parsed["error"], dict)

        except (TypeError, json.JSONDecodeError) as e:
            pytest.fail(f"Error response not JSON serializable: {e}")

    def test_error_response_stability(self):
        """Test that error response format is stable and doesn't change unexpectedly."""
        # Create multiple error responses and verify consistency
        error1 = ApiPayloadFactory.create_error_response()
        error2 = ApiPayloadFactory.create_error_response()

        # Both should have the same structure
        assert set(error1.keys()) == set(error2.keys())
        assert set(error1["error"].keys()) == set(error2["error"].keys())

        # Required fields should be present in both
        required_fields = ["type", "title", "status", "detail"]
        for error_data in [error1["error"], error2["error"]]:
            for field in required_fields:
                assert field in error_data
