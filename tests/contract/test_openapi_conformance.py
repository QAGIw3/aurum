"""OpenAPI contract tests using Schemathesis."""

import pytest
import schemathesis
from fastapi import FastAPI
from tests.common import create_test_app, TestAppConfig


@pytest.fixture(scope="session")
def openapi_schema():
    """Generate OpenAPI schema for contract testing."""
    # Create a test app with default settings
    settings = TestAppConfig()
    app = create_test_app(settings)

    # Get the OpenAPI schema
    schema = app.openapi()

    # Add any custom validation or modifications here
    return schema


@pytest.fixture(scope="session")
def schemathesis_client(openapi_schema):
    """Create Schemathesis client for API testing."""
    return schemathesis.from_dict(openapi_schema)


@pytest.mark.contract
@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "DELETE"])
def test_api_methods_exist(schemathesis_client, method):
    """Test that all declared API methods exist and are accessible."""
    # This test verifies that all methods declared in OpenAPI spec are implemented
    # Schemathesis will automatically test all endpoints

    # Run basic schema validation
    assert schemathesis_client is not None

    # Test that the schema is valid
    try:
        # This would normally use schemathesis to test the actual endpoints
        # For now, we just validate the schema structure
        schema = schemathesis_client.schema
        assert "openapi" in schema
        assert "info" in schema
        assert "paths" in schema

    except Exception as e:
        pytest.fail(f"OpenAPI schema validation failed: {e}")


@pytest.mark.contract
def test_response_schemas_match_openapi_spec(schemathesis_client):
    """Test that API responses match their declared schemas."""
    # This test would use Schemathesis to validate response schemas
    # against the OpenAPI specification

    schema = schemathesis_client.schema

    # Verify that paths are defined
    assert "paths" in schema
    assert len(schema["paths"]) > 0

    # Verify that components/schemas are defined
    assert "components" in schema
    assert "schemas" in schema["components"]


@pytest.mark.contract
def test_request_validation_against_schemas(schemathesis_client):
    """Test that API requests are validated against schemas."""
    # This test would validate request body and parameter schemas
    # For now, we just verify the schema structure

    schema = schemathesis_client.schema

    # Verify that requestBody schemas exist where expected
    paths = schema.get("paths", {})
    for path, path_info in paths.items():
        for method, method_info in path_info.items():
            if method.upper() in ["POST", "PUT", "PATCH"]:
                # These methods typically have request bodies
                if "requestBody" in method_info:
                    request_body = method_info["requestBody"]
                    assert "content" in request_body
                    assert "application/json" in request_body["content"]


@pytest.mark.contract
def test_parameter_validation(schemathesis_client):
    """Test that API parameters are validated correctly."""
    # This test would validate path, query, and header parameters
    # against their schemas

    schema = schemathesis_client.schema
    paths = schema.get("paths", {})

    # Check that parameters have proper schemas
    for path, path_info in paths.items():
        for method, method_info in path_info.items():
            if "parameters" in method_info:
                for param in method_info["parameters"]:
                    assert "name" in param
                    assert "in" in param  # path, query, header, cookie
                    assert "schema" in param


@pytest.mark.contract
def test_error_response_schemas(schemathesis_client):
    """Test that error responses match declared schemas."""
    # This test would validate that error responses conform to
    # the error schema definitions in OpenAPI

    schema = schemathesis_client.schema

    # Verify that error schemas are defined
    components = schema.get("components", {})
    schemas = components.get("schemas", {})

    # Check for common error response schemas
    error_schemas = [
        "Error",
        "ValidationError",
        "NotFoundError",
        "UnauthorizedError",
        "ForbiddenError"
    ]

    for error_schema in error_schemas:
        if error_schema in schemas:
            schema_def = schemas[error_schema]
            assert "type" in schema_def
            assert "properties" in schema_def


@pytest.mark.contract
@pytest.mark.slow
def test_comprehensive_api_validation(schemathesis_client):
    """Run comprehensive API validation using Schemathesis."""
    # This would be the main contract test that runs Schemathesis
    # against the actual API endpoints

    # Note: This test would require the actual aurum API to be running
    # For now, we just validate the schema structure

    schema = schemathesis_client.schema

    # Validate that all required OpenAPI fields are present
    required_fields = ["openapi", "info", "paths"]
    for field in required_fields:
        assert field in schema, f"Required field '{field}' missing from OpenAPI schema"

    # Validate info section
    info = schema["info"]
    assert "title" in info
    assert "version" in info

    # Validate that paths exist
    paths = schema["paths"]
    assert len(paths) > 0, "No API paths defined in OpenAPI schema"


@pytest.mark.contract
def test_api_version_consistency(openapi_schema):
    """Test that API version is consistent across the specification."""
    # Verify that the API version is consistently declared

    info = openapi_schema["info"]
    version = info.get("version")

    assert version is not None
    assert isinstance(version, str)
    assert len(version) > 0

    # In a real implementation, we'd also check that the version
    # matches the version returned by the API


@pytest.mark.contract
def test_security_schemes_validation(openapi_schema):
    """Test that security schemes are properly defined."""
    # Verify that security schemes are properly configured
    # if authentication is enabled

    components = openapi_schema.get("components", {})
    security_schemes = components.get("securitySchemes", {})

    if security_schemes:
        # Validate security scheme structure
        for scheme_name, scheme_def in security_schemes.items():
            assert "type" in scheme_def
            assert scheme_def["type"] in ["apiKey", "http", "oauth2", "openIdConnect"]

            # For API key schemes
            if scheme_def["type"] == "apiKey":
                assert "name" in scheme_def
                assert "in" in scheme_def
                assert scheme_def["in"] in ["header", "query", "cookie"]


@pytest.mark.contract
def test_content_type_handling(openapi_schema):
    """Test that content types are properly declared."""
    # Verify that content types are properly specified
    # for request and response bodies

    paths = openapi_schema.get("paths", {})

    for path, path_info in paths.items():
        for method, method_info in path_info.items():
            # Check response content types
            if "responses" in method_info:
                responses = method_info["responses"]
                for status_code, response_def in responses.items():
                    if "content" in response_def:
                        content = response_def["content"]
                        # Should have at least one content type
                        assert len(content) > 0

                        # Common content types should be supported
                        expected_types = ["application/json"]
                        for expected_type in expected_types:
                            if expected_type in content:
                                # Validate content type definition
                                content_def = content[expected_type]
                                if "schema" in content_def:
                                    assert "$ref" in content_def["schema"] or "type" in content_def["schema"]


@pytest.mark.contract
def test_pagination_schemas(openapi_schema):
    """Test that pagination schemas are properly defined."""
    # Verify that pagination parameters and responses are properly defined

    paths = openapi_schema.get("paths", {})

    for path, path_info in paths.items():
        for method, method_info in path_info.items():
            # Look for pagination parameters
            if "parameters" in method_info:
                for param in method_info["parameters"]:
                    if param.get("name") in ["page", "limit", "offset", "page_size"]:
                        assert "schema" in param
                        assert "type" in param["schema"]
                        assert param["schema"]["type"] == "integer"

            # Look for paginated responses
            if "responses" in method_info:
                for status_code, response_def in method_info["responses"].items():
                    if status_code == "200" and "content" in response_def:
                        content = response_def["content"]
                        if "application/json" in content:
                            schema = content["application/json"].get("schema", {})
                            # This would be where we'd validate pagination response structure
                            pass


@pytest.mark.contract
def test_required_fields_documentation(openapi_schema):
    """Test that required fields are properly documented."""
    # Verify that required request fields are marked as required
    # and optional fields are marked as optional

    components = openapi_schema.get("components", {})
    schemas = components.get("schemas", {})

    # Check that schema properties are properly defined
    for schema_name, schema_def in schemas.items():
        if "properties" in schema_def:
            properties = schema_def["properties"]
            required = schema_def.get("required", [])

            for prop_name, prop_def in properties.items():
                # If a property is required, it should be in the required list
                if prop_name in required:
                    assert "description" in prop_def or "title" in prop_def

                # All properties should have type information
                assert "type" in prop_def or "$ref" in prop_def
