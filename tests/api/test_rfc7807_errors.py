"""Tests for RFC7807 compliant error responses."""

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from aurum.api.exceptions import (
    AurumAPIException,
    ValidationException,
    NotFoundException,
    ForbiddenException,
    ServiceUnavailableException,
    create_rfc7807_error_response
)
from aurum.api.models import ProblemDetail


@pytest.fixture
def test_app():
    """Create a test FastAPI app with RFC7807 error handling."""
    app = FastAPI()
    
    @app.get("/test/validation-error")
    async def validation_error():
        raise ValidationException(
            detail="Invalid input provided",
            request_id="test-123"
        )
    
    @app.get("/test/not-found")
    async def not_found():
        raise NotFoundException(
            resource_type="scenario",
            resource_id="test-scenario-123",
            request_id="test-123"
        )
    
    @app.get("/test/forbidden")
    async def forbidden():
        raise ForbiddenException(
            detail="Access denied to resource",
            request_id="test-123"
        )
    
    @app.get("/test/service-unavailable")
    async def service_unavailable():
        raise ServiceUnavailableException(
            service="database",
            request_id="test-123"
        )
    
    @app.get("/test/http-exception")
    async def http_exception():
        raise HTTPException(status_code=422, detail="Unprocessable entity")
    
    @app.get("/test/generic-error")
    async def generic_error():
        raise ValueError("Something went wrong")
    
    # Add RFC7807 exception handler
    from aurum.api.app import _api_exception_handler
    app.add_exception_handler(Exception, _api_exception_handler)
    
    return app


@pytest.fixture
def client(test_app):
    """Create test client."""
    return TestClient(test_app)


class TestRFC7807Compliance:
    """Test RFC7807 Problem Details compliance."""
    
    def test_validation_exception_rfc7807_format(self, client):
        """Test that ValidationException returns RFC7807 compliant response."""
        response = client.get("/test/validation-error")
        
        assert response.status_code == 400
        assert response.headers["content-type"] == "application/problem+json"
        
        error_data = response.json()
        
        # Required RFC7807 fields
        assert "type" in error_data
        assert "title" in error_data
        assert "status" in error_data
        
        # Check specific values
        assert error_data["status"] == 400
        assert "validation-error" in error_data["type"]
        assert error_data["title"] == "Validation Error"
        assert error_data["detail"] == "Invalid input provided"
        
        # Aurum-specific fields
        assert "request_id" in error_data
        assert "timestamp" in error_data
        assert error_data["request_id"] == "test-123"
    
    def test_not_found_exception_rfc7807_format(self, client):
        """Test that NotFoundException returns RFC7807 compliant response."""
        response = client.get("/test/not-found")
        
        assert response.status_code == 404
        assert response.headers["content-type"] == "application/problem+json"
        
        error_data = response.json()
        
        # Required RFC7807 fields
        assert "type" in error_data
        assert "title" in error_data  
        assert "status" in error_data
        
        # Check specific values
        assert error_data["status"] == 404
        assert "not-found" in error_data["type"]
        assert error_data["title"] == "Not Found Error"
        assert "scenario 'test-scenario-123' not found" in error_data["detail"]
        
        # Check instance field
        assert "instance" in error_data
        assert "/test/not-found" in error_data["instance"]
    
    def test_forbidden_exception_rfc7807_format(self, client):
        """Test that ForbiddenException returns RFC7807 compliant response."""
        response = client.get("/test/forbidden")
        
        assert response.status_code == 403
        assert response.headers["content-type"] == "application/problem+json"
        
        error_data = response.json()
        
        assert error_data["status"] == 403
        assert "forbidden" in error_data["type"]
        assert error_data["title"] == "Forbidden Error"
        assert error_data["detail"] == "Access denied to resource"
    
    def test_service_unavailable_rfc7807_format(self, client):
        """Test that ServiceUnavailableException returns RFC7807 compliant response."""
        response = client.get("/test/service-unavailable")
        
        assert response.status_code == 503
        assert response.headers["content-type"] == "application/problem+json"
        
        error_data = response.json()
        
        assert error_data["status"] == 503
        assert "service-unavailable" in error_data["type"]
        assert error_data["title"] == "Service Unavailable Error"
        assert "database" in error_data["detail"]
    
    def test_http_exception_rfc7807_format(self, client):
        """Test that HTTPException returns RFC7807 compliant response."""
        response = client.get("/test/http-exception")
        
        assert response.status_code == 422
        assert response.headers["content-type"] == "application/problem+json"
        
        error_data = response.json()
        
        assert error_data["status"] == 422
        assert error_data["type"] == "about:blank"  # Standard for HTTP exceptions
        assert error_data["title"] == "Unprocessable Entity"
        assert error_data["detail"] == "Unprocessable entity"
    
    def test_generic_exception_rfc7807_format(self, client):
        """Test that generic exceptions return RFC7807 compliant response."""
        response = client.get("/test/generic-error")
        
        assert response.status_code == 500
        assert response.headers["content-type"] == "application/problem+json"
        
        error_data = response.json()
        
        assert error_data["status"] == 500
        assert error_data["type"] == "about:blank"
        assert error_data["title"] == "Internal Server Error"
        assert error_data["detail"] == "An unexpected error occurred"
    
    def test_error_response_excludes_none_values(self, client):
        """Test that error responses exclude None values."""
        response = client.get("/test/validation-error")
        error_data = response.json()
        
        # Should not include None values
        for key, value in error_data.items():
            assert value is not None
    
    def test_error_response_includes_timestamp(self, client):
        """Test that error responses include timestamp."""
        response = client.get("/test/validation-error")
        error_data = response.json()
        
        assert "timestamp" in error_data
        # Basic ISO format check
        assert "T" in error_data["timestamp"]
        assert "Z" in error_data["timestamp"] or "+" in error_data["timestamp"]


class TestProblemDetailModel:
    """Test the ProblemDetail Pydantic model."""
    
    def test_problem_detail_model_validation(self):
        """Test ProblemDetail model validation."""
        problem = ProblemDetail(
            type="https://api.aurum.com/problems/validation-error",
            title="Validation Error",
            status=400,
            detail="Request validation failed",
            instance="/v2/scenarios",
            request_id="req-123"
        )
        
        assert problem.type == "https://api.aurum.com/problems/validation-error"
        assert problem.title == "Validation Error"
        assert problem.status == 400
        assert problem.detail == "Request validation failed"
        assert problem.instance == "/v2/scenarios"
        assert problem.request_id == "req-123"
        assert problem.timestamp is not None
    
    def test_problem_detail_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError):
            ProblemDetail()  # Missing required fields
    
    def test_problem_detail_status_validation(self):
        """Test status code validation."""
        # Valid status codes
        ProblemDetail(type="test", title="Test", status=200)
        ProblemDetail(type="test", title="Test", status=400)
        ProblemDetail(type="test", title="Test", status=500)
        
        # Invalid status codes
        with pytest.raises(ValidationError):
            ProblemDetail(type="test", title="Test", status=99)  # Too low
        
        with pytest.raises(ValidationError):
            ProblemDetail(type="test", title="Test", status=600)  # Too high


class TestErrorConsistencyAcrossVersions:
    """Test that error responses are consistent across v1 and v2."""
    
    def test_v1_v2_error_format_consistency(self):
        """Test that v1 and v2 endpoints return consistent error formats."""
        # This test would need actual v1 and v2 endpoints to be meaningful
        # For now, we'll test that our RFC7807 handler works consistently
        
        app = FastAPI()
        
        @app.get("/v1/test")
        async def v1_endpoint():
            raise ValidationException("V1 validation error")
        
        @app.get("/v2/test") 
        async def v2_endpoint():
            raise ValidationException("V2 validation error")
        
        from aurum.api.app import _api_exception_handler
        app.add_exception_handler(Exception, _api_exception_handler)
        
        client = TestClient(app)
        
        v1_response = client.get("/v1/test")
        v2_response = client.get("/v2/test")
        
        # Both should have same structure
        v1_data = v1_response.json()
        v2_data = v2_response.json()
        
        # Same fields should be present
        assert set(v1_data.keys()) == set(v2_data.keys())
        
        # Same status codes
        assert v1_response.status_code == v2_response.status_code
        
        # Same content type
        assert v1_response.headers["content-type"] == v2_response.headers["content-type"]
        assert v1_response.headers["content-type"] == "application/problem+json"


if __name__ == "__main__":
    pytest.main([__file__])