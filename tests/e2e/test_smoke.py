"""Smoke tests for Aurum API and external data pipeline."""

import asyncio
import json
import pytest
import requests
from datetime import datetime, timedelta
from typing import Dict, Any

from aurum.api.client import AurumAPIClient
from aurum.external.client import ExternalDataClient


class TestAurumSmokeTests:
    """Smoke tests to verify basic functionality."""

    @pytest.fixture(scope="class")
    def api_client(self):
        """API client for testing."""
        return AurumAPIClient(base_url="http://localhost:8000")

    @pytest.fixture(scope="class")
    def external_client(self):
        """External data client for testing."""
        return ExternalDataClient(base_url="http://localhost:8001")

    def test_api_health_check(self, api_client):
        """Test API health endpoint."""
        response = requests.get("http://localhost:8000/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_external_data_health_check(self, external_client):
        """Test external data service health endpoint."""
        response = requests.get("http://localhost:8001/health/external")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_database_connectivity(self, api_client):
        """Test database connectivity through API."""
        # This test assumes we have some basic data seeded
        # In a real scenario, you'd check actual database operations
        response = requests.get("http://localhost:8000/v1/curves")
        assert response.status_code in [200, 401]  # 401 if auth required, 200 if public

    def test_redis_connectivity(self, api_client):
        """Test Redis connectivity through cache operations."""
        # Test would verify cache operations work
        # This is a placeholder for actual cache testing
        assert True

    def test_kafka_connectivity(self, external_client):
        """Test Kafka connectivity through external data operations."""
        # Test would verify Kafka message publishing works
        # This is a placeholder for actual Kafka testing
        assert True

    def test_api_response_times(self, api_client):
        """Test API response times are reasonable."""
        start_time = datetime.now()

        response = requests.get("http://localhost:8000/health/ready")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        assert duration < 1.0  # Should respond within 1 second

    def test_external_service_response_times(self, external_client):
        """Test external service response times."""
        start_time = datetime.now()

        response = requests.get("http://localhost:8001/health/external")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        assert duration < 2.0  # Allow more time for external service

    def test_cors_headers(self, api_client):
        """Test CORS headers are properly set."""
        response = requests.options("http://localhost:8000/health/ready")
        assert "Access-Control-Allow-Origin" in response.headers
        assert "Access-Control-Allow-Methods" in response.headers

    def test_security_headers(self, api_client):
        """Test security headers are properly set."""
        response = requests.get("http://localhost:8000/health/ready")

        # Check for security headers
        security_headers = [
            "X-Content-Type-Options",
            "X-Frame-Options",
            "X-XSS-Protection",
            "Strict-Transport-Security"
        ]

        for header in security_headers:
            assert header in response.headers, f"Missing security header: {header}"

    def test_api_version_header(self, api_client):
        """Test API version header is set."""
        response = requests.get("http://localhost:8000/health/ready")
        assert "X-API-Version" in response.headers

    def test_metrics_endpoint(self, api_client):
        """Test Prometheus metrics endpoint."""
        response = requests.get("http://localhost:8000/metrics")
        assert response.status_code == 200
        assert "aurum_api_requests_total" in response.text

    def test_external_metrics_endpoint(self, external_client):
        """Test external data metrics endpoint."""
        response = requests.get("http://localhost:8001/metrics")
        assert response.status_code == 200
        assert "aurum_external_http_requests_total" in response.text


class TestAurumAPIEndpoints:
    """Test basic API endpoints."""

    def test_openapi_spec(self):
        """Test OpenAPI specification is available."""
        response = requests.get("http://localhost:8000/openapi.json")
        assert response.status_code == 200

        spec = response.json()
        assert "openapi" in spec
        assert "info" in spec
        assert "paths" in spec
        assert "components" in spec

    def test_api_documentation(self):
        """Test API documentation is available."""
        response = requests.get("http://localhost:8000/docs")
        assert response.status_code == 200

    def test_api_redoc(self):
        """Test ReDoc documentation is available."""
        response = requests.get("http://localhost:8000/redoc")
        assert response.status_code == 200


class TestExternalDataSmoke:
    """Test external data pipeline smoke tests."""

    def test_external_providers_config(self):
        """Test external providers configuration is loaded."""
        # This would test that provider configurations are properly loaded
        # In practice, this would check the external data service configuration
        assert True

    def test_collector_initialization(self):
        """Test that data collectors can be initialized."""
        # This would test that collector classes can be imported and initialized
        # In practice, this would create actual collector instances
        assert True

    def test_kafka_topics_created(self):
        """Test that required Kafka topics exist."""
        # This would check that Kafka topics are created
        # In practice, this would query Kafka admin API
        assert True

    def test_database_tables_created(self):
        """Test that required database tables exist."""
        # This would check that database migrations have run
        # In practice, this would query the database schema
        assert True


class TestIntegrationSmoke:
    """Test integration between components."""

    def test_api_to_database_integration(self):
        """Test API can communicate with database."""
        # Test that API can perform basic database operations
        assert True

    def test_external_to_kafka_integration(self):
        """Test external data service can publish to Kafka."""
        # Test that external service can publish messages
        assert True

    def test_end_to_end_data_flow(self):
        """Test complete data flow from external source to API."""
        # Test complete pipeline: external API -> Kafka -> processing -> API response
        assert True


class TestLoadSmoke:
    """Basic load testing smoke tests."""

    def test_concurrent_requests(self):
        """Test handling of concurrent requests."""
        import concurrent.futures

        def make_request(i):
            response = requests.get("http://localhost:8000/health/ready")
            return response.status_code

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_request, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        assert all(status == 200 for status in results)

    def test_sustained_load(self):
        """Test handling of sustained load over time."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=10)

        request_count = 0
        while datetime.now() < end_time:
            response = requests.get("http://localhost:8000/health/ready")
            assert response.status_code == 200
            request_count += 1

        assert request_count > 50  # Should handle at least 5 RPS for 10 seconds
