"""API endpoint performance regression tests."""

import pytest
import asyncio
import time
from typing import Dict, Any
from tests.common import create_test_app, TestAppConfig
from tests.factories import ApiPayloadFactory


@pytest.mark.perf
class TestAPIEndpointPerformance:
    """Performance regression tests for API endpoints."""

    @pytest.fixture(scope="class")
    def api_app(self):
        """Create a test API app for performance testing."""
        settings = TestAppConfig()
        return create_test_app(settings)

    def test_health_endpoint_performance(self, api_app, benchmark):
        """Benchmark the health endpoint response time."""
        from fastapi.testclient import TestClient

        client = TestClient(api_app)

        # Benchmark the endpoint
        result = benchmark(client.get, "/health")

        # Verify the response is successful
        assert result.status_code == 200

        # In a real implementation, we'd define performance budgets
        # For now, we just ensure the endpoint responds

    def test_metadata_endpoint_performance(self, api_app, benchmark):
        """Benchmark the metadata endpoint response time."""
        from fastapi.testclient import TestClient

        client = TestClient(api_app)

        # Benchmark the endpoint
        result = benchmark(client.get, "/v1/metadata/units")

        # Verify the response is successful
        assert result.status_code == 200
        assert "data" in result.json()

    def test_json_serialization_performance(self, benchmark):
        """Benchmark JSON serialization performance."""
        # Create test data
        test_data = ApiPayloadFactory.create_success_response({
            "data": {
                "scenarios": [
                    {
                        "id": f"scenario-{i}",
                        "name": f"Test Scenario {i}",
                        "status": "completed",
                        "created_at": "2023-01-01T00:00:00Z"
                    }
                    for i in range(100)  # Large dataset
                ],
                "pagination": {
                    "page": 1,
                    "page_size": 100,
                    "total": 1000,
                    "total_pages": 10
                }
            }
        })

        # Benchmark JSON serialization
        result = benchmark(lambda: __import__("json").dumps(test_data))

        # Verify serialization works
        assert isinstance(result, str)
        assert len(result) > 100

    def test_data_processing_performance(self, benchmark):
        """Benchmark data processing operations."""
        # Create test data for processing
        test_data = {
            "scenarios": [
                {
                    "id": f"scenario-{i}",
                    "results": {
                        "expected_return": __import__("random").uniform(-0.1, 0.3),
                        "volatility": __import__("random").uniform(0.1, 0.5),
                        "sharpe_ratio": __import__("random").uniform(-1, 3),
                    }
                }
                for i in range(1000)  # Large dataset
            ]
        }

        # Benchmark data processing
        def process_data():
            scenarios = test_data["scenarios"]
            processed = []

            for scenario in scenarios:
                results = scenario["results"]

                # Simulate complex calculations
                risk_adjusted_return = results["expected_return"] / max(results["volatility"], 0.01)
                downside_risk = max(0, -results["expected_return"])
                sortino_ratio = results["expected_return"] / max(downside_risk, 0.01)

                processed.append({
                    "id": scenario["id"],
                    "risk_adjusted_return": risk_adjusted_return,
                    "downside_risk": downside_risk,
                    "sortino_ratio": sortino_ratio,
                    "performance_score": results["sharpe_ratio"] * risk_adjusted_return,
                })

            return processed

        result = benchmark(process_data)

        # Verify processing works
        assert len(result) == 1000
        assert all("risk_adjusted_return" in item for item in result)

    def test_memory_allocation_performance(self, benchmark):
        """Benchmark memory allocation patterns."""
        def allocate_memory():
            # Simulate memory allocation patterns
            data = []
            for i in range(1000):
                item = {
                    "id": f"item-{i}",
                    "data": [__import__("random").random() for _ in range(100)],
                    "metadata": {
                        "timestamp": time.time(),
                        "version": "1.0",
                        "tags": [f"tag-{j}" for j in range(5)],
                    }
                }
                data.append(item)
            return data

        result = benchmark(allocate_memory)

        # Verify allocation works
        assert len(result) == 1000
        assert all("data" in item for item in result)

    def test_concurrent_request_performance(self, api_app, benchmark):
        """Benchmark concurrent request handling."""
        from fastapi.testclient import TestClient
        import threading
        import queue

        client = TestClient(api_app)

        def make_request(request_id):
            """Make a single request."""
            try:
                response = client.get("/health")
                return request_id, response.status_code
            except Exception as e:
                return request_id, str(e)

        def concurrent_requests():
            """Make concurrent requests."""
            results = []
            threads = []

            for i in range(10):  # 10 concurrent requests
                thread = threading.Thread(target=lambda: results.append(make_request(i)))
                threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            return results

        result = benchmark(concurrent_requests)

        # Verify concurrent requests work
        assert len(result) == 10
        assert all(status == 200 for _, status in result)

    def test_database_query_performance_simulation(self, benchmark):
        """Benchmark simulated database query performance."""
        def simulate_query():
            """Simulate a database query operation."""
            # Simulate query planning and execution
            query_plan = {
                "type": "SELECT",
                "table": "scenarios",
                "conditions": [
                    {"field": "status", "operator": "IN", "values": ["completed", "running"]},
                    {"field": "created_at", "operator": ">=", "value": "2023-01-01"},
                ],
                "order_by": [{"field": "created_at", "direction": "DESC"}],
                "limit": 100,
            }

            # Simulate query execution
            results = []
            for i in range(100):
                result = {
                    "id": f"scenario-{i}",
                    "name": f"Test Scenario {i}",
                    "status": __import__("random").choice(["completed", "running", "failed"]),
            "created_at": f"2023-01-{i:02d}T00:00:00Z",
                    "duration": __import__("random").uniform(1, 100),
                }
                results.append(result)

            return results

        result = benchmark(simulate_query)

        # Verify query simulation works
        assert len(result) == 100
        assert all("id" in item for item in result)

    def test_api_response_time_distribution(self, api_app):
        """Test API response time distribution and percentiles."""
        from fastapi.testclient import TestClient

        client = TestClient(api_app)

        # Collect response times for multiple requests
        response_times = []
        for _ in range(100):
            start_time = time.time()
            response = client.get("/health")
            end_time = time.time()

            assert response.status_code == 200
            response_times.append((end_time - start_time) * 1000)  # Convert to milliseconds

        # Calculate statistics
        import statistics

        mean_time = statistics.mean(response_times)
        median_time = statistics.median(response_times)
        p95_time = statistics.quantiles(response_times, n=20)[18]  # 95th percentile
        p99_time = statistics.quantiles(response_times, n=100)[98]  # 99th percentile

        # Verify performance meets basic requirements
        assert mean_time < 100  # Average should be under 100ms
        assert p95_time < 200   # 95th percentile should be under 200ms
        assert p99_time < 500   # 99th percentile should be under 500ms

        # In a real implementation, these would be configurable thresholds
        # and the test would fail if performance degrades beyond acceptable limits
