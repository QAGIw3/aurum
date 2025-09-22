"""Locust performance test targeting high-traffic v2 endpoints and Trino-backed paths."""

from __future__ import annotations

import os
from locust import HttpUser, task, between


def _headers() -> dict[str, str]:
    token = os.getenv("AURUM_API_TOKEN")
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


class AurumApiUser(HttpUser):
    """Simulates API clients issuing read-heavy workloads."""

    wait_time = between(0.2, 1.0)

    def on_start(self) -> None:  # pragma: no cover - locust hook
        self.headers = _headers()
        self.query_params = {
            "iso": os.getenv("AURUM_PERF_ISO", "ISO-NE"),
            "market": os.getenv("AURUM_PERF_MARKET", "DAY_AHEAD"),
            "location": os.getenv("AURUM_PERF_LOCATION", "HUB"),
            "limit": os.getenv("AURUM_PERF_LIMIT", "200"),
        }

    @task(5)
    def get_curves(self):
        """Hot-path v2 curves endpoint with caching + Trino access."""
        params = {**self.query_params, "asof": os.getenv("AURUM_PERF_ASOF")}
        self.client.get(
            "/v2/curves",
            params=params,
            headers=self.headers,
            name="GET /v2/curves",
        )

    @task(1)
    def export_curves(self):
        """Exercise streaming export path to validate backpressure handling."""
        params = {**self.query_params, "chunk_size": os.getenv("AURUM_PERF_CHUNK", "1500")}
        with self.client.get(
            "/v2/curves/export",
            params=params,
            headers=self.headers,
            stream=True,
            name="GET /v2/curves/export",
        ) as response:
            for _ in response.iter_content(chunk_size=8192):
                break  # drain first chunk to exercise streaming code path

    @task(1)
    def trino_health(self):
        """Check Trino client health endpoint for pool metrics."""
        self.client.get(
            "/v1/admin/trino/health",
            headers=self.headers,
            name="GET /v1/admin/trino/health",
        )


class TrinoQueryUser(HttpUser):
    """Simulates direct query workload via admin diagnostic endpoint."""

    wait_time = between(1.0, 2.5)

    @task
    def pool_status(self):
        headers = _headers()
        self.client.get(
            "/v1/admin/trino/pool",
            headers=headers,
            name="GET /v1/admin/trino/pool",
        )
