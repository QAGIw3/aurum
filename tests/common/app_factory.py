"""Factory for creating test FastAPI applications without environment dependencies."""

import os
from typing import Dict, Any, Optional
from fastapi import FastAPI
from pydantic import BaseModel


class TestAppConfig(BaseModel):
    """Settings for test applications."""

    # Core settings
    backend_type: str = "trino"  # trino, clickhouse, timescale
    enable_auth: bool = False
    enable_cors: bool = True
    enable_rate_limiting: bool = False
    enable_cache: bool = True

    # Database settings
    trino_host: str = "localhost"
    trino_port: int = 8080
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 9000
    timescale_host: str = "localhost"
    timescale_port: int = 5432

    # Kafka settings
    kafka_bootstrap_servers: list = ["localhost:9092"]
    kafka_enabled: bool = False

    # External dependencies
    schema_registry_url: str = "http://localhost:8081"
    lakefs_endpoint: str = "http://localhost:8000"

    # Feature flags
    enable_golden_queries: bool = True
    enable_observability: bool = True
    enable_external_data: bool = False

    # Admin settings
    admin_group: str = "admin"


def create_test_app(
    settings: Optional[TestAppConfig] = None,
    overrides: Optional[Dict[str, Any]] = None,
) -> FastAPI:
    """Create a FastAPI app for testing with isolated settings.

    Args:
        settings: Base settings to use. If None, uses defaults.
        overrides: Additional settings to override.

    Returns:
        FastAPI app instance configured for testing.
    """
    if settings is None:
        settings = TestAppSettings()

    # Apply overrides
    if overrides:
        for key, value in overrides.items():
            setattr(settings, key, value)

    # Set environment variables temporarily
    env_overrides = {}
    env_map = {
        "AURUM_API_BACKEND": settings.backend_type,
        "AURUM_API_AUTH_DISABLED": "1" if not settings.enable_auth else "0",
        "AURUM_API_CORS_ORIGINS": "*" if settings.enable_cors else "",
        "AURUM_API_RATE_LIMIT_ENABLED": "1" if settings.enable_rate_limiting else "0",
        "AURUM_API_CACHE_ENABLED": "1" if settings.enable_cache else "0",
        "AURUM_API_TRINO_HOST": settings.trino_host,
        "AURUM_API_TRINO_PORT": str(settings.trino_port),
        "AURUM_API_CLICKHOUSE_HOST": settings.clickhouse_host,
        "AURUM_API_CLICKHOUSE_PORT": str(settings.clickhouse_port),
        "AURUM_API_TIMESCALE_HOST": settings.timescale_host,
        "AURUM_API_TIMESCALE_PORT": str(settings.timescale_port),
        "AURUM_API_KAFKA_BOOTSTRAP_SERVERS": ",".join(settings.kafka_bootstrap_servers),
        "AURUM_API_KAFKA_ENABLED": "1" if settings.kafka_enabled else "0",
        "AURUM_API_SCHEMA_REGISTRY_URL": settings.schema_registry_url,
        "AURUM_API_LAKEFS_ENDPOINT": settings.lakefs_endpoint,
        "AURUM_API_GOLDEN_QUERIES_ENABLED": "1" if settings.enable_golden_queries else "0",
        "AURUM_API_OBSERVABILITY_ENABLED": "1" if settings.enable_observability else "0",
        "AURUM_API_EXTERNAL_DATA_ENABLED": "1" if settings.enable_external_data else "0",
        "AURUM_API_ADMIN_GROUP": settings.admin_group,
    }

    for key, value in env_map.items():
        if key not in os.environ:
            env_overrides[key] = os.environ.get(key)
            os.environ[key] = str(value)

    try:
        # Create a fresh app instance
        app = FastAPI(
            title="Aurum API - Test",
            description="Test instance of Aurum API",
            version="1.0.0-test",
        )

        # For now, create a minimal app structure for testing
        # In a real implementation, this would import and configure
        # the actual aurum app components

        # Add a simple health endpoint for testing
        @app.get("/health")
        async def health():
            return {"status": "ok"}

        # Add a metadata endpoint for testing
        @app.get("/v1/metadata/units")
        async def metadata_units():
            from fastapi import Response
            import hashlib
            import json

            data = {"data": [], "meta": {"total": 0}}
            content = json.dumps(data, sort_keys=True)
            etag = hashlib.md5(content.encode()).hexdigest()

            return Response(
                content=content,
                media_type="application/json",
                headers={
                    "ETag": f'"{etag}"',
                    "Cache-Control": "max-age=300"
                }
            )

        return app

    finally:
        # Restore environment variables
        for key, original_value in env_overrides.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value
