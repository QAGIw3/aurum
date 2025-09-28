"""Aurum API package."""

from __future__ import annotations

import importlib
import os as _os
from typing import Any

if _os.getenv("AURUM_API_LIGHT_INIT", "0") == "1":
    __all__: list[str] = []
else:
    _EXPORTS: dict[str, tuple[str, str | None]] = {
        # Application factories: single, plus env-specific convenience
        "create_app": ("aurum.api.app", "create_app"),
        "create_dev_app": ("aurum.api.app", "create_dev_app"),
        "create_prod_app": ("aurum.api.app", "create_prod_app"),
        "create_test_app": ("aurum.api.app", "create_test_app"),
        # Back-compat: export module for test convenience
        "app": ("aurum.api.app", None),
        # Routers (kept minimal; prefer router registry within factory)
        "health_router": ("aurum.api.health", "router"),
        # Exceptions
        "AurumAPIException": ("aurum.api.exceptions", "AurumAPIException"),
        "ValidationException": ("aurum.api.exceptions", "ValidationException"),
        "NotFoundException": ("aurum.api.exceptions", "NotFoundException"),
        "ForbiddenException": ("aurum.api.exceptions", "ForbiddenException"),
        "ServiceUnavailableException": ("aurum.api.exceptions", "ServiceUnavailableException"),
        "DataProcessingException": ("aurum.api.exceptions", "DataProcessingException"),
        # Container and cache
        "get_service_provider": ("aurum.api.container", "get_service_provider"),
        "configure_services": ("aurum.api.container", "configure_services"),
        "get_service": ("aurum.api.container", "get_service"),
        "AsyncCache": ("aurum.api.cache.cache", "AsyncCache"),
        "CacheManager": ("aurum.api.cache.cache", "CacheManager"),
        "CacheBackend": ("aurum.api.cache.cache", "CacheBackend"),
        # Prefer unified cache manager; warming service remains available for now
        "get_unified_cache_manager": ("aurum.api.cache.consolidated_manager", "get_unified_cache_manager"),
        "get_cache_warming_service": ("aurum.api.cache.advanced_cache", "get_cache_warming_service"),
        # Observability helpers
        "get_metrics_collector": ("aurum.observability.metrics", "get_metrics_collector"),
        "get_trace_collector": ("aurum.observability.tracing", "get_trace_collector"),
        "get_api_logger": ("aurum.observability.logging", "get_api_logger"),
        "get_database_logger": ("aurum.observability.logging", "get_database_logger"),
        "get_cache_logger": ("aurum.observability.logging", "get_cache_logger"),
        "get_performance_logger": ("aurum.observability.logging", "get_performance_logger"),
        "get_security_logger": ("aurum.observability.logging", "get_security_logger"),
        "get_business_logger": ("aurum.observability.logging", "get_business_logger"),
        # Versioning
        "get_version_manager": ("aurum.api.versioning", "get_version_manager"),
        "get_versioned_router": ("aurum.api.versioning", "get_versioned_router"),
        "create_versioned_app": ("aurum.api.versioning", "create_versioned_app"),
        "create_v1_router": ("aurum.api.versioning", "create_v1_router"),
        "create_v2_router": ("aurum.api.versioning", "create_v2_router"),
        "VersionStatus": ("aurum.api.versioning", "VersionStatus"),
        "APIVersion": ("aurum.api.versioning", "APIVersion"),
        "VersionManager": ("aurum.api.versioning", "VersionManager"),
        "VersionedRouter": ("aurum.api.versioning", "VersionedRouter"),
        # Rate limiting
        "create_rate_limit_manager": ("aurum.api.rate_limiting", "create_rate_limit_manager"),
        "RateLimitMiddleware": ("aurum.api.rate_limiting", "RateLimitMiddleware"),
        "RateLimitManager": ("aurum.api.rate_limiting", "RateLimitManager"),
        "RateLimitStorage": ("aurum.api.rate_limiting", "RateLimitStorage"),
        "InMemoryRateLimitStorage": ("aurum.api.rate_limiting", "InMemoryRateLimitStorage"),
        "RedisRateLimitStorage": ("aurum.api.rate_limiting", "RedisRateLimitStorage"),
        "QuotaTier": ("aurum.api.rate_limiting", "QuotaTier"),
        "Quota": ("aurum.api.rate_limiting", "Quota"),
        "RateLimitRule": ("aurum.api.rate_limiting", "RateLimitRule"),
        "RateLimitAlgorithm": ("aurum.api.rate_limiting", "RateLimitAlgorithm"),
        "RateLimitState": ("aurum.api.rate_limiting", "RateLimitState"),
        "RateLimitResult": ("aurum.api.rate_limiting", "RateLimitResult"),
        # Websocket streaming
        "WebSocketManager": ("aurum.api.websocket_manager", "WebSocketManager"),
        "WebSocketConnection": ("aurum.api.websocket_manager", "WebSocketConnection"),
        "ConnectionState": ("aurum.api.websocket_manager", "ConnectionState"),
        "MessageType": ("aurum.api.websocket_manager", "MessageType"),
        "WebSocketMessage": ("aurum.api.websocket_manager", "WebSocketMessage"),
        "StreamSubscription": ("aurum.api.websocket_manager", "StreamSubscription"),
        "StreamingManager": ("aurum.api.scenarios.streaming_manager", "StreamingManager"),
        "DataStream": ("aurum.api.scenarios.streaming_manager", "DataStream"),
        "StreamConfig": ("aurum.api.scenarios.streaming_manager", "StreamConfig"),
        "get_streaming_manager": ("aurum.api.scenarios.streaming_manager", "get_streaming_manager"),
        "initialize_streaming": ("aurum.api.scenarios.streaming_manager", "initialize_streaming"),
        # OpenAPI utilities
        "generate_documentation": ("aurum.api.openapi_generator", "generate_documentation"),
        "OpenAPIGenerator": ("aurum.api.openapi_generator", "OpenAPIGenerator"),
        "SDKGenerator": ("aurum.api.openapi_generator", "SDKGenerator"),
        "DocumentationValidator": ("aurum.api.openapi_generator", "DocumentationValidator"),
        "DocumentationFormat": ("aurum.api.openapi_generator", "DocumentationFormat"),
        "SDKLanguage": ("aurum.api.openapi_generator", "SDKLanguage"),
        "APISchema": ("aurum.api.openapi_generator", "APISchema"),
        "APIEndpoint": ("aurum.api.openapi_generator", "APIEndpoint"),
        # Database helpers
        "get_database_monitor": ("aurum.api.database", "get_database_monitor"),
        "initialize_database_monitoring": ("aurum.api.database", "initialize_database_monitoring"),
        "DatabaseMonitor": ("aurum.api.database", "DatabaseMonitor"),
        "QueryMetrics": ("aurum.api.database", "QueryMetrics"),
        "QueryPattern": ("aurum.api.database", "QueryPattern"),
        "OptimizationSuggestion": ("aurum.api.database", "OptimizationSuggestion"),
        "QueryPerformanceLevel": ("aurum.api.database", "QueryPerformanceLevel"),
        "OptimizationType": ("aurum.api.database", "OptimizationType"),
        # Trino client
        "get_trino_client": ("aurum.api.database", "get_trino_client"),
        "get_trino_client_by_catalog": ("aurum.api.database", "get_trino_client_by_catalog"),
        "get_trino_raw_catalog_client": ("aurum.api.database", "get_trino_raw_catalog_client"),
        "get_trino_market_catalog_client": ("aurum.api.database", "get_trino_market_catalog_client"),
        "get_trino_catalog_config": ("aurum.api.database", "get_trino_catalog_config"),
        "configure_trino_catalogs": ("aurum.api.database", "configure_trino_catalogs"),
        "TrinoClientManager": ("aurum.api.database", "TrinoClientManager"),
        # Feature flags
        "get_feature_manager": ("aurum.api.features.feature_flags", "get_feature_manager"),
        "initialize_feature_flags": ("aurum.api.features.feature_flags", "initialize_feature_flags"),
        "FeatureFlagManager": ("aurum.api.features.feature_flags", "FeatureFlagManager"),
        "FeatureFlag": ("aurum.api.features.feature_flags", "FeatureFlag"),
        "FeatureFlagRule": ("aurum.api.features.feature_flags", "FeatureFlagRule"),
        "ABTestConfiguration": ("aurum.api.features.feature_flags", "ABTestConfiguration"),
        "FeatureFlagStatus": ("aurum.api.features.feature_flags", "FeatureFlagStatus"),
        "UserSegment": ("aurum.api.features.feature_flags", "UserSegment"),
        "RolloutStrategy": ("aurum.api.features.feature_flags", "RolloutStrategy"),
        "ABTestVariant": ("aurum.api.features.feature_flags", "ABTestVariant"),
        "FeatureFlagStore": ("aurum.api.features.feature_flags", "FeatureFlagStore"),
        "InMemoryFeatureFlagStore": ("aurum.api.features.feature_flags", "InMemoryFeatureFlagStore"),
        "RedisFeatureFlagStore": ("aurum.api.features.feature_flags", "RedisFeatureFlagStore"),
        # Guardrails
        "enforce_basic_query_guardrails": ("aurum.api.guardrails", "enforce_basic_query_guardrails"),
    }

    __all__ = sorted(_EXPORTS)

    def __getattr__(name: str) -> Any:
        try:
            module_path, attr_name = _EXPORTS[name]
        except KeyError as exc:
            raise AttributeError(f"module 'aurum.api' has no attribute {name!r}") from exc

        module = importlib.import_module(module_path)
        value = module if attr_name is None else getattr(module, attr_name)
        globals()[name] = value
        return value

    def __dir__() -> list[str]:
        return sorted(set(globals().keys()) | set(__all__))
