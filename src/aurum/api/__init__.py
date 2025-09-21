"""Aurum API package."""

from . import app as app_module
from .health import router as health_router
from .curves import router as curves_router
from .metadata import router as metadata_router
from .scenarios import router as scenarios_router
from .cache_analytics import router as cache_analytics_router
from .exceptions import (
    AurumAPIException,
    ValidationException,
    NotFoundException,
    ForbiddenException,
    ServiceUnavailableException,
    DataProcessingException,
)
from .container import get_service_provider, configure_services, get_service
from .cache import AsyncCache, CacheManager, CacheBackend
from .advanced_cache import (
    get_advanced_cache_manager,
    get_cache_warming_service,
    AdvancedCacheManager,
    CacheWarmingService,
)
from .observability import (
    metrics,
    tracing,
    logging,
    api,
)
from .observability.metrics import get_metrics_collector
from .observability.tracing import get_trace_collector
from .observability.logging import (
    get_api_logger,
    get_database_logger,
    get_cache_logger,
    get_performance_logger,
    get_security_logger,
    get_business_logger,
)
from .versioning import (
    get_version_manager,
    get_versioned_router,
    create_versioned_app,
    create_v1_router,
    create_v2_router,
    VersionStatus,
    APIVersion,
    VersionManager,
    VersionedRouter,
)
from .version_management import router as version_management_router
from .rate_limiting import (
    create_rate_limit_manager,
    RateLimitMiddleware,
    RateLimitManager,
    RateLimitStorage,
    InMemoryRateLimitStorage,
    RedisRateLimitStorage,
    QuotaTier,
    Quota,
    RateLimitRule,
    RateLimitAlgorithm,
    RateLimitState,
    RateLimitResult,
)
from .rate_limit_management import router as rate_limit_management_router
from .websocket_manager import (
    WebSocketManager,
    WebSocketConnection,
    ConnectionState,
    MessageType,
    WebSocketMessage,
    StreamSubscription,
)
from .streaming_manager import (
    StreamingManager,
    DataStream,
    StreamConfig,
    get_streaming_manager,
    initialize_streaming,
)
from .streaming_endpoints import router as streaming_endpoints_router
from .streaming_management import router as streaming_management_router
from .openapi_generator import (
    generate_documentation,
    OpenAPIGenerator,
    SDKGenerator,
    DocumentationValidator,
    DocumentationFormat,
    SDKLanguage,
    APISchema,
    APIEndpoint,
)
from .documentation_manager import router as documentation_manager_router
from .database_monitor import (
    get_database_monitor,
    initialize_database_monitoring,
    DatabaseMonitor,
    QueryMetrics,
    QueryPattern,
    OptimizationSuggestion,
    QueryPerformanceLevel,
    OptimizationType,
)
from .database_management import router as database_management_router
from .feature_flags import (
    get_feature_manager,
    initialize_feature_flags,
    FeatureFlagManager,
    FeatureFlag,
    FeatureFlagRule,
    ABTestConfiguration,
    FeatureFlagStatus,
    UserSegment,
    RolloutStrategy,
    ABTestVariant,
    FeatureFlagStore,
    InMemoryFeatureFlagStore,
    RedisFeatureFlagStore,
)
from .feature_management import router as feature_management_router

create_app = app_module.create_app
app = app_module

__all__ = [
    "create_app",
    "app",
    "health_router",
    "curves_router",
    "metadata_router",
    "scenarios_router",
    "cache_analytics_router",
    "version_management_router",
    "AurumAPIException",
    "ValidationException",
    "NotFoundException",
    "ForbiddenException",
    "ServiceUnavailableException",
    "DataProcessingException",
    "get_service_provider",
    "configure_services",
    "get_service",
    "AsyncCache",
    "CacheManager",
    "CacheBackend",
    "get_advanced_cache_manager",
    "get_cache_warming_service",
    "AdvancedCacheManager",
    "CacheWarmingService",
    "get_metrics_collector",
    "get_trace_collector",
    "get_api_logger",
    "get_database_logger",
    "get_cache_logger",
    "get_performance_logger",
    "get_security_logger",
    "get_business_logger",
    "get_version_manager",
    "get_versioned_router",
    "create_versioned_app",
    "create_v1_router",
    "create_v2_router",
    "VersionStatus",
    "APIVersion",
    "VersionManager",
    "VersionedRouter",    "create_rate_limit_manager",
    "RateLimitMiddleware",
    "RateLimitManager",
    "RateLimitStorage",
    "InMemoryRateLimitStorage",
    "RedisRateLimitStorage",
    "QuotaTier",
    "Quota",
    "RateLimitRule",
    "RateLimitAlgorithm",
    "RateLimitState",
    "RateLimitResult",
    "rate_limit_management_router",
    "WebSocketManager",
    "WebSocketConnection",
    "ConnectionState",
    "MessageType",
    "WebSocketMessage",
    "StreamSubscription",
    "StreamingManager",
    "DataStream",
    "StreamConfig",
    "get_streaming_manager",
    "initialize_streaming",
    "streaming_endpoints_router",
    "streaming_management_router",
    "generate_documentation",
    "OpenAPIGenerator",
    "SDKGenerator",
    "DocumentationValidator",
    "DocumentationFormat",
    "SDKLanguage",
    "APISchema",
    "APIEndpoint",
    "documentation_manager_router",
    "get_database_monitor",
    "initialize_database_monitoring",
    "DatabaseMonitor",
    "QueryMetrics",
    "QueryPattern",
    "OptimizationSuggestion",
    "QueryPerformanceLevel",
    "OptimizationType",
    "database_management_router",
    "get_feature_manager",
    "initialize_feature_flags",
    "FeatureFlagManager",
    "FeatureFlag",
    "FeatureFlagRule",
    "ABTestConfiguration",
    "FeatureFlagStatus",
    "UserSegment",
    "RolloutStrategy",
    "ABTestVariant",
    "FeatureFlagStore",
    "InMemoryFeatureFlagStore",
    "RedisFeatureFlagStore",
    "feature_management_router",
]
