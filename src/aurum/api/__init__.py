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
]
