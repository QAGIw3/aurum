"""Aurum core domain package providing shared models and configuration."""
from .enums import CurrencyCode, IsoCode, IsoMarket, PriceBlock, UnitOfMeasure
from .models import AurumBaseModel, CurveKey, PaginationMeta, PriceObservation, UnitNormalization
from .settings import AurumSettings, get_settings
from .pagination import CursorPage, OffsetPage, Paginator

# Enhanced infrastructure components
from .dependency_injection import (
    EnhancedServiceContainer,
    IServiceProvider,
    ServiceContext,
    ServiceLifecycle,
    get_container,
    get_service,
    get_optional_service,
    is_service_registered,
    register_singleton,
    register_scoped,
    register_transient,
)
from .async_context import (
    RequestContext,
    AsyncResourceManager,
    BackpressureManager,
    AsyncBatch,
    AsyncPool,
    get_request_id,
    get_tenant_id,
    get_user_id,
    get_current_context,
    request_context,
    managed_resources,
)
from .enhanced_caching import (
    CacheLevel,
    EvictionPolicy,
    CacheMetrics,
    CacheEntry,
    CacheBackend,
    MemoryCacheBackend,
    MultiLevelCache,
    CacheWarmer,
    create_memory_cache,
    create_multi_level_cache,
)

__all__ = [
    # Original core components
    "AurumSettings",
    "get_settings",
    "AurumBaseModel",
    "UnitNormalization",
    "CurveKey",
    "PriceObservation",
    "PaginationMeta",
    "CurrencyCode",
    "UnitOfMeasure",
    "IsoCode",
    "IsoMarket",
    "PriceBlock",
    "Paginator",
    "OffsetPage",
    "CursorPage",
    
    # Enhanced dependency injection
    "EnhancedServiceContainer",
    "IServiceProvider",
    "ServiceContext",
    "ServiceLifecycle",
    "get_container",
    "get_service",
    "get_optional_service",
    "is_service_registered",
    "register_singleton",
    "register_scoped",
    "register_transient",
    
    # Enhanced async context
    "RequestContext",
    "AsyncResourceManager",
    "BackpressureManager",
    "AsyncBatch",
    "AsyncPool",
    "get_request_id",
    "get_tenant_id",
    "get_user_id",
    "get_current_context",
    "request_context",
    "managed_resources",
    
    # Enhanced caching
    "CacheLevel",
    "EvictionPolicy",
    "CacheMetrics",
    "CacheEntry",
    "CacheBackend",
    "MemoryCacheBackend",
    "MultiLevelCache",
    "CacheWarmer",
    "create_memory_cache",
    "create_multi_level_cache",
]
