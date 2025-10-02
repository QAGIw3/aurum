"""Collector framework for streaming external provider data."""

from .base import (
    AvroEncoder,
    CollectorConfig,
    CollectorContext,
    CollectorError,
    CollectorMetrics,
    ExternalCollector,
    provider_series_key,
    HttpRequest,
    HttpRequestError,
    HttpResponse,
    RateLimitConfig,
    RateLimiter,
    RetryConfig,
    RetryLimitExceeded,
    create_avro_producer,
)
from .checkpoints import (
    Checkpoint,
    CheckpointStore,
    PostgresCheckpointStore,
    RedisCheckpointStore,
)
from .unified_collector import (
    BaseProviderCollector,
    DataTransformer,
    DatasetConfig,
    JSONDataTransformer,
    ProviderConfig,
    create_provider_collector,
)

__all__ = [
    # Base framework
    "AvroEncoder",
    "Checkpoint",
    "CheckpointStore",
    "CollectorConfig",
    "CollectorContext",
    "CollectorError",
    "CollectorMetrics",
    "ExternalCollector",
    "provider_series_key",
    "HttpRequest",
    "HttpRequestError",
    "HttpResponse",
    "PostgresCheckpointStore",
    "RateLimitConfig",
    "RateLimiter",
    "RedisCheckpointStore",
    "RetryConfig",
    "RetryLimitExceeded",
    "create_avro_producer",

    # Unified collector framework
    "BaseProviderCollector",
    "DataTransformer",
    "DatasetConfig",
    "JSONDataTransformer",
    "ProviderConfig",
    "create_provider_collector",
]
