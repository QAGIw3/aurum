"""Cross-ISO adapter framework for external ingestion.

Adapters provide a thin layer over the shared ExternalCollector abstractions
to implement ISO-specific request construction, chunking, paging and auth.

Modules:
  - base:    Base classes and utilities (circuit breaker, idempotent paging)
  - miso:    MISO Data Exchange adapter
  - caiso:   CAISO OASIS adapter
  - isone:   ISO-NE Web Services adapter
"""

from .base import (
    CircuitBreakerConfig,
    CircuitOpenError,
    IdempotentPager,
    IsoAdapterConfig,
    IsoIngestMetrics,
    IsoRequestChunk,
    IsoAdapter,
)
from .miso import MisoAdapter
from .caiso import CaisoAdapter
from .isone import IsoneAdapter

__all__ = [
    "CircuitBreakerConfig",
    "CircuitOpenError",
    "IdempotentPager",
    "IsoAdapterConfig",
    "IsoIngestMetrics",
    "IsoRequestChunk",
    "IsoAdapter",
    "MisoAdapter",
    "CaisoAdapter",
    "IsoneAdapter",
]

