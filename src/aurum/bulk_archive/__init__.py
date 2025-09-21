"""Bulk archive ingestion system for EIA and other data sources."""

from __future__ import annotations

from .manager import (
    BulkArchiveManager,
    ArchiveConfig,
    ArchiveStatus,
    DownloadResult,
    VerificationResult
)
from .verifier import (
    ChecksumVerifier,
    VerificationError,
    ChecksumMismatchError,
    FileNotFoundError
)
from .processor import (
    BulkDataProcessor,
    ProcessingConfig,
    ProcessingResult,
    ProcessingError
)
from .monitor import (
    BulkArchiveMonitor,
    ArchiveMetrics,
    ProcessingMetrics
)

__all__ = [
    "BulkArchiveManager",
    "ArchiveConfig",
    "ArchiveStatus",
    "DownloadResult",
    "VerificationResult",
    "ChecksumVerifier",
    "VerificationError",
    "ChecksumMismatchError",
    "FileNotFoundError",
    "BulkDataProcessor",
    "ProcessingConfig",
    "ProcessingResult",
    "ProcessingError",
    "BulkArchiveMonitor",
    "ArchiveMetrics",
    "ProcessingMetrics"
]
