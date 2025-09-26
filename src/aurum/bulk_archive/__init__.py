"""Bulk archive ingestion system for EIA and other data sources."""

from __future__ import annotations

from .manager import (
    BulkArchiveManager,
    ArchiveConfig,
    ArchiveStatus,
    DownloadResult,
    VerificationResult
)
__all__ = [
    "BulkArchiveManager",
    "ArchiveConfig",
    "ArchiveStatus",
    "DownloadResult",
    "VerificationResult",
]
