"""Compatibility wrapper for structured logging utilities.

The canonical implementation lives under ``aurum.logging.structured_logger``.
This module forwards imports so legacy paths used by the API layer remain
functional during the refactor.
"""

from __future__ import annotations

from ..logging import structured_logger as _structured_logger

get_logger = getattr(_structured_logger, "get_logger", lambda *_, **__: None)

__all__ = ["get_logger"]
